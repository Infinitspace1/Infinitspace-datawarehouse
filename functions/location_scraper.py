"""
Location Scraper — Azure Durable Functions module.

Functions registered here:
  - location_scraper_http   POST /api/scrape  (HTTP starter)
  - location_scraper_orch   (Durable orchestrator)
  - ls_resolve_source       (activity)
  - ls_start_apify_run      (activity)
  - ls_check_apify_run      (activity)
  - ls_fetch_dataset        (activity)
  - ls_normalize            (activity)
  - ls_dedupe_agencies      (activity)
  - ls_filter_new_agencies  (activity)
  - ls_enrich_agency        (activity — one per agency, fan-out target)
  - ls_consolidate_contacts (activity)
  - ls_upsert_sql           (activity)
  - ls_write_logs           (activity)

Trigger: POST /api/scrape
Body: {"City": str, "shape": str|null, "run_id": str}
Response: 202 Accepted with {"run_id": str, "status_url": str}
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import timedelta

import azure.durable_functions as df
import azure.functions as func

from shared.location_scraper.activities import enrich as enrich_act
from shared.location_scraper.activities import log_run as log_act
from shared.location_scraper.activities import persist as persist_act
from shared.location_scraper.activities import resolve as resolve_act
from shared.location_scraper.activities import scrape as scrape_act

logger = logging.getLogger(__name__)

bp = df.Blueprint()


def _summarize_enrichment_diagnostics(enriched_agencies: list[dict]) -> dict:
    summary: dict[str, int] = {
        "agencies_total": 0,
        "agencies_with_contacts": 0,
        "path_individual": 0,
        "path_company": 0,
        "path_individual_fallback_company": 0,
        "individual_email_primary": 0,
        "individual_email_email_addresses": 0,
        "domains_found_total": 0,
        "raw_contacts_found_total": 0,
        "final_contacts_found_total": 0,
    }
    reasons: dict[str, int] = {}

    for agency in enriched_agencies:
        d = agency.get("_diagnostics") or {}
        summary["agencies_total"] += 1
        if d.get("has_contact"):
            summary["agencies_with_contacts"] += 1

        path = d.get("path")
        if path == "individual":
            summary["path_individual"] += 1
        elif path == "company":
            summary["path_company"] += 1
        elif path == "individual_fallback_company":
            summary["path_individual_fallback_company"] += 1

        email_src = d.get("individual_email_source")
        if email_src == "primaryEmail":
            summary["individual_email_primary"] += 1
        elif email_src == "emailAddresses":
            summary["individual_email_email_addresses"] += 1

        summary["domains_found_total"] += int(d.get("domains_found") or 0)
        summary["raw_contacts_found_total"] += int(d.get("raw_contacts_found") or 0)
        summary["final_contacts_found_total"] += int(d.get("final_contacts_found") or 0)

        reason = d.get("reason")
        if reason:
            reasons[reason] = reasons.get(reason, 0) + 1

    summary["reasons"] = reasons
    return summary

# ---------------------------------------------------------------------------
# HTTP Trigger — POST /api/scrape
# ---------------------------------------------------------------------------

@bp.route(route="api/scrape", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def location_scraper_http(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient,
) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Request body must be valid JSON.", status_code=400)

    city = body.get("City") or body.get("city")
    if not city:
        return func.HttpResponse('Missing required field "City".', status_code=400)

    shape = body.get("shape")
    run_id = body.get("run_id") or str(uuid.uuid4())

    # Persist a RUNNING row immediately so the caller can track status.
    try:
        log_act.init_run_log(run_id, city.lower().strip())
    except Exception:
        logger.exception("Could not insert initial log row for run_id=%s", run_id)

    orchestrator_input = {
        "city": city,
        "shape": shape,
        "run_id": run_id,
    }

    instance_id = await client.start_new("location_scraper_orch", None, orchestrator_input)
    logger.info("Started orchestrator instance_id=%s run_id=%s city=%s", instance_id, run_id, city)

    return client.create_check_status_response(req, instance_id)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def location_scraper_orch(context: df.DurableOrchestrationContext):
    payload: dict = context.get_input()
    city: str = payload["city"]
    shape = payload.get("shape")
    run_id: str = payload["run_id"]

    # 1. Resolve city → SourceConfig
    source_config: dict = yield context.call_activity(
        "ls_resolve_source",
        {"city": city, "shape": shape, "run_id": run_id},
    )

    # 2. Start Apify actor (async — do NOT block on completion)
    run_info: dict = yield context.call_activity("ls_start_apify_run", source_config)

    # 3. Poll until Apify run finishes (exponential-ish backoff, max 45 min)
    poll_delays = [30, 60, 90, 120, 120, 120, 120, 120, 120, 120, 120, 120, 120, 120, 120, 120, 120, 120, 120, 120]
    for delay_seconds in poll_delays:
        status: dict = yield context.call_activity("ls_check_apify_run", run_info)
        if status["finished"]:
            break
        deadline = context.current_utc_datetime + timedelta(seconds=delay_seconds)
        yield context.create_timer(deadline)
    else:
        raise RuntimeError(f"Apify run did not finish within timeout. run_id={run_info.get('run_id')}")

    if not status.get("succeeded"):
        raise RuntimeError(
            f"Apify run failed with status={status.get('status')} run_id={run_info.get('run_id')}"
        )

    # 4. Download dataset
    raw_items: list[dict] = yield context.call_activity("ls_fetch_dataset", run_info)

    # 5. Normalize via source adapter
    listings: list[dict] = yield context.call_activity(
        "ls_normalize",
        {"actor": source_config["actor"], "items": raw_items, "city": source_config["city"]},
    )

    # 6. Dedupe agencies
    agencies: list[dict] = yield context.call_activity("ls_dedupe_agencies", listings)

    # 7. Filter out agencies already enriched in SQL
    new_agencies: list[dict] = yield context.call_activity("ls_filter_new_agencies", agencies)

    # 8. Fan-out: enrich each agency in parallel
    enrich_tasks = [
        context.call_activity(
            "ls_enrich_agency",
            {
                "agency": agency,
                "country": source_config["country"],
                "country_code": source_config["country_code"],
            },
        )
        for agency in new_agencies
    ]
    enriched_agencies: list[dict] = yield context.task_all(enrich_tasks)
    enrichment_diag = _summarize_enrichment_diagnostics(enriched_agencies)
    logger.info(
        "Location scraper enrichment summary run_id=%s city=%s %s",
        run_id,
        source_config["city"],
        json.dumps(enrichment_diag, sort_keys=True),
    )

    # 9. Consolidate contacts (dedup + top-3 per agency)
    bundles: list[dict] = yield context.call_activity("ls_consolidate_contacts", enriched_agencies)

    # 10. Upsert buildings, listings, contacts to SQL
    stats: dict = yield context.call_activity(
        "ls_upsert_sql",
        {"listings": listings, "bundles": bundles, "run_id": run_id, "city": source_config["city"]},
    )
    stats["enrichment_diagnostics"] = enrichment_diag

    # 11. Write completion log
    yield context.call_activity("ls_write_logs", stats)

    return stats


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

@bp.activity_trigger(input_name="payload")
def ls_resolve_source(payload: dict) -> dict:
    cfg = resolve_act.resolve_source(
        city=payload["city"],
        shape=payload.get("shape"),
        run_id=payload["run_id"],
    )
    return cfg.to_dict()


@bp.activity_trigger(input_name="config")
def ls_start_apify_run(config: dict) -> dict:
    return scrape_act.start_apify_run(config)


@bp.activity_trigger(input_name="run_info")
def ls_check_apify_run(run_info: dict) -> dict:
    return scrape_act.check_apify_run(run_info)


@bp.activity_trigger(input_name="run_info")
def ls_fetch_dataset(run_info: dict) -> list:
    return scrape_act.fetch_dataset(run_info)


@bp.activity_trigger(input_name="payload")
def ls_normalize(payload: dict) -> list:
    return scrape_act.normalize_listings(payload)


@bp.activity_trigger(input_name="listings")
def ls_dedupe_agencies(listings: list) -> list:
    return enrich_act.dedupe_agencies(listings)


@bp.activity_trigger(input_name="agencies")
def ls_filter_new_agencies(agencies: list) -> list:
    return enrich_act.filter_new_agencies(agencies)


@bp.activity_trigger(input_name="payload")
def ls_enrich_agency(payload: dict) -> dict:
    return enrich_act.enrich_agency(payload)


@bp.activity_trigger(input_name="agencies")
def ls_consolidate_contacts(agencies: list) -> list:
    return enrich_act.consolidate_contacts(agencies)


@bp.activity_trigger(input_name="payload")
def ls_upsert_sql(payload: dict) -> dict:
    return persist_act.upsert_sql(payload)


@bp.activity_trigger(input_name="stats")
def ls_write_logs(stats: dict) -> None:
    log_act.write_logs(stats)
