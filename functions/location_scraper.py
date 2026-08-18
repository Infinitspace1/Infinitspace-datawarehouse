"""
Location Scraper — Azure Durable Functions module.

Functions registered here:
  - location_scraper_http   POST /api/scrape  (HTTP starter)
  - location_scraper_weekly  (weekly timer starter, Monday — starts the parent)
  - location_scraper_weekly_orch (parent orchestrator — sequential waves)
  - location_scraper_orch   (Durable orchestrator — one city)
  - ls_init_run_log         (activity — RUNNING log row per city)
  - ls_cities_needing_run   (activity — skip cities already completed this month)
  - ls_resolve_source       (activity)
  - ls_start_apify_run      (activity)
  - ls_check_apify_run      (activity)
  - ls_fetch_and_persist_raw (activity — streams Apify dataset → bronze.location_scraper_raw)
  - ls_fetch_dataset        (activity — legacy, no longer on the hot path)
  - ls_persist_raw          (activity — legacy, no longer on the hot path)
  - ls_normalize            (activity — reads raw back from SQL by run_id)
  - ls_dedupe_agencies      (activity)
  - ls_filter_new_agencies  (activity)
  - ls_enrich_agency        (activity — one per agency, fan-out target)
  - ls_consolidate_contacts (activity)
  - ls_upsert_sql           (activity)
  - ls_write_lusha_diagnostics (activity)
  - ls_write_logs           (activity)
  - ls_materialize_globe    (activity)
  - ls_refresh_gold_map_markers (activity)
  - ls_refresh_globe_quality (activity)

Trigger: POST /api/scrape
Body: {"City": str, "shape": str|null, "run_id": str}
Response: 202 Accepted with {"run_id": str, "status_url": str}
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import azure.durable_functions as df
import azure.functions as func

from shared.location_scraper import run_health
from shared.location_scraper.activities import alerts as alerts_act
from shared.location_scraper.activities import enrich as enrich_act
from shared.location_scraper.activities import enumerate_loopnet as enumerate_loopnet_act
from shared.location_scraper.activities import log_run as log_act
from shared.location_scraper.activities import materialize_globe as materialize_act
from shared.location_scraper.activities import persist as persist_act
from shared.location_scraper.activities import raw_payload as raw_act
from shared.location_scraper.activities import resolve as resolve_act
from shared.location_scraper.activities import run_health as run_health_act
from shared.location_scraper.activities import scrape as scrape_act
from shared.location_scraper.run_quality import compute_run_quality_payload

logger = logging.getLogger(__name__)

bp = df.Blueprint()

# Mondays at 08:00 UTC (NCRONTAB: sec min hour day month day-of-week).
#
# Moved off 01:00 UTC on 2026-08-18. The LoopNet actor's mobile API is 403 on
# every listing, so each one is recovered through its paid unblocker chain, and
# that chain is throttled in the small hours: the same London scrape lost 39/48
# listings at 01:13 UTC and 2/48 at 08:46 UTC on the same day (2026-07-20), and
# 21/395 at 10:36 UTC on 2026-07-31. Every post-403 daytime sample recovers
# 95-100%, every night sample 19-29%.
WEEKLY_SCHEDULE = os.getenv("LOCATION_SCRAPER_WEEKLY_SCHEDULE", "0 0 8 * * 1")

# Wednesdays at 08:00 UTC -- second pass over the cities that did not complete
# on Monday (failed, degraded, or never started). ls_cities_needing_run skips
# the ones already completed, so a clean week is a no-op.
WEEKLY_RETRY_SCHEDULE = os.getenv("LOCATION_SCRAPER_WEEKLY_RETRY_SCHEDULE", "0 0 8 * * 3")
SCRAPE_CITIES = (
    "barcelona",
    "madrid",
    "milan",
    "berlin",
    "munich",
    "hamburg",
    "cologne",
    "frankfurt",
    "dusseldorf",
    "stuttgart",
    "warsaw",
    "london",
    # US (LoopNet)
    "new york",
    "san francisco",
    "palo alto",
    "los angeles",
    "austin",
    "seattle",
    "redwood city",
    "san mateo",
    "san bruno",
    "cupertino",
    # Canada (LoopNet)
    "toronto",
)

# Sources whose listing payload already carries broker name/company/phone/email,
# so the Lusha enrichment fan-out is skipped (broker email is persisted + shown
# on the globe directly). LoopNet returns broker contact on every listing.
LUSHA_SKIP_SOURCES = {"loopnet"}

# LoopNet country codes that use the listing-URL enumeration path instead of
# the broad search.
#
# Measured 2026-07-23, the day the actor dev restored broker extraction:
#   - loopnet.co.uk / loopnet.ca ignore the `min-space-size` URL filter and cap
#     the broad search at ~30 items (London: 30 vs 383 qualifying buildings on
#     the site). Enumeration is the only way to see that market.
#   - loopnet.com serves the broad search properly since the same fix
#     (Los Angeles 248, New York 187, Seattle 75 buildings).
# Enumeration is NOT free: each enumerated URL needs a per-listing detail fetch,
# and those currently fail ~75% of the time (mobile API 403 -> the actor's paid
# unblocker erroring). Seattle regressed 75 -> 31 buildings when enumerated, so
# the path is restricted to the domains where the broad search cannot work.
# Revisit once the dev ships the faster internal path he announced.
LOOPNET_ENUMERATION_COUNTRIES = {"gb", "ca"}


def _loopnet_uses_enumeration(source_config: dict) -> bool:
    if source_config.get("actor") != "loopnet":
        return False
    raw = (os.getenv("LOOPNET_ENUMERATION_COUNTRIES") or "").strip()
    countries = (
        {c.strip().lower() for c in raw.split(",") if c.strip()}
        if raw
        else LOOPNET_ENUMERATION_COUNTRIES
    )
    return str(source_config.get("country_code") or "").lower() in countries

def _enum_attempts() -> int:
    """How many times the LoopNet URL enumeration is tried before giving up.

    The enumeration actor returns 0 items when the residential edge refuses it
    ("Connection refused" x10, then its own stop-after-10-attempts guard). That
    is transient -- the same input returned the full page 1 minutes later on
    2026-08-18 -- so a retry is worth far more than the silent fallback to the
    broad search it used to trigger.
    """
    raw = (os.getenv("LOCATION_SCRAPER_ENUM_ATTEMPTS") or "3").strip()
    try:
        value = int(raw)
    except ValueError:
        return 3
    return value if value > 0 else 3


def _enum_retry_minutes() -> int:
    raw = (os.getenv("LOCATION_SCRAPER_ENUM_RETRY_MINUTES") or "5").strip()
    try:
        value = int(raw)
    except ValueError:
        return 5
    return value if value > 0 else 5


# Weekly run processes cities in sequential waves of this size so that only a
# handful of (memory-heavy) Apify datasets are loaded at once — prevents the
# worker OOM (exit code 137) seen when all cities fan out simultaneously.
# Override via LOCATION_SCRAPER_WAVE_SIZE.
def _wave_size() -> int:
    raw = (os.getenv("LOCATION_SCRAPER_WAVE_SIZE") or "3").strip()
    try:
        value = int(raw)
    except ValueError:
        return 3
    return value if value > 0 else 3


def _safe_mark_run_failed(run_id: str | None, error: str) -> None:
    if not run_id:
        return
    try:
        log_act.mark_run_failed(run_id, error)
    except Exception:
        logger.exception("Could not mark location scraper run failed. run_id=%s", run_id)


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
    client,
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
        # On-demand sourcing has no item cap, same as the weekly job — we want
        # every available building, not just Apify's first 100. Pass
        # "unlimited_items": false in the request body to re-enable the cap.
        "unlimited_items": bool(body.get("unlimited_items", True)),
    }

    instance_id = await client.start_new("location_scraper_orch", None, orchestrator_input)
    logger.info("Started orchestrator instance_id=%s run_id=%s city=%s", instance_id, run_id, city)

    return client.create_check_status_response(req, instance_id)


# ---------------------------------------------------------------------------
# Timer Trigger — weekly scrape (Monday) for all supported rollout cities
# ---------------------------------------------------------------------------

@bp.timer_trigger(schedule=WEEKLY_SCHEDULE, arg_name="timer", run_on_startup=False)
@bp.durable_client_input(client_name="client")
async def location_scraper_weekly(timer: func.TimerRequest, client) -> None:
    """Start the weekly parent orchestrator (one instance per ISO week).

    The parent (``location_scraper_weekly_orch``) processes cities in sequential
    waves so only a few memory-heavy Apify datasets load at once (avoids the
    worker OOM seen with a full fan-out), and skips cities already completed this
    week so a re-trigger only retries failed/missing ones.
    """
    now = datetime.now(timezone.utc)
    period_key = now.strftime("%G-W%V")  # ISO week, e.g. 2026-W25

    if timer.past_due:
        logger.warning("Weekly location scraper timer is past due")

    await _start_weekly_parent(client, period_key, f"location-scraper-weekly-{period_key}")


async def _start_weekly_parent(client, period_key: str, instance_id: str) -> None:
    """Start (or restart) the weekly parent orchestrator for one ISO week."""
    existing = await client.get_status(instance_id)
    if existing:
        existing_status = str(getattr(existing, "runtime_status", ""))
        status_name = existing_status.split(".")[-1].lower()
        # A (re-)trigger always supersedes the current parent. If it is still in
        # progress, TERMINATE it first — otherwise a hung/stuck parent would
        # silently block every Test/Run forever (the previous "skip if running"
        # behaviour). Then purge so start_new can reuse the same instance_id.
        # SQL idempotency (ls_cities_needing_run) means the fresh parent only
        # re-runs cities not yet completed this week, so superseding is safe.
        if status_name in {"running", "pending", "suspended"}:
            logger.info(
                "Terminating in-progress weekly parent before re-run. "
                "instance_id=%s status=%s",
                instance_id,
                existing_status,
            )
            try:
                await client.terminate(instance_id, "Superseded by a new weekly trigger")
            except Exception:
                logger.warning("Could not terminate weekly parent %s", instance_id)
        try:
            await client.purge_instance_history(instance_id)
        except Exception:
            logger.warning("Could not purge prior weekly parent instance %s", instance_id)

    try:
        await client.start_new(
            "location_scraper_weekly_orch",
            instance_id,
            {
                "period_key": period_key,
                "cities": list(SCRAPE_CITIES),
                "wave_size": _wave_size(),
            },
        )
    except Exception:
        # Rare terminate/purge race: the old instance was not fully cleared yet.
        # The next trigger will start cleanly (it is now terminated + purged).
        logger.exception(
            "Could not start weekly parent %s (terminate/purge race?) — re-trigger once",
            instance_id,
        )
        return
    logger.info(
        "Weekly location scraper parent started instance_id=%s week=%s wave_size=%d",
        instance_id,
        period_key,
        _wave_size(),
    )


# ---------------------------------------------------------------------------
# Timer Trigger — mid-week retry of the cities that did not complete
# ---------------------------------------------------------------------------

@bp.timer_trigger(schedule=WEEKLY_RETRY_SCHEDULE, arg_name="timer", run_on_startup=False)
@bp.durable_client_input(client_name="client")
async def location_scraper_weekly_retry(timer: func.TimerRequest, client) -> None:
    """Second pass over the current ISO week.

    Runs the same parent under its own instance id; ls_cities_needing_run keeps
    only the cities whose weekly row is not 'completed' — i.e. the ones that
    failed or came back degraded on Monday. A clean week is a no-op, so this
    costs nothing when nothing is broken.
    """
    now = datetime.now(timezone.utc)
    period_key = now.strftime("%G-W%V")

    if timer.past_due:
        logger.warning("Weekly location scraper retry timer is past due")

    logger.info("Weekly location scraper retry pass starting for %s", period_key)
    await _start_weekly_parent(
        client, period_key, f"location-scraper-weekly-{period_key}-retry"
    )


# ---------------------------------------------------------------------------
# Parent orchestrator — sequential waves of cities (OOM-safe fan-out)
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def location_scraper_weekly_orch(context: df.DurableOrchestrationContext):
    """Run the weekly cities in sequential waves of ``wave_size``.

    Each wave fans out its cities concurrently (via sub-orchestrations) and the
    next wave only starts once the current one finishes — so at most ``wave_size``
    Apify datasets are in memory at any time. A city failure is isolated to its
    wave (caught, logged) and does not block later waves; it stays non-completed
    in SQL so the next weekly run retries it.
    """
    payload: dict = context.get_input() or {}
    period_key: str = payload.get("period_key") or payload["month_key"]
    cities: list = payload.get("cities") or list(SCRAPE_CITIES)
    wave_size: int = int(payload.get("wave_size") or 3)
    force_full: bool = bool(payload.get("force_full"))

    # Slugify here (orchestrator) so run_id/labels stay deterministic; cities
    # such as "new york" carry a space that is unsafe in run_id keys.
    candidates = [
        {"city": c, "run_id": f"weekly-{c.replace(' ', '-')}-{period_key}"}
        for c in cities
    ]

    todo: list = yield context.call_activity(
        "ls_cities_needing_run",
        {"candidates": candidates, "force_full": force_full},
    )

    summary = {
        "period_key": period_key,
        "cities_total": len(cities),
        "cities_run": len(todo),
        "waves": 0,
        "waves_with_failures": 0,
    }

    for start in range(0, len(todo), wave_size):
        wave = todo[start : start + wave_size]
        summary["waves"] += 1

        tasks = []
        for entry in wave:
            # Mark the run RUNNING before its sub-orchestration starts.
            yield context.call_activity(
                "ls_init_run_log",
                {"run_id": entry["run_id"], "city": entry["city"]},
            )
            # No explicit instance_id → Durable assigns a unique child id; the
            # month-scoped run_id is what drives SQL/gold de-duplication.
            tasks.append(
                context.call_sub_orchestrator(
                    "location_scraper_orch",
                    {
                        "city": entry["city"],
                        "shape": None,
                        "run_id": entry["run_id"],
                        "unlimited_items": True,
                    },
                )
            )

        try:
            yield context.task_all(tasks)
        except Exception:
            # One or more cities in this wave failed (each already recorded its
            # own failure in SQL). Continue with the next wave regardless.
            summary["waves_with_failures"] += 1
            if not context.is_replaying:
                logger.warning(
                    "Weekly wave %d had failures (cities=%s); continuing",
                    summary["waves"],
                    [e["city"] for e in wave],
                )

    # Email the cities that did not come back clean (silent on a clean week).
    alert: dict = yield context.call_activity(
        "ls_alert_run_health", {"period_key": period_key}
    )
    summary["cities_unhealthy"] = int((alert or {}).get("cities_unhealthy") or 0)

    if not context.is_replaying:
        logger.info("Weekly location scraper parent complete: %s", json.dumps(summary, sort_keys=True))
    return summary


def _scrape_with_retries(context, *, city: str, run_id: str, source_config: dict):
    """Run the Apify scrape for one city, retrying attempts that come back bad.

    A sub-generator of ``location_scraper_orch``: it yields Durable tasks (the
    caller delegates with ``yield from``) and returns
    ``(base_config, raw_item_count, health_verdict)``.

    A LoopNet run can finish SUCCEEDED while carrying a fraction of the city:
    the actor's mobile API is 403 on every listing and its paid unblocker chain
    drops whatever it cannot recover (34/48 London listings on 2026-08-17), and
    the gb/ca enumeration can come back with zero URLs. Both are transient, so
    every attempt is graded and a bad one is retried after a delay instead of
    being written to SQL as a normal week.
    See shared/location_scraper/run_health.py.
    """
    attempts = run_health.max_attempts()
    uses_enumeration = _loopnet_uses_enumeration(source_config)
    base_config = source_config
    best: dict | None = None
    persisted_dataset_id: str | None = None
    raw_item_count = 0

    for attempt in range(1, attempts + 1):
        source_config = base_config
        enumerated_url_count = 0

        # LoopNet (gb/ca): enumerate the space-available-filtered search
        # pages and scrape exactly those listing-detail URLs. The broad
        # search is capped per bounding box -- London plateaus at ~30 items
        # against 383 qualifying buildings on the site, and the
        # min-space-size URL filter is ignored on the non-.com domains.
        #
        # History: this 2-step was retired 2026-06-29 because the actor's
        # 2026-06-27 rebuild stripped broker contact + surface from the
        # listing-URL payload. The actor dev restored broker extraction on
        # 2026-07-23, re-validated the same day: enumeration returns 397
        # London URLs in 97s, carrying brokerName/company/email.
        #
        # Enumeration returning nothing is still not fatal (we fall through
        # to the broad search), but it is retried first and it marks the run
        # degraded, because the fallback yields a tenth of the market.
        if uses_enumeration:
            listing_urls: list = []
            enum_attempts = _enum_attempts()
            for enum_attempt in range(1, enum_attempts + 1):
                enumerated: dict = yield context.call_activity(
                    "ls_enumerate_loopnet_urls", base_config
                )
                listing_urls = (enumerated or {}).get("listing_urls") or []
                if listing_urls or enum_attempt >= enum_attempts:
                    break
                if not context.is_replaying:
                    logger.warning(
                        "LoopNet enumeration returned no URLs (attempt %d/%d); "
                        "retrying in %d min. city=%s run_id=%s",
                        enum_attempt,
                        enum_attempts,
                        _enum_retry_minutes(),
                        city,
                        run_id,
                    )
                yield context.create_timer(
                    context.current_utc_datetime
                    + timedelta(minutes=_enum_retry_minutes())
                )
            enumerated_url_count = len(listing_urls)
            if listing_urls:
                source_config = {**base_config, "listing_urls": listing_urls}
            elif not context.is_replaying:
                logger.warning(
                    "LoopNet enumeration returned no URLs after %d attempts; "
                    "falling back to the broad search. city=%s run_id=%s",
                    enum_attempts,
                    city,
                    run_id,
                )

        # 2. Start Apify actor (async -- do NOT block on completion)
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
            # FAILED / ABORTED / TIMED-OUT on the Apify side: no dataset to
            # grade, but the run log still says WHY. Read it so the failure
            # reaches SQL in words, and so an exhausted upstream quota stops
            # the loop instead of buying two more identical failures.
            failure: dict = yield context.call_activity(
                "ls_assess_run_health",
                {
                    "run_id": run_id,
                    "city": source_config["city"],
                    "source": source_config["actor"],
                    "apify_run_id": run_info["run_id"],
                    "raw_item_count": 0,
                    "used_enumeration": uses_enumeration,
                    "enumerated_url_count": enumerated_url_count,
                },
            )
            if attempt >= attempts or failure.get("retry_useless"):
                raise RuntimeError(
                    f"Apify run failed with status={status.get('status')} "
                    f"run_id={run_info.get('run_id')}: {failure.get('reason') or 'no further detail'}"
                )
            if not context.is_replaying:
                logger.warning(
                    "Apify run %s ended %s (attempt %d/%d); retrying in %d min. city=%s run_id=%s",
                    run_info.get("run_id"),
                    status.get("status"),
                    attempt,
                    attempts,
                    run_health.retry_delay_minutes(),
                    city,
                    run_id,
                )
            yield context.create_timer(
                context.current_utc_datetime
                + timedelta(minutes=run_health.retry_delay_minutes())
            )
            continue

        # 4 + 4b. Stream the Apify dataset straight into bronze.location_scraper_raw.
        # The full dataset is deliberately NOT returned to the orchestrator: doing
        # so serialised the whole payload into the orchestration history and
        # re-materialised it on every replay (and again as input to persist +
        # normalize), which OOM-killed the worker on large cities. Only a row
        # count comes back here.
        raw_summary: dict = yield context.call_activity(
            "ls_fetch_and_persist_raw",
            {
                "run_id": run_id,
                "dataset_id": run_info["dataset_id"],
                "source": source_config["actor"],
                "city": source_config["city"],
            },
        )
        raw_item_count = int((raw_summary or {}).get("item_count", 0))
        persisted_dataset_id = run_info["dataset_id"]

        # 4c. Grade the attempt: the actor's own per-listing detail losses
        # plus the city's recent volume.
        health: dict = yield context.call_activity(
            "ls_assess_run_health",
            {
                "run_id": run_id,
                "city": source_config["city"],
                "source": source_config["actor"],
                "apify_run_id": run_info["run_id"],
                "raw_item_count": raw_item_count,
                "used_enumeration": uses_enumeration,
                "enumerated_url_count": enumerated_url_count,
            },
        )
        if best is None or raw_item_count > int(best["raw_item_count"]):
            best = {
                "dataset_id": run_info["dataset_id"],
                "raw_item_count": raw_item_count,
                "health": health,
            }

        # retry_useless: the actor's shared unblocker budget is spent, so the
        # next two attempts would fail identically and still bill actor starts.
        if health.get("ok") or health.get("retry_useless") or attempt >= attempts:
            break

        if not context.is_replaying:
            logger.warning(
                "Degraded scrape (attempt %d/%d): %s. Retrying in %d min. city=%s run_id=%s",
                attempt,
                attempts,
                health.get("reason"),
                run_health.retry_delay_minutes(),
                city,
                run_id,
            )
        yield context.create_timer(
            context.current_utc_datetime
            + timedelta(minutes=run_health.retry_delay_minutes())
        )

    if best is None:
        raise RuntimeError(f"No Apify attempt produced a dataset. run_id={run_id}")

    # Keep the best attempt: fetch_and_persist_raw replaces the run's bronze
    # rows, so a later, weaker retry would otherwise be the one persisted.
    if best["dataset_id"] != persisted_dataset_id:
        raw_summary = yield context.call_activity(
            "ls_fetch_and_persist_raw",
            {
                "run_id": run_id,
                "dataset_id": best["dataset_id"],
                "source": base_config["actor"],
                "city": base_config["city"],
            },
        )
        raw_item_count = int((raw_summary or {}).get("item_count", 0))
    else:
        raw_item_count = int(best["raw_item_count"])
    return base_config, raw_item_count, best["health"]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

@bp.orchestration_trigger(context_name="context")
def location_scraper_orch(context: df.DurableOrchestrationContext):
    payload: dict = context.get_input()
    city: str = payload["city"]
    shape = payload.get("shape")
    run_id: str = payload["run_id"]
    try:
        # 1. Resolve city → SourceConfig
        source_config: dict = yield context.call_activity(
            "ls_resolve_source",
            {
                "city": city,
                "shape": shape,
                "run_id": run_id,
                "unlimited_items": bool(payload.get("unlimited_items")),
            },
        )

        # 1b -> 4. Scrape the city (with graded retries).
        base_config, raw_item_count, run_health_verdict = yield from _scrape_with_retries(
            context,
            city=city,
            run_id=run_id,
            source_config=source_config,
        )
        source_config = base_config

        # 5. Normalize via source adapter — reads raw payloads back from SQL by
        # run_id (paged), so the dataset never transits the orchestrator.
        listings: list[dict] = yield context.call_activity(
            "ls_normalize",
            {
                "actor": source_config["actor"],
                "run_id": run_id,
                "city": source_config["city"],
            },
        )

        # Steps 6-9 (Lusha enrichment) are skipped for sources that already
        # carry broker contact + email in the listing payload (e.g. LoopNet):
        # the broker email is persisted by ls_upsert_sql and surfaced on the
        # globe directly from the raw payload, so Lusha would be redundant cost.
        if source_config["actor"] in LUSHA_SKIP_SOURCES:
            logger.info(
                "Skipping Lusha enrichment for source=%s city=%s (broker contact comes from the listing)",
                source_config["actor"],
                source_config["city"],
            )
            enriched_agencies = []
            enrichment_diag = _summarize_enrichment_diagnostics([])
            bundles = []
        else:
            # 6. Dedupe agencies
            agencies: list[dict] = yield context.call_activity("ls_dedupe_agencies", listings)

            # 7. Filter out agencies already enriched in SQL
            new_agencies: list[dict] = yield context.call_activity(
                "ls_filter_new_agencies",
                {"agencies": agencies, "listings": listings},
            )

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
                for agency in (new_agencies or [])
            ]
            if enrich_tasks:
                enriched_agencies: list[dict] = (yield context.task_all(enrich_tasks)) or []
            else:
                enriched_agencies = []
            enrichment_diag = _summarize_enrichment_diagnostics(enriched_agencies)
            logger.info(
                "Location scraper enrichment summary run_id=%s city=%s %s",
                run_id,
                source_config["city"],
                json.dumps(enrichment_diag, sort_keys=True),
            )
            yield context.call_activity(
                "ls_write_lusha_diagnostics",
                {
                    "run_id": run_id,
                    "source": source_config["actor"],
                    "city": source_config["city"],
                    "enriched_agencies": enriched_agencies,
                },
            )

            # 9. Consolidate contacts (dedup + top-3 per agency)
            bundles: list[dict] = yield context.call_activity("ls_consolidate_contacts", enriched_agencies)

        # 10. Upsert buildings, listings, contacts to SQL
        stats: dict = yield context.call_activity(
            "ls_upsert_sql",
            {"listings": listings, "bundles": bundles, "run_id": run_id, "city": source_config["city"]},
        )
        stats["enrichment_diagnostics"] = enrichment_diag
        # A degraded run keeps its buildings (they are real) but is NOT marked
        # completed: ls_cities_needing_run then picks the city up again in the
        # mid-week pass, and the weekly alert names it.
        stats["run_status"] = "completed" if run_health_verdict.get("ok") else "degraded"
        stats["health_note"] = run_health_verdict.get("reason") or ""
        stats["health"] = run_health_verdict
        stats.update(
            compute_run_quality_payload(
                raw_item_count=raw_item_count,
                listings=listings,
                bundles=bundles,
                enrichment_diag=enrichment_diag,
            )
        )

        # 11. Write completion log
        yield context.call_activity("ls_write_logs", stats)
        yield context.call_activity("ls_materialize_globe", {"run_id": run_id})
        yield context.call_activity("ls_refresh_gold_map_markers", {"run_id": run_id})
        yield context.call_activity("ls_refresh_globe_quality", {"run_id": run_id})
        return stats
    except Exception as exc:
        yield context.call_activity(
            "ls_mark_run_failed",
            {"run_id": run_id, "error": str(exc)},
        )
        raise


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

@bp.activity_trigger(input_name="payload")
def ls_resolve_source(payload: dict) -> dict:
    cfg = resolve_act.resolve_source(
        city=payload["city"],
        shape=payload.get("shape"),
        run_id=payload["run_id"],
        unlimited_items=bool(payload.get("unlimited_items")),
    )
    return cfg.to_dict()


@bp.activity_trigger(input_name="config")
def ls_enumerate_loopnet_urls(config: dict) -> dict:
    urls = enumerate_loopnet_act.enumerate_listing_urls(config["start_url"])
    return {"listing_urls": urls, "count": len(urls)}


@bp.activity_trigger(input_name="payload")
def ls_assess_run_health(payload: dict) -> dict:
    """Grade a finished Apify run (never fatal -- a missing verdict means ok)."""
    try:
        return run_health_act.assess_run_health(payload)
    except Exception:
        logger.exception(
            "Run health assessment failed; treating the run as healthy. run_id=%s",
            payload.get("run_id"),
        )
        return {"ok": True, "status": "ok", "reason": "", "raw_item_count": int(payload.get("raw_item_count") or 0)}


@bp.activity_trigger(input_name="payload")
def ls_alert_run_health(payload: dict) -> dict:
    """Email the weekly city results when at least one did not complete."""
    try:
        return alerts_act.alert_run_health(payload)
    except Exception:
        logger.exception(
            "Could not send the weekly location scraper alert. period_key=%s",
            payload.get("period_key"),
        )
        return {"sent": False, "reason": "alert failed"}


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
def ls_persist_raw(payload: dict) -> dict:
    # Legacy: retained for back-compat. The orchestrator now uses
    # ls_fetch_and_persist_raw (streaming) instead of ls_fetch_dataset +
    # ls_persist_raw, so this is no longer on the hot path.
    try:
        return raw_act.persist_raw_items(payload)
    except Exception as exc:
        _safe_mark_run_failed(payload.get("run_id"), f"ls_persist_raw failed: {exc}")
        raise


@bp.activity_trigger(input_name="payload")
def ls_fetch_and_persist_raw(payload: dict) -> dict:
    """Stream the Apify dataset straight into bronze.location_scraper_raw.

    Replaces the ls_fetch_dataset -> ls_persist_raw pair so the full dataset
    never transits the orchestrator history (worker-OOM fix on large cities).
    """
    try:
        return raw_act.fetch_and_persist_raw(payload)
    except Exception as exc:
        _safe_mark_run_failed(payload.get("run_id"), f"ls_fetch_and_persist_raw failed: {exc}")
        raise


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
def ls_write_lusha_diagnostics(payload: dict) -> dict:
    try:
        return log_act.write_lusha_diagnostics(payload)
    except Exception as exc:
        _safe_mark_run_failed(payload.get("run_id"), f"ls_write_lusha_diagnostics failed: {exc}")
        raise


@bp.activity_trigger(input_name="payload")
def ls_upsert_sql(payload: dict) -> dict:
    try:
        return persist_act.upsert_sql(payload)
    except Exception as exc:
        _safe_mark_run_failed(payload.get("run_id"), f"ls_upsert_sql failed: {exc}")
        raise


@bp.activity_trigger(input_name="stats")
def ls_write_logs(stats: dict) -> None:
    try:
        log_act.write_logs(stats)
    except Exception as exc:
        _safe_mark_run_failed(stats.get("run_id"), f"ls_write_logs failed: {exc}")
        raise


@bp.activity_trigger(input_name="payload")
def ls_mark_run_failed(payload: dict) -> None:
    run_id = payload.get("run_id")
    if not run_id:
        return
    _safe_mark_run_failed(run_id, payload.get("error", "unknown error"))


@bp.activity_trigger(input_name="payload")
def ls_init_run_log(payload: dict) -> None:
    """Upsert a RUNNING log row for a weekly city before its sub-orchestration."""
    try:
        log_act.init_run_log(payload["run_id"], payload["city"])
    except Exception:
        logger.exception("Could not init weekly log row for run_id=%s", payload.get("run_id"))


@bp.activity_trigger(input_name="payload")
def ls_cities_needing_run(payload: dict) -> list:
    """Return the subset of candidate cities not yet completed this week.

    payload = {"candidates": [{"city": str, "run_id": str}, ...], "force_full": bool}
    With force_full, returns all candidates (full re-scrape).
    """
    candidates = payload.get("candidates") or []
    if payload.get("force_full"):
        return candidates
    try:
        completed = log_act.completed_run_ids([c["run_id"] for c in candidates])
    except Exception:
        logger.exception("Could not query completed weekly runs; running all candidates")
        return candidates
    return [c for c in candidates if c["run_id"] not in completed]


@bp.activity_trigger(input_name="payload")
def ls_materialize_globe(payload: dict) -> dict:
    try:
        return materialize_act.materialize_globe_run(payload)
    except Exception as exc:
        _safe_mark_run_failed(payload.get("run_id"), f"ls_materialize_globe failed: {exc}")
        raise


@bp.activity_trigger(input_name="payload")
def ls_refresh_gold_map_markers(payload: dict) -> dict:
    try:
        return materialize_act.refresh_gold_map_markers(payload)
    except Exception as exc:
        logger.exception(
            "location_scraper gold map marker refresh failed but scrape output remains usable. run_id=%s",
            payload.get("run_id"),
        )
        return {
            "run_id": payload.get("run_id", ""),
            "status": "skipped_error",
            "error": str(exc),
        }


@bp.activity_trigger(input_name="payload")
def ls_refresh_globe_quality(payload: dict) -> dict:
    try:
        return materialize_act.refresh_globe_quality(payload)
    except Exception as exc:
        logger.exception(
            "location_scraper globe quality refresh failed but scrape output remains usable. run_id=%s",
            payload.get("run_id"),
        )
        return {
            "run_id": payload.get("run_id", ""),
            "status": "skipped_error",
            "error": str(exc),
        }
