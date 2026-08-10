"""
functions/nexudus_dashboard_refresh.py

Blueprint: HTTP-triggered on-demand Nexudus refresh for the strategic-
partnership dashboard's "Refresh data" button (2026-08-10).

The nightly pipeline (bronze 02:00 → silver 02:30 → materialize 03:00 UTC)
leaves the dashboard up to a day behind Nexudus. This route runs the same
code path inline for the small, contract-shaped entities so an ops change in
Nexudus (new contract, moved desks, availability dates) is visible on the
dashboard minutes later:

  1. Bronze: incremental UpdatedSince fetch (reuses bronze_nexudus._sync_*,
     so watermarks in meta.sync_runs advance exactly like the nightly run).
  2. Silver: the same writer classes silver_worker runs from the queue,
     executed inline so the response can report completion.
  3. Materialize: TRUNCATE+INSERT of gold.t_landlord_*_monthly (same specs
     as the 03:00 cron), so the dashboard — which reads the t_ tables —
     sees the change immediately.

Deliberately EXCLUDED by default: coworker_invoices / invoice lines /
events / coworkers — the big entities that make the nightly run long. The
button's job is "my contract edit from this morning", not a full resync.
POST body can override:

    POST /api/nexudus_dashboard_refresh   (x-functions-key: <key>)
    {"entities": ["locations","products","contracts","tariffs",
                  "financial_accounts"],          // optional, this default
     "materialize": true,                          // optional, default true
     "finance_snapshot": false}                    // optional, default false:
                                                   // also EXEC gold.sp_refresh_
                                                   // finance_dashboard (heavier,
                                                   // rebuilds invoice KPIs too)

Response: 200 with per-step row counts and timings, 500 with the error.
Everything here is idempotent (hash-guarded bronze writes, MERGE silver
upserts, transactional TRUNCATE+INSERT), so concurrent or repeated calls
are safe — just wasteful.
"""
import json
import logging
import time
import uuid
from datetime import datetime, timezone

import azure.functions as func

from functions.bronze_nexudus import (
    _sync_contracts,
    _sync_financial_accounts,
    _sync_locations,
    _sync_products,
    _sync_tariffs,
)
from functions.landlord_materialize_dashboard import _REFRESHES, _refresh_one
from functions.silver_worker import _ENTITY_MAP
from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.bronze_writer import BronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient

logger = logging.getLogger(__name__)

bp = func.Blueprint()

# Contract-shaped entities the dashboard needs fresh. Order matters:
# products/contracts reference locations; tariffs/financial_accounts drive
# the membership-fee filter.
DEFAULT_ENTITIES = ["locations", "products", "contracts", "tariffs", "financial_accounts"]


async def _run_bronze(entities: list[str], run_id: uuid.UUID) -> dict:
    """Incremental bronze fetch for the requested entities. Returns
    {entity: seconds}. Unknown entities were filtered out by the caller."""
    timings: dict = {}
    bearer_token = get_bearer_token()
    async with NexudusClient(bearer_token) as client:
        blob_writer = BlobWriter()
        writer = BronzeWriter(run_id)

        t0 = time.time()
        locations = await _sync_locations(client, blob_writer, writer, run_id)
        timings["locations"] = round(time.time() - t0, 1)

        products: list[dict] = []
        if "products" in entities:
            t0 = time.time()
            products, _resource_ids = await _sync_products(
                client, blob_writer, writer, run_id, locations)
            timings["products"] = round(time.time() - t0, 1)

        if "contracts" in entities:
            t0 = time.time()
            await _sync_contracts(client, blob_writer, writer, run_id, products)
            timings["contracts"] = round(time.time() - t0, 1)

        if "tariffs" in entities:
            t0 = time.time()
            await _sync_tariffs(client, blob_writer, writer, run_id)
            timings["tariffs"] = round(time.time() - t0, 1)

        if "financial_accounts" in entities:
            t0 = time.time()
            await _sync_financial_accounts(client, blob_writer, writer, run_id)
            timings["financial_accounts"] = round(time.time() - t0, 1)
    return timings


async def _run_silver(entities: list[str], sync_run_id: uuid.UUID) -> dict:
    """Inline bronze→silver transformation per entity (same writers the
    queue worker uses). Returns {entity: {"rows": n, "seconds": s}}."""
    results: dict = {}
    for entity in entities:
        writer_cls, rows_fn = _ENTITY_MAP[entity]
        t0 = time.time()
        async with RunTracker(
            "nexudus", entity, "silver",
            triggered_by="http", metadata=str(sync_run_id),
        ) as run:
            result = writer_cls(sync_run_id).run()
            run.rows_read = int(result.get("rows_read") or 0)
            run.rows_written = rows_fn(result)
        results[entity] = {"rows": rows_fn(result), "seconds": round(time.time() - t0, 1)}
    return results


@bp.route(
    route="nexudus_dashboard_refresh",
    auth_level=func.AuthLevel.FUNCTION,
    methods=["POST"],
)
async def nexudus_dashboard_refresh(req: func.HttpRequest) -> func.HttpResponse:
    started = time.time()
    try:
        body = req.get_json() if req.get_body() else {}
    except ValueError:
        body = {}

    requested = body.get("entities") or DEFAULT_ENTITIES
    entities = [e for e in requested if e in _ENTITY_MAP]
    # locations is always synced (bronze products/contracts take its output);
    # keep it in the silver list too so name/state changes propagate.
    if "locations" not in entities:
        entities.insert(0, "locations")
    materialize = body.get("materialize", True)
    finance_snapshot = body.get("finance_snapshot", False)

    run_id = uuid.uuid4()
    logger.info("Dashboard refresh started [run_id=%s entities=%s]", run_id, entities)

    try:
        bronze_timings = await _run_bronze(entities, run_id)
        silver_results = await _run_silver(entities, run_id)

        materialized: dict = {}
        if materialize:
            sql = get_sql_client()
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            for spec in _REFRESHES:
                rows, secs = _refresh_one(sql, spec, now_iso)
                materialized[spec["name"]] = {"rows": rows, "seconds": round(secs, 1)}

        if finance_snapshot:
            sql = get_sql_client()
            t0 = time.time()
            sql.execute_non_query("EXEC gold.sp_refresh_finance_dashboard")
            materialized["finance_dashboard"] = {"seconds": round(time.time() - t0, 1)}

        payload = {
            "status": "ok",
            "run_id": str(run_id),
            "entities": entities,
            "bronze_seconds": bronze_timings,
            "silver": silver_results,
            "materialized": materialized,
            "total_seconds": round(time.time() - started, 1),
        }
        logger.info("Dashboard refresh complete [run_id=%s %.1fs]", run_id, time.time() - started)
        return func.HttpResponse(
            json.dumps(payload), status_code=200, mimetype="application/json")

    except Exception as exc:
        logger.error("Dashboard refresh failed [run_id=%s]: %s", run_id, exc, exc_info=True)
        return func.HttpResponse(
            json.dumps({"status": "error", "run_id": str(run_id), "error": str(exc)}),
            status_code=500, mimetype="application/json")
