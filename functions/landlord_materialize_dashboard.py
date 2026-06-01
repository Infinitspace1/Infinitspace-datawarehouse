"""
functions/landlord_materialize_dashboard.py

Materializes the slow gold.vw_landlord_*_monthly views into physical tables
(gold.t_landlord_*_monthly) on a timer. The strategic-partnership dashboard
reads from the tables instead of the views — at Aldgate-scale (~4k contracts)
the views take 6+ seconds per query because of the recursive month spine plus
CROSS APPLY STRING_SPLIT(floor_plan_desk_ids). Table reads are <50ms.

What it does:
    Every refresh window:
      1. SELECT all rows from gold.vw_landlord_contract_book_monthly
      2. TRUNCATE gold.t_landlord_contract_book_monthly
      3. Bulk INSERT the new rows + refreshed_at = utcnow
      Repeated for the membership-book and invoice-revenue views.
    Each table refresh is in its own transaction so readers never see an
    empty / partial table.

Schedule:
    LANDLORD_MATERIALIZE_DASHBOARD_SCHEDULE  (NCRONTAB; default "0 0 3 * * *")
    = 03:00 UTC daily — sits between the silver sync (02:30 UTC) and the
    sync-health email (06:00 UTC), so the snapshot always reflects the latest
    silver refresh. Override the env var for a different cadence if needed
    (e.g. "0 0 */6 * * *" for every 6 hours).

Staleness:
    Silver itself only refreshes once per day, so running materialization
    more often than that is wasted compute. The dashboard surfaces
    refreshed_at as "Last refreshed" badge so users can see freshness.

Idempotency:
    Full TRUNCATE + INSERT — safe to run any time. If the silver layer is
    mid-sync, we may capture an intermediate state, but the next refresh
    will overwrite it. Not run during silver sync explicitly is not a
    requirement — the gold views are read-only and silver writes are atomic
    per row.
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("LANDLORD_MATERIALIZE_DASHBOARD_SCHEDULE", "0 0 3 * * *")


# ── Refresh specs ─────────────────────────────────────────────────────────────
# Each spec: (source_view, target_table, column_list, select_clause).
# Column order in INSERT MUST match the SELECT, which MUST match the table DDL
# in scripts/sql_scripts/landlord_dashboard_materialized.sql.
_REFRESHES = [
    {
        "name": "contract_book_monthly",
        "source": "gold.vw_landlord_contract_book_monthly",
        "target": "gold.t_landlord_contract_book_monthly",
        "columns": [
            "period", "month_start_date", "location_source_id", "location_name",
            "location_city", "location_country_name", "total_workstation_capacity",
            "active_contract_count", "occupied_workstations", "vacant_workstations",
            "occupancy_pct", "sold_monthly_revenue", "list_monthly_revenue",
            "avg_sold_price_per_ws", "avg_list_price_per_ws", "avg_discount_pct",
            "discount_monthly_value", "private_office_contract_count",
            "private_office_capacity", "private_office_sold_revenue",
            "private_office_list_revenue", "new_workstations_starting",
            "workstations_cancelling", "net_workstation_change",
            "contracts_missing_list_price", "adjustment_contract_count",
            "adjustment_monthly_value", "calculation_basis",
        ],
    },
    {
        "name": "membership_book_monthly",
        "source": "gold.vw_landlord_membership_book_monthly",
        "target": "gold.t_landlord_membership_book_monthly",
        "columns": [
            "period", "month_start_date", "location_source_id", "location_name",
            "location_city", "location_country_name", "total_workstation_capacity",
            "active_contract_count", "occupied_workstations", "vacant_workstations",
            "occupancy_pct", "sold_monthly_revenue", "list_monthly_revenue",
            "avg_sold_price_per_ws", "avg_list_price_per_ws",
            "adjustment_contract_count", "adjustment_monthly_value",
            "calculation_basis",
        ],
    },
    {
        "name": "revenue_past_location_monthly",
        "source": "gold.vw_landlord_revenue_past_location_monthly",
        "target": "gold.t_landlord_revenue_past_location_monthly",
        "columns": [
            "period", "month_start_date", "location_source_id", "location_name",
            "currency_code", "sold_monthly_revenue", "line_count",
            "negative_line_count", "member_count",
        ],
    },
]


def _refresh_one(sql, spec: dict, now_iso: str) -> tuple[int, float]:
    """TRUNCATE + INSERT one table. Returns (rows_written, elapsed_seconds)."""
    import time
    cols = ", ".join(spec["columns"])
    select_cols = ", ".join(spec["columns"])
    target = spec["target"]
    source = spec["source"]

    t0 = time.time()
    # The view query is the expensive part; doing it inside an INSERT…SELECT
    # avoids round-tripping the rows to the Function and back. Wrapping in
    # a transaction keeps readers from seeing an empty table mid-refresh.
    sql.execute_non_query(f"""
        BEGIN TRANSACTION;
        TRUNCATE TABLE {target};
        INSERT INTO {target} ({cols}, refreshed_at)
        SELECT {select_cols}, CAST(? AS DATETIME2(0))
        FROM {source};
        COMMIT TRANSACTION;
    """, (now_iso,))
    elapsed = time.time() - t0

    # Row count after refresh — cheap and tells the operator if a refresh
    # silently produced 0 rows (would happen if the source view broke).
    n_rows = sql.execute_query(f"SELECT COUNT(*) AS c FROM {target}")[0]["c"]
    return n_rows, elapsed


async def _run_refresh(trigger_source: str) -> dict:
    """Shared refresh routine used by both the timer trigger and the HTTP
    trigger. Returns a small dict summary so the HTTP caller can render
    something useful."""
    now = datetime.now(timezone.utc)
    now_iso = now.replace(microsecond=0).isoformat(sep=" ")
    run_id = uuid.uuid4()

    logger.info(
        "Landlord materialize starting: run_id=%s trigger=%s timestamp=%s",
        run_id, trigger_source, now_iso,
    )

    async with RunTracker(
        "landlord_dashboard", "materialize_dashboard", "gold",
        metadata=f"run_id={run_id} trigger={trigger_source}",
    ) as run:
        sql = get_sql_client()
        per_table = []
        total_rows = 0

        for spec in _REFRESHES:
            try:
                n_rows, elapsed = _refresh_one(sql, spec, now_iso)
                total_rows += n_rows
                per_table.append({"target": spec["target"], "rows": n_rows, "elapsed_s": round(elapsed, 2)})
                logger.info(
                    "Refreshed %s: %d rows in %.2fs",
                    spec["target"], n_rows, elapsed,
                )
                if n_rows == 0:
                    run.log_error(
                        spec["name"],
                        RuntimeError(f"Refresh produced 0 rows for {spec['target']}"),
                        raw_payload=f"source={spec['source']}",
                    )
            except Exception as exc:
                logger.exception("Failed to refresh %s", spec["target"])
                per_table.append({"target": spec["target"], "error": str(exc)})
                run.log_error(
                    spec["name"],
                    exc,
                    raw_payload=f"source={spec['source']} target={spec['target']}",
                )

        run.rows_written = total_rows
        logger.info(
            "Landlord materialize complete: run_id=%s total_rows=%d",
            run_id, total_rows,
        )
        return {
            "run_id": str(run_id),
            "trigger": trigger_source,
            "timestamp": now_iso,
            "total_rows_written": total_rows,
            "tables": per_table,
        }


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def landlord_materialize_dashboard(timer: func.TimerRequest) -> None:
    """Daily 03:00 UTC refresh of the materialized landlord-dashboard tables."""
    await _run_refresh(trigger_source="timer")


# HTTP trigger — lets the strategic-partnership Flask app refresh the
# materialized tables on demand (e.g. after ops fixes data in Nexudus and
# wants the dashboard to reflect it without waiting for the next daily run).
# Auth is function-key based: caller must pass the function key in the
# `x-functions-key` header or `?code=` query param. The Flask app stores the
# key as LANDLORD_MATERIALIZE_FUNCTION_KEY.
@bp.route(route="landlord_materialize_dashboard/refresh", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
async def landlord_materialize_dashboard_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP-triggered refresh. Returns JSON with per-table row counts."""
    import json
    try:
        result = await _run_refresh(trigger_source="http")
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json",
        )
    except Exception as exc:
        logger.exception("HTTP-triggered refresh failed")
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=500,
            mimetype="application/json",
        )
