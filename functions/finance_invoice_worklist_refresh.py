"""
functions/finance_invoice_worklist_refresh.py

HTTP POST /api/finance/refresh-invoice-worklist

On-demand endpoint called from the finance dashboard to get a fresh
snapshot of the invoice worklist without waiting for the nightly schedule.

Steps (sequential — order matters: invoices before lines due to FK):
  1. Silver: coworker_invoices        (paid/unpaid status changes on payment)
  2. Silver: coworker_invoice_lines   (workflow_type classification)
  3. Silver: coworkers                (billing email / name — rarely changes)
  4. Gold:   sp_refresh_invoice_worklist

Each silver step is incremental: only bronze rows changed since the last
successful silver run for that entity are processed, so this is fast when
nothing has changed (~0s per entity) and proportional to actual changes.

Auth: FUNCTION key (?code= or x-functions-key header).

Response 200:
    {
        "ok": true,
        "silver": {
            "coworker_invoices": <rows_written>,
            "coworker_invoice_lines": <rows_written>,
            "coworkers": <rows_written>
        },
        "gold_rows": <total rows in worklist after rebuild>,
        "duration_s": <float>
    }

Response 500:
    {"ok": false, "error": "<message>", "duration_s": <float>}
"""
from __future__ import annotations

import json
import logging
import time
import uuid

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_writer_coworker_invoices import SilverCoworkerInvoicesWriter
from shared.azure_clients.silver_writer_coworker_invoice_lines import SilverCoworkerInvoiceLinesWriter
from shared.azure_clients.silver_writer_coworkers import SilverCoworkersWriter
from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

bp = func.Blueprint()

_GOLD_SP = "EXEC gold.sp_refresh_invoice_worklist"
_GOLD_COUNT = "SELECT COUNT(1) FROM gold.finance_dashboard_invoice_worklist"


def _json(payload: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload),
        status_code=status_code,
        mimetype="application/json",
    )


@bp.route(
    route="finance/refresh-invoice-worklist",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
async def refresh_invoice_worklist(req: func.HttpRequest) -> func.HttpResponse:
    """Refresh invoice-related silver entities then rebuild the gold invoice worklist."""
    logger.info("Finance invoice worklist on-demand refresh started")
    t_start = time.monotonic()
    sync_run_id = uuid.uuid4()
    silver_rows: dict[str, int] = {}

    try:
        # 1. coworker_invoices — must come before lines (FK dependency)
        async with RunTracker(
            "nexudus", "coworker_invoices", "silver", triggered_by="http"
        ) as run:
            result = SilverCoworkerInvoicesWriter(sync_run_id).run()
            run.rows_read = int(result.get("rows_read") or 0)
            run.rows_written = int(result.get("coworker_invoices") or 0)
            silver_rows["coworker_invoices"] = run.rows_written

        # 2. coworker_invoice_lines
        async with RunTracker(
            "nexudus", "coworker_invoice_lines", "silver", triggered_by="http"
        ) as run:
            result = SilverCoworkerInvoiceLinesWriter(sync_run_id).run()
            run.rows_read = int(result.get("rows_read") or 0)
            run.rows_written = int(result.get("coworker_invoice_lines") or 0)
            silver_rows["coworker_invoice_lines"] = run.rows_written

        # 3. coworkers — billing email used for reminder recipient resolution
        async with RunTracker(
            "nexudus", "coworkers", "silver", triggered_by="http"
        ) as run:
            result = SilverCoworkersWriter(sync_run_id).run()
            run.rows_read = int(result.get("rows_read") or 0)
            run.rows_written = int(result.get("coworkers") or 0)
            silver_rows["coworkers"] = run.rows_written

        # 4. Rebuild gold invoice worklist (skips user_access — BambooHR is daily-only)
        async with RunTracker(
            "finance_dashboard", "invoice_worklist", "gold", triggered_by="http"
        ) as run:
            sql = get_sql_client()
            sql.execute_non_query(_GOLD_SP)
            gold_rows = int(sql.execute_scalar(_GOLD_COUNT) or 0)
            run.rows_written = gold_rows

        duration = round(time.monotonic() - t_start, 1)
        logger.info(
            "Finance invoice worklist refresh complete in %.1fs — silver=%s gold_rows=%d",
            duration,
            silver_rows,
            gold_rows,
        )
        return _json({"ok": True, "silver": silver_rows, "gold_rows": gold_rows, "duration_s": duration})

    except Exception as exc:
        duration = round(time.monotonic() - t_start, 1)
        logger.error(
            "Finance invoice worklist refresh failed after %.1fs: %s",
            duration,
            exc,
            exc_info=True,
        )
        return _json({"ok": False, "error": str(exc), "duration_s": duration}, status_code=500)
