"""
functions/finance_invoice_worklist_refresh.py

HTTP POST /api/finance/refresh-invoice-worklist

On-demand endpoint called from the finance dashboard (the header "Sync"
button) to get a fresh snapshot of the invoice worklist without waiting for
the nightly schedule.

Steps (sequential — order matters: invoices before lines due to FK):
  0. Bronze: open-invoice resync      (LIVE Nexudus re-fetch, by ID — see below)
  1. Silver: coworker_invoices        (paid/unpaid status changes on payment)
  2. Silver: coworker_invoice_lines   (workflow_type classification)
  3. Silver: coworkers                (billing email / name — rarely changes)
  4. Gold:   sp_refresh_invoice_worklist

Step 0 (added 2026-08-10): without it this endpoint only re-promoted the same
~02:00 bronze snapshot — every silver count came back 0 and the dashboard
never changed, which is exactly how the sync button "did nothing" for CMs.
`_resync_open_invoices` re-fetches every open unpaid invoice OBJECT from
Nexudus by ID (measured 2–46 s), so payments/credits applied since the nightly
run reach the worklist. It does NOT fetch payment histories (the `processing`
flag) or brand-new invoices created since the nightly window — those remain
nightly/pre-send concerns. Nexudus auth/API failure in step 0 degrades
gracefully: the error is reported in the response and the silver→gold refresh
still runs on the existing bronze.

Note the bronze resync run is tracked under the same
(nexudus, coworker_invoices_resync, bronze) entity as the nightly run, so a
manual sync later in the day replaces the nightly row in the sync-health
report. Button presses are sporadic (unlike the daily pre-send job, which uses
an entity suffix for this reason) — acceptable.

Each silver step is incremental: only bronze rows changed since the last
successful silver run for that entity are processed, so this is fast when
nothing has changed (~0s per entity) and proportional to actual changes —
after step 0 the silver `coworker_invoices` count IS "how many invoices
actually changed in Nexudus since the last refresh".

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
        "duration_s": <float>,
        "bronze_error": "<message>"   # only present when step 0 failed
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

from functions.bronze_nexudus import _resync_open_invoices
from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.bronze_writer import BronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_writer_coworker_invoices import SilverCoworkerInvoicesWriter
from shared.azure_clients.silver_writer_coworker_invoice_lines import SilverCoworkerInvoiceLinesWriter
from shared.azure_clients.silver_writer_coworkers import SilverCoworkersWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient

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
    bronze_error: str | None = None

    try:
        # 0. Bronze: re-fetch open invoices from Nexudus so the refresh actually
        #    pulls LIVE data instead of re-promoting the nightly snapshot.
        #    Best-effort: a Nexudus outage shouldn't kill the silver->gold pass.
        try:
            bearer_token = get_bearer_token()
            async with NexudusClient(bearer_token) as client:
                await _resync_open_invoices(
                    client, BlobWriter(), BronzeWriter(sync_run_id), sync_run_id,
                )
        except Exception as exc:  # noqa: BLE001 — degrade, don't die
            bronze_error = str(exc)
            logger.error(
                "Open-invoice resync failed (continuing with existing bronze): %s",
                exc,
                exc_info=True,
            )

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
            "Finance invoice worklist refresh complete in %.1fs — silver=%s gold_rows=%d bronze_error=%s",
            duration,
            silver_rows,
            gold_rows,
            bronze_error,
        )
        payload = {"ok": True, "silver": silver_rows, "gold_rows": gold_rows, "duration_s": duration}
        if bronze_error:
            payload["bronze_error"] = bronze_error
        return _json(payload)

    except Exception as exc:
        duration = round(time.monotonic() - t_start, 1)
        logger.error(
            "Finance invoice worklist refresh failed after %.1fs: %s",
            duration,
            exc,
            exc_info=True,
        )
        return _json({"ok": False, "error": str(exc), "duration_s": duration}, status_code=500)
