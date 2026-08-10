"""Pre-send refresh of Nexudus payment-result histories.

WHY THIS EXISTS
---------------
The finance dashboard suppresses an invoice from its worklist while a direct
debit is genuinely in flight (`gold.sp_refresh_invoice_worklist`:
`AND ISNULL(nci.processing, 0) = 0`). That gate is load-bearing: collections
start on the due date itself and take a median 5.9 days to clear, while the
app's recurrent pre-reminders fire at end-of-month −5 / −2 / 0. Measured over
457 real collections, the first pre-reminder lands INSIDE the live collection
window 77.5% of the time. The gate is the only thing stopping ~80% of direct
debit payers being chased mid-collection.

But `processing` is derived from Nexudus invoice histories, and until this job
existed those were fetched ONLY by the ~02:00 UTC nightly `nexudus_to_bronze`
run. Neither the 05:30 nor the 10:00 gold rebuild re-reads Nexudus, and the app's
`POST /api/finance/refresh-invoice-worklist` re-reads the same 02:00 snapshot.
The automated pre-reminders send at 08:00 UTC (Berlin/Amsterdam) and 09:00 UTC
(London), so the flag was **6–7 hours stale at send time**.

It held only by coincidence: Nexudus submits 87% of collections between 22:00
and 02:00 UTC and 96% on `due_date − 1`, so almost everything landed before the
02:00 fetch. Measured over 91 days there were 27 windows where an invoice went in
flight after the fetch but before the send; none happened to collide with a
scheduled reminder. That is luck, not design — if Nexudus ever moves its
collection job later in the day, we start chasing live collections in bulk.

This job closes the window by refreshing histories → silver → the gold worklist
at 07:00 UTC, an hour before the earliest send.

It deliberately does NOT touch coworkers / products / contracts / events — the
nightly 02:00 run owns those.

Ordering constraint: MUST finish before 08:00 UTC. Measured worst case from
`meta.sync_runs` over 45 days: history fetch 713 s + open-invoice resync 46 s +
silver 73 s + gold SP 51 s ≈ 883 s, so a 07:00 start lands by ~07:15 with ~45
minutes of headroom.

NO collapse guardrail here (unlike `finance_dashboard_refresh._is_collapse`): a
month-end morning where many direct debits are genuinely in flight is exactly
when the worklist SHOULD shrink, and a rollback guard would defeat the purpose.
The baseline → new counts are logged instead.
"""
from __future__ import annotations

import logging
import os
import time
import uuid

import azure.functions as func

from functions.bronze_nexudus import (
    _resync_open_invoices,
    _sync_coworker_invoice_histories,
)
from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.bronze_writer import BronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_writer_coworker_invoices import (
    SilverCoworkerInvoicesWriter,
)
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("FINANCE_PRESEND_REFRESH_SCHEDULE", "0 0 7 * * *")

_GOLD_SP = "EXEC gold.sp_refresh_invoice_worklist"
_GOLD_COUNT = "SELECT COUNT(1) FROM gold.finance_dashboard_invoice_worklist"


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def finance_presend_refresh(timer: func.TimerRequest) -> None:
    """Refresh DD payment histories → silver → gold worklist before the sends."""
    if not _env_flag("FINANCE_PRESEND_REFRESH_ENABLED", True):
        logger.info(
            "Finance pre-send refresh disabled (FINANCE_PRESEND_REFRESH_ENABLED)"
        )
        return

    t_start = time.monotonic()
    logger.info("Finance pre-send refresh started")

    try:
        bearer_token = get_bearer_token()
    except EnvironmentError as exc:
        logger.error("Auth failed: %s", exc)
        raise

    # 0 / unset => inherit the nightly NEXUDUS_INVOICE_HISTORY_LOOKBACK_MONTHS.
    # Kept as a tuning knob only: the candidate query is deliberately the SAME
    # one the nightly run uses, because an invoice omitted here would keep its
    # 02:00 flag — i.e. silently reintroduce the very staleness this job removes.
    lookback = int(os.getenv("FINANCE_PRESEND_HISTORY_LOOKBACK_MONTHS", "0")) or None

    async with RunTracker("finance_dashboard", "presend_refresh", "gold") as top:
        run_id = uuid.uuid4()
        blob_writer = BlobWriter()
        writer = BronzeWriter(run_id)

        # 1 + 2 — bronze. The `_presend` entity suffix keeps these runs from
        # masking the 02:00 nightly runs in the 06:00 sync-health report, which
        # keeps only the latest run per (source, entity, layer).
        async with NexudusClient(bearer_token) as client:
            await _sync_coworker_invoice_histories(
                client,
                blob_writer,
                writer,
                run_id,
                entity_suffix="_presend",
                lookback_months=lookback,
            )
            # Cheap (2–46 s) and additionally drops invoices paid overnight out
            # of the worklist before the send.
            await _resync_open_invoices(
                client, blob_writer, writer, run_id, entity_suffix="_presend",
            )

        # 3 — silver. MUST use entity="coworker_invoices"/layer="silver" so the
        # incremental watermark inside SilverCoworkerInvoicesWriter advances
        # (same contract as functions/finance_invoice_worklist_refresh.py).
        # `_load_latest_bronze` re-transforms an invoice when its own bronze row
        # OR any of its histories changed since the watermark, which is exactly
        # what this job just touched.
        async with RunTracker(
            "nexudus",
            "coworker_invoices",
            "silver",
            triggered_by="timer",
            metadata="presend_refresh",
        ) as run:
            result = SilverCoworkerInvoicesWriter(run_id).run()
            run.rows_read = int(result.get("rows_read") or 0)
            run.rows_written = int(result.get("coworker_invoices") or 0)

        # 4 — gold. The focused SP, same as the HTTP refresh endpoint;
        # user_access is BambooHR-daily and already current at 07:00.
        sql = get_sql_client()
        baseline = int(sql.execute_scalar(_GOLD_COUNT) or 0)
        sql.execute_non_query(_GOLD_SP)
        gold_rows = int(sql.execute_scalar(_GOLD_COUNT) or 0)
        top.rows_read = baseline
        top.rows_written = gold_rows

        logger.info(
            "Finance pre-send refresh complete in %.1fs — invoice worklist %s -> %s "
            "(%+d; a NEGATIVE delta is normal and expected — it is direct debits "
            "going in flight, which is the whole point of this job)",
            time.monotonic() - t_start,
            baseline,
            gold_rows,
            gold_rows - baseline,
        )
