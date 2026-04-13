"""
functions/xero_sync.py

Timer trigger: syncs Xero invoices from all connected tenants into bronze/silver.
Runs daily at 04:00 UTC (after the Nexudus pipeline at 02:00-03:00 UTC).

Schedule override: set XERO_INVOICE_SYNC_SCHEDULE env var (cron syntax).
Force full resync: set XERO_INVOICE_SYNC_FORCE_FULL=1 env var.
"""
from __future__ import annotations

import json
import logging
import os

import azure.functions as func

from shared.xero.invoice_sync import XeroInvoiceSyncService

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("XERO_INVOICE_SYNC_SCHEDULE", "0 0 4 * * *")  # 04:00 UTC daily


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def xero_invoice_sync(timer: func.TimerRequest) -> None:
    """Incremental Xero invoice sync across all connected tenants."""
    force_full = os.getenv("XERO_INVOICE_SYNC_FORCE_FULL", "0") == "1"
    logger.info(
        "Xero invoice sync started",
        extra={"force_full": force_full, "past_due": timer.past_due},
    )

    try:
        service = XeroInvoiceSyncService()
        stats = service.sync_invoices(
            owner_type="workspace",
            owner_id="default",
            force_full=force_full,
        )
        logger.info("Xero invoice sync complete", extra={"stats": json.dumps(stats, default=str)})

        if stats.get("failed_tenant_ids"):
            logger.warning(
                "Some tenants failed during Xero sync",
                extra={"failed": stats["failed_tenant_ids"]},
            )

        # Cache PDFs for any invoices (any status) that don't have one yet.
        # This reuses the shared backfill logic so the ETL timer gets retry/pacing safeguards too.
        pdf_stats = service.cache_missing_pdfs()
        logger.info("Xero PDF cache complete", extra={"pdf_stats": json.dumps(pdf_stats, default=str)})

    except Exception:
        logger.exception("Xero invoice sync failed")
        raise
