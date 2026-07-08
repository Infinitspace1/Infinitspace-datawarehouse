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

from shared.xero.bank_transaction_sync import XeroBankTransactionSyncService
from shared.xero.invoice_sync import XeroInvoiceSyncService
from shared.xero.profit_loss_sync import XeroProfitLossSyncService

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

        # Bank transactions (spend/receive money) — bank fees never appear on
        # ACCPAY invoices, so these feed the P&L actuals gap. Requires the
        # accounting.transactions.read scope; tenants on a token missing
        # it are skipped with a warning until the OAuth re-consent lands.
        bank_force_full = os.getenv("XERO_BANK_TX_SYNC_FORCE_FULL", "0") == "1"
        bank_stats = XeroBankTransactionSyncService().sync_bank_transactions(
            owner_type="workspace",
            owner_id="default",
            force_full=bank_force_full,
        )
        logger.info(
            "Xero bank transaction sync complete",
            extra={"bank_stats": json.dumps(bank_stats, default=str)},
        )
        if bank_stats.get("scope_skipped_tenant_ids"):
            logger.warning(
                "Bank transactions skipped for tenants missing the accounting.transactions.read scope",
                extra={"skipped": bank_stats["scope_skipped_tenant_ids"]},
            )

        # Monthly Xero-computed Profit & Loss. This is the canonical budget-tool
        # actuals feed because it includes invoices, credit notes, payroll
        # journals, deferred income, and other Xero-native postings by design.
        profit_loss_force_full = os.getenv("XERO_PROFIT_LOSS_SYNC_FORCE_FULL", "0") == "1"
        profit_loss_stats = XeroProfitLossSyncService().sync_profit_loss(
            owner_type="workspace",
            owner_id="default",
            force_full=profit_loss_force_full,
        )
        logger.info(
            "Xero Profit & Loss sync complete",
            extra={"profit_loss_stats": json.dumps(profit_loss_stats, default=str)},
        )
        if profit_loss_stats.get("scope_skipped_tenant_ids"):
            logger.warning(
                "Profit & Loss skipped for tenants missing accounting.reports.read/settings scope",
                extra={"skipped": profit_loss_stats["scope_skipped_tenant_ids"]},
            )

    except Exception:
        logger.exception("Xero invoice sync failed")
        raise
