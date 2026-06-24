"""
functions/nexudus_invoice_pdf_cache.py

Timer trigger: downloads and caches Nexudus coworker invoice PDFs to
Azure Blob Storage for invoices that don't have a pdf_blob_path yet.

Runs after silver sync completes (03:30 UTC by default) so all invoices
are up to date before PDFs are fetched.
"""
from __future__ import annotations

import logging
import os

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient
from shared.nexudus.invoice_pdf_cache import cache_missing_nexudus_pdfs

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("NEXUDUS_PDF_CACHE_SCHEDULE", "0 30 3 * * *")


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def nexudus_invoice_pdf_cache(timer: func.TimerRequest) -> None:
    """Cache PDFs for Nexudus invoices missing a blob path."""
    logger.info("Nexudus invoice PDF cache started")

    try:
        bearer_token = get_bearer_token()
    except EnvironmentError as e:
        logger.error("Auth failed: %s", e)
        raise

    try:
        async with RunTracker("nexudus", "invoice_pdfs", "silver") as run:
            async with NexudusClient(bearer_token) as client:
                stats = await cache_missing_nexudus_pdfs(client)

            run.rows_read = stats["pdfs_total"]
            run.rows_written = stats["pdfs_cached"]
            run.rows_skipped = stats["pdfs_skipped"]

            logger.info(
                "Nexudus invoice PDF cache complete: %s",
                stats,
            )

            # Fail the run if we had candidates and every fetch errored — this
            # is the regression signal (e.g. the run-command contract changed or
            # temp URLs consistently expire). Partial failures self-heal next
            # run, so only an all-failed run trips the health report.
            if stats["pdfs_total"] > 0 and stats["pdfs_cached"] == 0 and stats["pdfs_failed"] > 0:
                raise RuntimeError(
                    f"Nexudus PDF cache: all {stats['pdfs_failed']} fetch(es) failed, "
                    f"0 cached (of {stats['pdfs_total']} candidates)"
                )

    except Exception as exc:
        logger.error("Nexudus invoice PDF cache failed: %s", exc, exc_info=True)
        raise
