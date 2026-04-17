"""
shared/nexudus/invoice_pdf_cache.py

Downloads and caches Nexudus coworker invoice PDFs to Azure Blob Storage.
Only processes invoices where silver.nexudus_coworker_invoices.pdf_blob_path IS NULL.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.client import NexudusClient

logger = logging.getLogger(__name__)

PDF_FETCH_THROTTLE_SECONDS = 0.5


async def cache_missing_nexudus_pdfs(
    client: NexudusClient,
    limit: int | None = None,
    lookback_days: int = 2,
) -> dict[str, Any]:
    """
    Fetch and cache PDFs for recent Nexudus invoices missing a pdf_blob_path.

    Only processes invoices from the last ``lookback_days`` days to avoid
    hitting the entire backlog every run.  Invoices that return a server
    error from Nexudus are marked with a sentinel path so they are not
    retried on the next run.

    Returns stats dict: {pdfs_cached, pdfs_failed, pdfs_skipped, pdfs_total}.
    """
    sql = get_sql_client()
    blob_writer = BlobWriter()

    top = f"TOP {limit}" if limit else ""
    rows = sql.execute_query(
        f"""
        SELECT {top}
            source_id,
            location_source_id,
            invoice_number
        FROM silver.nexudus_coworker_invoices
        WHERE pdf_blob_path IS NULL
          AND updated_on >= DATEADD(DAY, -{lookback_days}, GETUTCDATE())
        ORDER BY updated_on DESC
        """
    )

    if not rows:
        logger.info("Nexudus PDF cache: no invoices missing PDFs")
        return {"pdfs_cached": 0, "pdfs_failed": 0, "pdfs_skipped": 0, "pdfs_total": 0}

    logger.info("Nexudus PDF cache: %s invoices to process", len(rows))

    cached = 0
    failed = 0
    skipped = 0

    for row in rows:
        invoice_id = int(row["source_id"])
        location_id = row.get("location_source_id")
        invoice_number = row.get("invoice_number", "unknown")

        if not location_id:
            logger.warning(
                "Skipping invoice %s (no location_source_id)", invoice_number
            )
            skipped += 1
            continue

        try:
            pdf_bytes = await client.get_invoice_pdf(invoice_id)

            if pdf_bytes is None:
                logger.info("No PDF available for invoice %s", invoice_number)
                skipped += 1
                continue

            blob_path = blob_writer.write_nexudus_pdf(
                location_source_id=int(location_id),
                invoice_source_id=invoice_id,
                pdf_bytes=pdf_bytes,
            )

            sql.execute_non_query(
                """
                UPDATE silver.nexudus_coworker_invoices
                SET pdf_blob_path = ?,
                    pdf_cached_at = GETUTCDATE()
                WHERE source_id = ?
                """,
                (blob_path, invoice_id),
            )

            cached += 1
            logger.info(
                "PDF cached for invoice %s [blob=%s]", invoice_number, blob_path
            )
        except Exception as exc:
            failed += 1
            logger.exception("Failed to cache PDF for invoice %s", invoice_number)

            # Mark server-side errors with a sentinel so we don't retry forever
            is_server_error = (
                hasattr(exc, "status") and exc.status in (500, 502, 503)
            )
            if is_server_error:
                sql.execute_non_query(
                    """
                    UPDATE silver.nexudus_coworker_invoices
                    SET pdf_blob_path = '__unavailable__',
                        pdf_cached_at = GETUTCDATE()
                    WHERE source_id = ?
                    """,
                    (invoice_id,),
                )
                logger.info(
                    "Marked invoice %s as PDF unavailable (server error %s)",
                    invoice_number,
                    getattr(exc, "status", "unknown"),
                )

        await asyncio.sleep(PDF_FETCH_THROTTLE_SECONDS)

    stats = {
        "pdfs_cached": cached,
        "pdfs_failed": failed,
        "pdfs_skipped": skipped,
        "pdfs_total": len(rows),
    }
    logger.info("Nexudus PDF cache complete: %s", stats)
    return stats
