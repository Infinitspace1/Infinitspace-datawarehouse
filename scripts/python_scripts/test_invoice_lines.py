"""
scripts/python_scripts/test_invoice_lines.py

Fetches Nexudus coworker invoice lines for all invoices in bronze,
writes them to bronze + silver.

Resumable: skips invoices that already have lines in bronze.
Writes to bronze in batches so progress is saved if you stop.

Usage:
    python scripts/python_scripts/test_invoice_lines.py
    python scripts/python_scripts/test_invoice_lines.py --limit 10
    python scripts/python_scripts/test_invoice_lines.py --dry-run
    python scripts/python_scripts/test_invoice_lines.py --batch-size 50
"""
import argparse
import asyncio
import logging
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_invoice_lines")

# Delay between API calls to avoid rate limiting
THROTTLE_SECONDS = 0.6


async def main(dry_run: bool = False, limit: int | None = None, batch_size: int = 100):
    from shared.nexudus.auth import get_bearer_token
    from shared.nexudus.client import NexudusClient
    from shared.azure_clients.sql_client import get_sql_client
    from shared.azure_clients.bronze_writer import BronzeWriter
    from shared.azure_clients.blob_writer import BlobWriter
    from shared.azure_clients.silver_writer_coworker_invoice_lines import SilverCoworkerInvoiceLinesWriter

    sql = get_sql_client()
    bearer_token = get_bearer_token()

    # Get invoices that DON'T have lines in bronze yet (resumable)
    already_done = sql.execute_query(
        "SELECT DISTINCT invoice_source_id FROM bronze.nexudus_coworker_invoice_lines"
    )
    done_set = {int(r["invoice_source_id"]) for r in already_done if r.get("invoice_source_id")}

    all_invoices = sql.execute_query(
        "SELECT DISTINCT source_id FROM bronze.nexudus_coworker_invoices ORDER BY source_id"
    )
    all_ids = [int(r["source_id"]) for r in all_invoices if r.get("source_id")]
    remaining_ids = [sid for sid in all_ids if sid not in done_set]

    if limit:
        remaining_ids = remaining_ids[:limit]

    logger.info(
        "Invoices: %s total, %s already have lines, %s remaining to fetch",
        len(all_ids), len(done_set), len(remaining_ids),
    )

    if not remaining_ids:
        logger.info("All invoices already have lines in bronze — nothing to do")
        # Still run silver in case bronze has data not yet in silver
        if not dry_run:
            _run_silver(sql)
        return

    if dry_run:
        logger.info("DRY RUN — would fetch lines for %s invoices", len(remaining_ids))
        return

    # Fetch and write in batches
    run_id = uuid.uuid4()
    writer = BronzeWriter(run_id)
    total_lines = 0
    total_errors = 0

    async with NexudusClient(bearer_token, max_concurrent=1) as client:
        batch_lines: list[dict] = []

        for i, invoice_id in enumerate(remaining_ids, 1):
            try:
                lines = await client.get_coworker_invoice_lines(invoice_id)
                batch_lines.extend(lines)
            except Exception as exc:
                logger.warning("Invoice %s failed: %s", invoice_id, exc)
                total_errors += 1

            # Write batch to bronze periodically
            if len(batch_lines) >= batch_size or i == len(remaining_ids):
                if batch_lines:
                    writer.write_coworker_invoice_lines(batch_lines)
                    total_lines += len(batch_lines)
                    batch_lines = []

                logger.info(
                    "Progress: %s/%s invoices (%s lines written, %s errors)",
                    i, len(remaining_ids), total_lines, total_errors,
                )

            await asyncio.sleep(THROTTLE_SECONDS)

    logger.info(
        "Fetch complete: %s invoices, %s lines written to bronze, %s errors",
        len(remaining_ids), total_lines, total_errors,
    )

    _run_silver(sql)


def _run_silver(sql):
    """Run the silver writer and show results."""
    from shared.azure_clients.silver_writer_coworker_invoice_lines import SilverCoworkerInvoiceLinesWriter

    run_id = uuid.uuid4()
    silver_writer = SilverCoworkerInvoiceLinesWriter(run_id)
    result = silver_writer.run()
    logger.info("Silver: %s", result)

    bronze_total = sql.execute_scalar("SELECT COUNT(*) FROM bronze.nexudus_coworker_invoice_lines")
    silver_total = sql.execute_scalar("SELECT COUNT(*) FROM silver.nexudus_coworker_invoice_lines")
    logger.info("Final counts — bronze: %s, silver: %s", bronze_total, silver_total)

    sample_accounts = sql.execute_query("""
        SELECT TOP 10
            financial_account_code,
            financial_account_name,
            COUNT(*) AS line_count
        FROM silver.nexudus_coworker_invoice_lines
        GROUP BY financial_account_code, financial_account_name
        ORDER BY line_count DESC
    """)
    if sample_accounts:
        logger.info("Top account codes:")
        for row in sample_accounts:
            logger.info(
                "  %s — %s (%s lines)",
                row["financial_account_code"],
                row["financial_account_name"],
                row["line_count"],
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run, limit=args.limit, batch_size=args.batch_size))
