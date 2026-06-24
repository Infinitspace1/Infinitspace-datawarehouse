"""
Backfill PDFs for the Nexudus coworker invoices shown on the finance dashboard.

Uses the COWORKER_INVOICE_PRINT run-command flow (NexudusClient.get_invoice_pdf),
uploads each PDF to the nexudus-invoice-pdfs blob container, and stores the path
in silver.nexudus_coworker_invoices.pdf_blob_path.

DEFAULT SCOPE = exactly the invoices on the finance dashboard: every row in
gold.finance_dashboard_invoice_worklist whose silver invoice has no PDF yet.
This is NOT a backfill of every invoice in the database. Pass --all to backfill
every invoice missing a PDF.

The pdf_blob_path IS NULL filter is the natural watermark — already-cached
invoices are never re-fetched. Safe to resume after a failure.

IMPORTANT: the previous (broken) PDF endpoint marked invoices it could not
download with the sentinel pdf_blob_path = '__unavailable__'. Those are NOT
genuinely unavailable — they failed because of the wrong endpoint. Pass
--reset-unavailable to clear the sentinel so they are retried with the working
run-command flow (recommended for the first run after the fix).

Usage:
  # Dry-run: how many dashboard invoices need PDFs (incl. ones to un-stick)
  python scripts/python_scripts/backfill_nexudus_pdfs.py --reset-unavailable --dry-run

  # Test a small batch first
  python scripts/python_scripts/backfill_nexudus_pdfs.py --reset-unavailable --limit 20

  # Full dashboard backfill (the normal one-off after deploy)
  python scripts/python_scripts/backfill_nexudus_pdfs.py --reset-unavailable

  # Every invoice missing a PDF, not just the dashboard subset
  python scripts/python_scripts/backfill_nexudus_pdfs.py --reset-unavailable --all
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient

THROTTLE_SECONDS = 0.5

# When --reset-unavailable is in play, treat the old endpoint's sentinel as
# "missing" so the dry-run count and the live run agree.
def _missing_predicate(include_unavailable: bool) -> str:
    if include_unavailable:
        return "(nci.pdf_blob_path IS NULL OR nci.pdf_blob_path = '__unavailable__')"
    return "nci.pdf_blob_path IS NULL"


def _count_unavailable(sql) -> int:
    rows = sql.execute_query(
        """
        SELECT COUNT(*) AS n
        FROM silver.nexudus_coworker_invoices
        WHERE pdf_blob_path = '__unavailable__'
        """
    )
    return int(rows[0]["n"]) if rows else 0


def _reset_unavailable(sql) -> int:
    n = _count_unavailable(sql)
    if n:
        sql.execute_non_query(
            """
            UPDATE silver.nexudus_coworker_invoices
            SET pdf_blob_path = NULL,
                pdf_cached_at = NULL
            WHERE pdf_blob_path = '__unavailable__'
            """
        )
    return n


def _load_dashboard_missing(sql, limit: int | None, include_unavailable: bool) -> list[dict]:
    """Invoices that appear on the finance dashboard worklist and have no PDF."""
    top = f"TOP {limit}" if limit else ""
    return sql.execute_query(
        f"""
        SELECT {top}
            nci.source_id,
            nci.location_source_id,
            nci.invoice_number,
            nci.due_amount,
            nci.due_date
        FROM gold.finance_dashboard_invoice_worklist w
        JOIN silver.nexudus_coworker_invoices nci
            ON nci.source_id = w.nexudus_invoice_source_id
        WHERE {_missing_predicate(include_unavailable)}
          AND nci.is_deleted = 0
        ORDER BY w.due_date DESC
        """
    )


def _load_all_missing(sql, limit: int | None, include_unavailable: bool) -> list[dict]:
    top = f"TOP {limit}" if limit else ""
    return sql.execute_query(
        f"""
        SELECT {top}
            nci.source_id,
            nci.location_source_id,
            nci.invoice_number,
            nci.due_amount,
            nci.due_date
        FROM silver.nexudus_coworker_invoices nci
        WHERE {_missing_predicate(include_unavailable)}
          AND nci.is_deleted = 0
        ORDER BY nci.due_date DESC
        """
    )


async def _run(args) -> None:
    sql = get_sql_client()

    # In a live run, clear the sentinel first so those rows become candidates.
    # In a dry-run, don't write — just report what would be reset, and count
    # sentinel rows as missing so the candidate count is accurate.
    if args.reset_unavailable:
        if args.dry_run:
            print(f"[dry-run] would reset {_count_unavailable(sql)} '__unavailable__' sentinel(s) to NULL")
        else:
            print(f"Reset {_reset_unavailable(sql)} '__unavailable__' sentinel(s) back to NULL for retry")

    include_unavailable = args.reset_unavailable
    if args.all:
        invoices = _load_all_missing(sql, args.limit, include_unavailable)
        scope = "all invoices"
    else:
        invoices = _load_dashboard_missing(sql, args.limit, include_unavailable)
        scope = "finance dashboard worklist"
    print(f"Invoices needing PDFs ({scope}): {len(invoices)}")

    if not args.all and not invoices:
        # Most likely the gold worklist hasn't been refreshed recently.
        open_rows = sql.execute_query(
            """
            SELECT COUNT(*) AS n
            FROM silver.nexudus_coworker_invoices
            WHERE pdf_blob_path IS NULL AND is_deleted = 0
              AND due_amount > 0 AND void = 0 AND draft = 0 AND paid = 0
            """
        )
        n_open = int(open_rows[0]["n"]) if open_rows else 0
        if n_open:
            print(
                f"  Note: {n_open} open unpaid invoices have no PDF but are not in the "
                "gold worklist — refresh it (POST /api/finance/refresh-invoice-worklist "
                "or wait for the nightly rebuild), or run with --all."
            )

    if args.dry_run or not invoices:
        for r in invoices[:10]:
            print(
                f"  {r['invoice_number']}  due={r['due_date']}  "
                f"due_amount={r['due_amount']}  loc={r['location_source_id']}"
            )
        if len(invoices) > 10:
            print(f"  ... and {len(invoices) - 10} more")
        return

    blob = BlobWriter()
    bearer_token = get_bearer_token()

    ok = skipped = errors = 0
    async with NexudusClient(bearer_token) as client:
        for i, row in enumerate(invoices, 1):
            invoice_id = int(row["source_id"])
            location_id = row.get("location_source_id")
            invoice_number = row.get("invoice_number", "unknown")

            if not location_id:
                print(f"  [{i}/{len(invoices)}] {invoice_number} SKIP (no location)")
                skipped += 1
                continue

            try:
                pdf_bytes = await client.get_invoice_pdf(invoice_id)
                if pdf_bytes is None:
                    print(f"  [{i}/{len(invoices)}] {invoice_number} SKIP (no PDF)")
                    skipped += 1
                    continue

                blob_path = blob.write_nexudus_pdf(
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
                ok += 1
                print(
                    f"  [{i}/{len(invoices)}] {invoice_number} -> {blob_path} "
                    f"({len(pdf_bytes):,} bytes)"
                )
            except Exception as exc:  # noqa: BLE001 - report and continue
                errors += 1
                print(f"  [{i}/{len(invoices)}] {invoice_number} FAILED: {exc}")

            await asyncio.sleep(THROTTLE_SECONDS)

    print(f"\nDone: {ok} uploaded, {skipped} skipped, {errors} failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--all",
        action="store_true",
        help="Backfill every invoice missing a PDF (not just the dashboard worklist)",
    )
    parser.add_argument(
        "--reset-unavailable",
        action="store_true",
        help="Clear '__unavailable__' sentinels (set by the old endpoint) and retry them",
    )
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
