"""
Backfill PDFs for overdue Xero invoices that don't have a blob_path yet.

Fetches from the Xero API one at a time, uploads to xero-invoice-pdfs blob
container, and saves the path to both bronze.xero_invoice_pdfs and
silver.xero_invoices.

Usage:
  # Dry-run: see how many invoices need PDFs
  python scripts/python_scripts/backfill_xero_pdfs.py --dry-run

  # Backfill all overdue invoices missing a PDF
  python scripts/python_scripts/backfill_xero_pdfs.py

  # Limit for testing
  python scripts/python_scripts/backfill_xero_pdfs.py --limit 10
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.xero.client import XeroApiClient
from shared.xero.store import XeroStore


def _load_missing(sql, limit: int | None) -> list[dict]:
    top = f"TOP {limit}" if limit else ""
    return sql.execute_query(
        f"""
        SELECT {top}
            xi.source_id,
            xi.xero_tenant_id,
            xi.xero_connection_id,
            xi.invoice_number,
            xi.due_date,
            xi.amount_due
        FROM silver.xero_invoices xi
        WHERE xi.invoice_status = 'AUTHORISED'
          AND xi.amount_due > 0
          AND xi.due_date < CAST(GETUTCDATE() AS DATE)
          AND xi.pdf_blob_path IS NULL
        ORDER BY xi.due_date ASC
        """
    )


def _save(sql, sync_run_id: str, row: dict, blob_path: str, content_type: str, file_name: str | None) -> None:
    sql.execute_non_query(
        """
        MERGE bronze.xero_invoice_pdfs AS target
        USING (SELECT ? AS xero_tenant_id, ? AS invoice_source_id) AS source
            ON target.xero_tenant_id = source.xero_tenant_id
           AND target.invoice_source_id = source.invoice_source_id
        WHEN MATCHED THEN UPDATE SET
            sync_run_id = ?, xero_connection_id = ?,
            content_type = ?, file_name = ?, blob_path = ?,
            synced_at = GETUTCDATE()
        WHEN NOT MATCHED THEN INSERT (
            sync_run_id, xero_connection_id, xero_tenant_id,
            invoice_source_id, content_type, file_name, blob_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (
            row["xero_tenant_id"], row["source_id"],
            sync_run_id, row["xero_connection_id"], content_type, file_name, blob_path,
            sync_run_id, row["xero_connection_id"], row["xero_tenant_id"],
            row["source_id"], content_type, file_name, blob_path,
        ),
    )
    sql.execute_non_query(
        """
        UPDATE silver.xero_invoices
        SET pdf_cached_at = GETUTCDATE(),
            pdf_content_type = ?,
            pdf_file_name = ?,
            pdf_blob_path = ?,
            last_synced_at = GETUTCDATE()
        WHERE xero_tenant_id = ? AND source_id = ?
        """,
        (content_type, file_name, blob_path, row["xero_tenant_id"], row["source_id"]),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    sql = get_sql_client()
    store = XeroStore(sql_client=sql)

    invoices = _load_missing(sql, args.limit)
    print(f"Invoices needing PDFs: {len(invoices)}")

    if args.dry_run or not invoices:
        for r in invoices[:10]:
            print(f"  {r['invoice_number']}  due={r['due_date']}  amount_due={r['amount_due']}")
        if len(invoices) > 10:
            print(f"  ... and {len(invoices) - 10} more")
        return

    blob = BlobWriter()
    sync_run_id = str(uuid.uuid4())

    # Group by connection_id to reuse client per connection
    connections: dict[int, XeroApiClient] = {}
    ok = errors = 0

    for i, row in enumerate(invoices, 1):
        conn_id = int(row["xero_connection_id"])
        if conn_id not in connections:
            connections[conn_id] = XeroApiClient(connection_id=conn_id, store=store)
        client = connections[conn_id]

        try:
            pdf_bytes, content_type, file_name = client.get_invoice_pdf(
                invoice_id=str(row["source_id"]),
                tenant_id=str(row["xero_tenant_id"]),
            )
            blob_path = blob.write_pdf(
                tenant_id=str(row["xero_tenant_id"]),
                invoice_source_id=str(row["source_id"]),
                pdf_bytes=pdf_bytes,
                content_type=content_type,
            )
            _save(sql, sync_run_id, row, blob_path, content_type, file_name)
            ok += 1
            print(f"  [{i}/{len(invoices)}] {row['invoice_number']} -> {blob_path}")
        except Exception as exc:
            errors += 1
            print(f"  [{i}/{len(invoices)}] {row['invoice_number']} FAILED: {exc}")

        # Xero rate limit: ~60 req/min, so pace at ~50/min to be safe
        time.sleep(1.2)

    print(f"\nDone: {ok} uploaded, {errors} failed")


if __name__ == "__main__":
    main()
