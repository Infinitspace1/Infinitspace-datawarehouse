"""
End-to-end test for the Xero PDF blob pipeline:

  1. Pick one overdue invoice from silver.xero_invoices
  2. Fetch its PDF from the Xero API
  3. Upload it to Azure Blob Storage (xero-invoice-pdfs container)
  4. Save the blob_path in bronze.xero_invoice_pdfs + silver.xero_invoices
  5. Read the bytes back from blob and verify size matches

Run:
  python scripts/python_scripts/test_xero_pdf_cache.py
  python scripts/python_scripts/test_xero_pdf_cache.py --invoice-id <uuid> --tenant-id <uuid>
"""
from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.xero.client import XeroApiClient
from shared.xero.store import XeroStore


def _pick_invoice(sql) -> tuple[str, str, str, int]:
    rows = sql.execute_query(
        """
        SELECT TOP 1
            xi.source_id,
            xi.xero_tenant_id,
            xi.invoice_number,
            xi.xero_connection_id
        FROM silver.xero_invoices xi
        WHERE xi.invoice_status = 'AUTHORISED'
          AND xi.amount_due > 0
          AND xi.due_date < CAST(GETUTCDATE() AS DATE)
          AND xi.pdf_blob_path IS NULL
        ORDER BY xi.due_date ASC
        """
    )
    if not rows:
        raise RuntimeError("No uncached overdue invoices found — all may already have blob_path set")
    r = rows[0]
    return str(r["source_id"]), str(r["xero_tenant_id"]), str(r["invoice_number"]), int(r["xero_connection_id"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invoice-id", default=None)
    parser.add_argument("--tenant-id", default=None)
    args = parser.parse_args()

    sql = get_sql_client()
    store = XeroStore(sql_client=sql)
    blob = BlobWriter()

    if args.invoice_id and args.tenant_id:
        invoice_id = args.invoice_id
        tenant_id = args.tenant_id
        rows = sql.execute_query(
            "SELECT xero_connection_id FROM silver.xero_invoices WHERE source_id = ? AND xero_tenant_id = ?",
            (invoice_id, tenant_id),
        )
        connection_id = int(rows[0]["xero_connection_id"]) if rows else None
        invoice_number = invoice_id
    else:
        invoice_id, tenant_id, invoice_number, connection_id = _pick_invoice(sql)
        print(f"Auto-selected: {invoice_number}  (id={invoice_id})")

    # 1. Fetch PDF from Xero
    print(f"Fetching PDF from Xero...")
    client = XeroApiClient(connection_id=connection_id, store=store)
    pdf_bytes, content_type, file_name = client.get_invoice_pdf(
        invoice_id=invoice_id,
        tenant_id=tenant_id,
    )
    print(f"  content_type : {content_type}")
    print(f"  file_name    : {file_name}")
    print(f"  size         : {len(pdf_bytes):,} bytes")

    # 2. Upload to blob
    print(f"Uploading to blob storage...")
    blob_path = blob.write_pdf(
        tenant_id=tenant_id,
        invoice_source_id=invoice_id,
        pdf_bytes=pdf_bytes,
        content_type=content_type,
    )
    print(f"  blob_path    : {blob_path}")

    # 3. Save to SQL
    print(f"Saving blob_path to SQL...")
    sync_run_id = str(uuid.uuid4())
    sql.execute_non_query(
        """
        MERGE bronze.xero_invoice_pdfs AS target
        USING (SELECT ? AS xero_tenant_id, ? AS invoice_source_id) AS source
            ON target.xero_tenant_id = source.xero_tenant_id
           AND target.invoice_source_id = source.invoice_source_id
        WHEN MATCHED THEN UPDATE SET
            sync_run_id = ?,
            xero_connection_id = ?,
            content_type = ?,
            file_name = ?,
            blob_path = ?,
            synced_at = GETUTCDATE()
        WHEN NOT MATCHED THEN INSERT (
            sync_run_id, xero_connection_id, xero_tenant_id,
            invoice_source_id, content_type, file_name, blob_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (
            tenant_id, invoice_id,
            sync_run_id, connection_id, content_type, file_name, blob_path,
            sync_run_id, connection_id, tenant_id, invoice_id, content_type, file_name, blob_path,
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
        (content_type, file_name, blob_path, tenant_id, invoice_id),
    )
    print(f"  bronze + silver updated")

    # 4. Read back from blob and verify
    print(f"Reading back from blob...")
    roundtrip_bytes = blob.read_pdf(blob_path)
    assert len(roundtrip_bytes) == len(pdf_bytes), (
        f"Size mismatch: uploaded {len(pdf_bytes)}, got back {len(roundtrip_bytes)}"
    )
    print(f"  round-trip OK: {len(roundtrip_bytes):,} bytes match")

    # 5. Confirm SQL has the path
    rows = sql.execute_query(
        "SELECT pdf_blob_path, pdf_cached_at FROM silver.xero_invoices WHERE source_id = ? AND xero_tenant_id = ?",
        (invoice_id, tenant_id),
    )
    print(f"  silver.xero_invoices.pdf_blob_path = {rows[0]['pdf_blob_path']}")
    print()
    print("PDF blob pipeline is working correctly.")


if __name__ == "__main__":
    main()
