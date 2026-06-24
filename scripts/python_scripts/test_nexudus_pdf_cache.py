"""
End-to-end test for the Nexudus PDF blob pipeline:

  1. Pick one open invoice from silver.nexudus_coworker_invoices missing a PDF
  2. Fetch its PDF via the COWORKER_INVOICE_PRINT run-command flow
  3. Upload it to Azure Blob Storage (nexudus-invoice-pdfs container)
  4. Save pdf_blob_path / pdf_cached_at on silver.nexudus_coworker_invoices
  5. Read the bytes back from blob and verify size matches

Run:
  python scripts/python_scripts/test_nexudus_pdf_cache.py
  python scripts/python_scripts/test_nexudus_pdf_cache.py --invoice-id 1429261774
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


def _pick_invoice(sql) -> tuple[int, int, str]:
    rows = sql.execute_query(
        """
        SELECT TOP 1 source_id, location_source_id, invoice_number
        FROM silver.nexudus_coworker_invoices
        WHERE is_deleted = 0
          AND pdf_blob_path IS NULL
          AND location_source_id IS NOT NULL
          AND due_amount > 0
          AND void  = 0
          AND draft = 0
          AND paid  = 0
        ORDER BY due_date DESC
        """
    )
    if not rows:
        raise RuntimeError(
            "No uncached open invoices found — all may already have a pdf_blob_path"
        )
    r = rows[0]
    return int(r["source_id"]), int(r["location_source_id"]), str(r["invoice_number"])


async def _run(args) -> None:
    sql = get_sql_client()
    blob = BlobWriter()

    if args.invoice_id:
        invoice_id = int(args.invoice_id)
        rows = sql.execute_query(
            """
            SELECT location_source_id, invoice_number
            FROM silver.nexudus_coworker_invoices
            WHERE source_id = ?
            """,
            (invoice_id,),
        )
        if not rows:
            raise RuntimeError(f"Invoice {invoice_id} not found in silver")
        location_id = int(rows[0]["location_source_id"])
        invoice_number = str(rows[0]["invoice_number"])
    else:
        invoice_id, location_id, invoice_number = _pick_invoice(sql)
        print(f"Auto-selected: {invoice_number}  (id={invoice_id}, loc={location_id})")

    bearer_token = get_bearer_token()

    # 1. Fetch PDF via the run-command flow
    print("Fetching PDF from Nexudus (COWORKER_INVOICE_PRINT)...")
    async with NexudusClient(bearer_token) as client:
        pdf_bytes = await client.get_invoice_pdf(invoice_id)
    if pdf_bytes is None:
        raise RuntimeError("get_invoice_pdf returned None — no usable PDF")
    print(f"  size        : {len(pdf_bytes):,} bytes")
    print(f"  starts with : {pdf_bytes[:8]!r}")

    # 2. Upload to blob
    print("Uploading to blob storage...")
    blob_path = blob.write_nexudus_pdf(
        location_source_id=location_id,
        invoice_source_id=invoice_id,
        pdf_bytes=pdf_bytes,
    )
    print(f"  blob_path   : {blob_path}")

    # 3. Save to SQL
    print("Saving pdf_blob_path to silver...")
    sql.execute_non_query(
        """
        UPDATE silver.nexudus_coworker_invoices
        SET pdf_blob_path = ?,
            pdf_cached_at = GETUTCDATE()
        WHERE source_id = ?
        """,
        (blob_path, invoice_id),
    )

    # 4. Read back from blob and verify
    print("Reading back from blob...")
    roundtrip = blob._nexudus_pdf_container.get_blob_client(blob_path).download_blob().readall()
    assert len(roundtrip) == len(pdf_bytes), (
        f"Size mismatch: uploaded {len(pdf_bytes)}, got back {len(roundtrip)}"
    )
    print(f"  round-trip OK: {len(roundtrip):,} bytes match")

    # 5. Confirm SQL has the path
    rows = sql.execute_query(
        "SELECT pdf_blob_path, pdf_cached_at FROM silver.nexudus_coworker_invoices WHERE source_id = ?",
        (invoice_id,),
    )
    print(f"  silver.pdf_blob_path = {rows[0]['pdf_blob_path']}")
    print()
    print("Nexudus PDF blob pipeline is working correctly.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invoice-id", default=None)
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
