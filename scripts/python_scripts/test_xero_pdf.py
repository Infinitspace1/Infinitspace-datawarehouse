"""
Quick smoke test: fetch one Xero invoice PDF and save it to disk.

Usage:
  python scripts/python_scripts/test_xero_pdf.py
  python scripts/python_scripts/test_xero_pdf.py --invoice-id <xero_invoice_uuid> --tenant-id <xero_tenant_id>

If no args given, auto-picks the first overdue invoice from your DB.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.azure_clients.sql_client import get_sql_client
from shared.xero.client import XeroApiClient


def _pick_invoice(sql) -> tuple[str, str, str]:
    rows = sql.execute_query(
        """
        SELECT TOP 1
            xi.source_id,
            xi.xero_tenant_id,
            xi.invoice_number
        FROM silver.xero_invoices xi
        WHERE xi.invoice_status = 'AUTHORISED'
          AND xi.amount_due > 0
          AND xi.due_date < CAST(GETUTCDATE() AS DATE)
        ORDER BY xi.due_date ASC
        """
    )
    if not rows:
        raise RuntimeError("No overdue invoices found in silver.xero_invoices")
    r = rows[0]
    return str(r["source_id"]), str(r["xero_tenant_id"]), str(r["invoice_number"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invoice-id", default=None, help="Xero InvoiceID (UUID)")
    parser.add_argument("--tenant-id", default=None, help="Xero tenant ID")
    parser.add_argument("--out", default=None, help="Output path (default: /tmp/<invoice_number>.pdf)")
    args = parser.parse_args()

    sql = get_sql_client()

    if args.invoice_id and args.tenant_id:
        invoice_id = args.invoice_id
        tenant_id = args.tenant_id
        invoice_number = invoice_id
    else:
        invoice_id, tenant_id, invoice_number = _pick_invoice(sql)
        print(f"Auto-selected invoice: {invoice_number}  (id={invoice_id})")

    client = XeroApiClient(tenant_id=tenant_id)
    print(f"Fetching PDF for invoice {invoice_id} from tenant {tenant_id}...")

    pdf_bytes, content_type, filename = client.get_invoice_pdf(
        invoice_id=invoice_id,
        tenant_id=tenant_id,
    )

    out_path = Path(args.out) if args.out else Path(f"{invoice_number}.pdf")
    out_path.write_bytes(pdf_bytes)

    print(f"  content_type : {content_type}")
    print(f"  filename     : {filename}")
    print(f"  size         : {len(pdf_bytes):,} bytes")
    print(f"  saved to     : {out_path.resolve()}")
    print()
    print("If that file opens correctly as a PDF, the attachment pipeline is ready.")


if __name__ == "__main__":
    main()
