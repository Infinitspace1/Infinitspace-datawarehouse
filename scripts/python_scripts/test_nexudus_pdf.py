"""
Quick smoke test: fetch one Nexudus coworker-invoice PDF and save it to disk.

Exercises the COWORKER_INVOICE_PRINT run-command flow end to end against the
live Nexudus API (no blob / SQL writes).

Usage:
  python scripts/python_scripts/test_nexudus_pdf.py
  python scripts/python_scripts/test_nexudus_pdf.py --invoice-id 1429261774
  python scripts/python_scripts/test_nexudus_pdf.py --invoice-id 1429261774 --out invoice.pdf

If no --invoice-id is given, auto-picks a recent open invoice from
silver.nexudus_coworker_invoices.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient


def _pick_invoice(sql) -> tuple[int, str]:
    rows = sql.execute_query(
        """
        SELECT TOP 1 source_id, invoice_number
        FROM silver.nexudus_coworker_invoices
        WHERE is_deleted = 0
          AND due_amount > 0
          AND void  = 0
          AND draft = 0
          AND paid  = 0
        ORDER BY due_date DESC
        """
    )
    if not rows:
        raise RuntimeError("No open invoices found in silver.nexudus_coworker_invoices")
    r = rows[0]
    return int(r["source_id"]), str(r["invoice_number"])


async def _run(args) -> None:
    if args.invoice_id:
        invoice_id = int(args.invoice_id)
        invoice_number = str(invoice_id)
    else:
        sql = get_sql_client()
        invoice_id, invoice_number = _pick_invoice(sql)
        print(f"Auto-selected invoice: {invoice_number}  (id={invoice_id})")

    bearer_token = get_bearer_token()

    async with NexudusClient(bearer_token) as client:
        print(f"Running COWORKER_INVOICE_PRINT for invoice {invoice_id}...")
        redirect_url = await client.run_invoice_print_command([invoice_id])
        print(f"  RedirectURL : {redirect_url}")
        if not redirect_url:
            print("No RedirectURL returned — Nexudus produced no document.")
            return

        pdf_bytes = await client.get_invoice_pdf(invoice_id)

    if pdf_bytes is None:
        print("get_invoice_pdf returned None (no usable PDF).")
        return

    out_path = Path(args.out) if args.out else Path(f"{invoice_number}.pdf")
    out_path.write_bytes(pdf_bytes)

    print(f"  size        : {len(pdf_bytes):,} bytes")
    print(f"  starts with : {pdf_bytes[:8]!r}")
    print(f"  saved to    : {out_path.resolve()}")
    print()
    print("If that file opens as a PDF, the Nexudus PDF pipeline is working.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invoice-id", default=None, help="Nexudus coworker invoice id")
    parser.add_argument("--out", default=None, help="Output path (default: <invoice_number>.pdf)")
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
