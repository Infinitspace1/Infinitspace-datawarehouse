"""
Sync Xero accounts and invoices from all stored tenants into bronze/silver.

Usage:
  python scripts/python_scripts/xero_sync_invoices.py
  python scripts/python_scripts/xero_sync_invoices.py --tenant-id <tenant_uuid> --include-pdfs
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.xero.invoice_sync import XeroInvoiceSyncService


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--owner-type", default="workspace")
    parser.add_argument("--owner-id", default="default")
    parser.add_argument("--connection-id", type=int)
    parser.add_argument("--tenant-id")
    parser.add_argument("--include-pdfs", action="store_true")
    parser.add_argument("--force-full", action="store_true")
    args = parser.parse_args()

    service = XeroInvoiceSyncService()
    result = service.sync_invoices(
        owner_type=args.owner_type,
        owner_id=args.owner_id,
        connection_id=args.connection_id,
        tenant_id=args.tenant_id,
        include_pdfs=args.include_pdfs,
        force_full=args.force_full,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
