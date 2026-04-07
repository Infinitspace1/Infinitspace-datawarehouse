"""
List synced Xero invoices from silver.xero_invoices.
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
    parser.add_argument("--top", type=int, default=50)
    args = parser.parse_args()

    service = XeroInvoiceSyncService()
    rows = service.list_invoices(
        owner_type=args.owner_type,
        owner_id=args.owner_id,
        connection_id=args.connection_id,
        tenant_id=args.tenant_id,
        top=args.top,
    )
    print(json.dumps(rows, indent=2, default=str))


if __name__ == "__main__":
    main()
