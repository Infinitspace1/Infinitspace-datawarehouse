"""
Download and cache a Xero invoice PDF into bronze.xero_invoice_pdfs.
"""
from __future__ import annotations

import argparse
import json

from shared.xero.invoice_sync import XeroInvoiceSyncService


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invoice-id", required=True)
    parser.add_argument("--tenant-id")
    parser.add_argument("--owner-type", default="workspace")
    parser.add_argument("--owner-id", default="default")
    parser.add_argument("--connection-id", type=int)
    args = parser.parse_args()

    service = XeroInvoiceSyncService()
    result = service.cache_invoice_pdf(
        invoice_id=args.invoice_id,
        tenant_id=args.tenant_id,
        owner_type=args.owner_type,
        owner_id=args.owner_id,
        connection_id=args.connection_id,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
