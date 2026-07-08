"""
Sync monthly Xero Profit & Loss reports into bronze/silver.

Usage:
  python scripts/python_scripts/xero_sync_profit_loss.py
  python scripts/python_scripts/xero_sync_profit_loss.py --tenant-id <tenant_uuid> --from-month 2024-01 --to-month 2026-06 --force-full
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

from shared.xero.profit_loss_sync import XeroProfitLossSyncService


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync monthly Xero Profit & Loss reports")
    parser.add_argument("--owner-type", default="workspace")
    parser.add_argument("--owner-id", default="default")
    parser.add_argument("--connection-id", type=int)
    parser.add_argument("--tenant-id")
    parser.add_argument("--from-month", help="YYYY-MM or YYYY-MM-DD; defaults to 2024-01")
    parser.add_argument("--to-month", help="YYYY-MM or YYYY-MM-DD; defaults to the latest complete month")
    parser.add_argument("--refresh-months", type=int, help="recent complete months to re-pull; default 3")
    parser.add_argument("--force-full", action="store_true", help="sync every month in the requested range")
    parser.add_argument("--include-current-month", action="store_true", help="include the current partial month")
    args = parser.parse_args()

    service = XeroProfitLossSyncService()
    result = service.sync_profit_loss(
        owner_type=args.owner_type,
        owner_id=args.owner_id,
        connection_id=args.connection_id,
        tenant_id=args.tenant_id,
        from_month=args.from_month,
        to_month=args.to_month,
        refresh_months=args.refresh_months,
        force_full=args.force_full,
        include_current_month=args.include_current_month,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
