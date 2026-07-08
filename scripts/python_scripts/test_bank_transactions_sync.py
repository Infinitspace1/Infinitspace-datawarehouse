"""
scripts/python_scripts/test_bank_transactions_sync.py

Local harness for the Xero bank transactions sync.

Default (dry run, read-only against the API):
  - lists tenants on the stored connection
  - probes GET /BankTransactions page 1 per tenant — a 403 means the token
    is still missing the accounting.banktransactions[.read] scope
  - prints a sample payload + its transformed silver header/lines

Options:
  --count        paginate every tenant fully to size the backfill (API reads only)
  --tenant ID    restrict to one tenant
  --write        run the real XeroBankTransactionSyncService end-to-end
                 (bronze + silver writes; first run = full backfill)
  --force-full   ignore watermarks when combined with --write
  --max-pages N  safety cap for --count pagination (default 300/tenant)

Usage:
  .\\venv\\Scripts\\python.exe scripts\\python_scripts\\test_bank_transactions_sync.py
  .\\venv\\Scripts\\python.exe scripts\\python_scripts\\test_bank_transactions_sync.py --count
  .\\venv\\Scripts\\python.exe scripts\\python_scripts\\test_bank_transactions_sync.py --write
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import requests

from shared.xero.bank_transaction_sync import (
    XeroBankTransactionSyncService,
    line_item_rows,
    silver_header_values,
)
from shared.xero.client import XeroApiClient
from shared.xero.store import XeroStore


def dry_run(args) -> None:
    store = XeroStore()
    connection = store.get_connection(owner_type="workspace", owner_id="default")
    if connection is None:
        print("No Xero connection found")
        return
    print(f"connection_id={connection.id} is_connected={connection.is_connected}")
    print(f"granted scope: {connection.scope}")
    if connection.scope and "accounting.banktransactions" not in connection.scope:
        print(">>> The stored token does NOT include accounting.banktransactions — expect 403s below.")

    tenants = store.list_tenants(connection_id=connection.id)
    if args.tenant:
        tenants = [t for t in tenants if str(t.get("xero_tenant_id")) == args.tenant]
    print(f"tenants: {len(tenants)}\n")

    client = XeroApiClient(connection_id=connection.id, store=store)
    sample_printed = False
    grand_total = 0

    for tenant in tenants:
        tid = str(tenant["xero_tenant_id"])
        name = tenant.get("tenant_name") or tid
        try:
            payload = client.get_bank_transactions(page=1, tenant_id=tid)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            print(f"[{name}] HTTP {status} — missing scope or access")
            continue

        transactions = payload.get("BankTransactions", [])
        count = len(transactions)

        if args.count:
            page = 2
            while len(payload.get("BankTransactions", [])) > 0 and page <= args.max_pages:
                payload = client.get_bank_transactions(page=page, tenant_id=tid)
                count += len(payload.get("BankTransactions", []))
                page += 1
            capped = " (page cap reached!)" if page > args.max_pages else ""
            print(f"[{name}] {count} bank transactions{capped}")
        else:
            more = " (100 = full first page, use --count for the real total)" if count == 100 else ""
            print(f"[{name}] page 1: {count} bank transactions{more}")

        grand_total += count

        if transactions and not sample_printed:
            sample = transactions[0]
            print("\n--- sample payload keys ---")
            print(sorted(sample.keys()))
            print("\n--- transformed silver header ---")
            print(silver_header_values(sample))
            print("\n--- transformed line rows ---")
            for row in line_item_rows(tid, str(sample.get("BankTransactionID")), sample.get("LineItems") or []):
                print(row)
            print()
            sample_printed = True

    label = "total" if args.count else "total on first pages"
    print(f"\n{label}: {grand_total} bank transactions")


def write_run(args) -> None:
    service = XeroBankTransactionSyncService()
    stats = service.sync_bank_transactions(
        owner_type="workspace",
        owner_id="default",
        tenant_id=args.tenant,
        force_full=args.force_full,
    )
    print(json.dumps(stats, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Xero bank transactions sync harness")
    parser.add_argument("--write", action="store_true", help="run the real sync (writes to bronze/silver)")
    parser.add_argument("--count", action="store_true", help="paginate fully to size the backfill (dry run only)")
    parser.add_argument("--tenant", default=None, help="restrict to one xero_tenant_id")
    parser.add_argument("--force-full", action="store_true", help="ignore watermarks (with --write)")
    parser.add_argument("--max-pages", type=int, default=300, help="safety cap per tenant for --count")
    args = parser.parse_args()

    if args.write:
        write_run(args)
    else:
        dry_run(args)


if __name__ == "__main__":
    main()
