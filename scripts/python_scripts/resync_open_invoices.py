"""
One-off backfill for stale invoice balances on the finance dashboard.

Nexudus applies a payment/credit to an invoice WITHOUT bumping its UpdatedOn,
so the nightly incremental invoice fetch (keyed on from_CoworkerInvoice_UpdatedOn
with a 2-day window) never re-pulls a settled-but-aged invoice. Its
DueAmount/PaidAmount freeze at the last in-window value and the dashboard shows
the full gross amount for an invoice that is really almost paid.

The permanent fix is functions/bronze_nexudus.py::_resync_open_invoices, which
runs every nightly bronze sync. This script runs the SAME re-fetch on demand and
then rebuilds silver + the gold worklist, so the dashboard is corrected
immediately without waiting for the nightly run.

Steps (--apply):
  1. Re-fetch every open unpaid invoice OBJECT by ID -> bronze
     (bypasses the UpdatedOn window; SHA-256 hash check only writes changes).
  2. Silver: re-transform coworker_invoices from the refreshed bronze.
  3. Gold:   EXEC gold.sp_refresh_invoice_worklist.

Read-only unless --apply is passed.

Usage (from repo root):
  python -m scripts.python_scripts.resync_open_invoices             # dry-run
  python -m scripts.python_scripts.resync_open_invoices --apply     # fetch + bronze + silver + gold
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from functions.bronze_nexudus import _load_open_invoice_ids, _resync_open_invoices
from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.bronze_writer import BronzeWriter
from shared.azure_clients.silver_writer_coworker_invoices import SilverCoworkerInvoicesWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient

GOLD_SP = "EXEC gold.sp_refresh_invoice_worklist"


def _worklist_snapshot(sql, source_ids: list[int]) -> dict[int, dict]:
    """Current gold worklist due_amount for the given invoice source_ids."""
    if not source_ids:
        return {}
    placeholders = ",".join("?" * len(source_ids))
    rows = sql.execute_query(
        f"""
        SELECT nexudus_invoice_source_id AS sid, invoice_number,
               due_amount, total_amount, currency_code
        FROM gold.finance_dashboard_invoice_worklist
        WHERE nexudus_invoice_source_id IN ({placeholders})
        """,
        tuple(source_ids),
    )
    return {int(r["sid"]): dict(r) for r in rows}


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Fetch open invoices, write bronze, rebuild silver + gold. "
             "Without this flag the script only reports what it would refresh.",
    )
    parser.add_argument(
        "--lookback-months", type=int, default=12,
        help="Only refresh invoices due within this many months (default 12).",
    )
    args = parser.parse_args()

    # Make --lookback-months govern the by-ID re-fetch too (the resync step
    # reads this env var), so the reported set and the applied set always match.
    os.environ["NEXUDUS_INVOICE_RESYNC_LOOKBACK_MONTHS"] = str(args.lookback_months)

    sql = get_sql_client()
    invoice_ids = _load_open_invoice_ids(args.lookback_months)
    print(f"Open invoices to refresh: {len(invoice_ids)}")
    if not invoice_ids:
        return

    before = _worklist_snapshot(sql, invoice_ids)

    if not args.apply:
        print("\nDRY-RUN — no writes. Current gold worklist due_amount for these invoices:")
        for sid in invoice_ids:
            row = before.get(sid)
            if row:
                print(f"  {row['invoice_number']:<24} due={row['due_amount']} {row['currency_code']} "
                      f"(total {row['total_amount']})")
            else:
                print(f"  source_id={sid}  (not currently on the worklist)")
        print("\nRe-run with --apply to fetch fresh balances and rebuild silver + gold.")
        return

    # 1. Bronze re-fetch by ID.
    token = get_bearer_token()
    run_id = uuid.uuid4()
    blob_writer = BlobWriter()
    writer = BronzeWriter(run_id)
    print("\n[1/3] Re-fetching open invoices by ID into bronze ...")
    async with NexudusClient(token) as client:
        await _resync_open_invoices(client, blob_writer, writer, run_id)

    # 2. Silver re-transform (coworker_invoices only; picks up the fresh bronze).
    print("[2/3] Rebuilding silver.nexudus_coworker_invoices ...")
    result = SilverCoworkerInvoicesWriter(uuid.uuid4()).run()
    print(f"      silver upserted={result.get('coworker_invoices')} "
          f"(read {result.get('rows_read')}, errors {result.get('errors')})")

    # 3. Gold worklist rebuild.
    print("[3/3] Rebuilding gold.finance_dashboard_invoice_worklist ...")
    sql.execute_non_query(GOLD_SP)

    # Report what actually changed.
    after = _worklist_snapshot(sql, invoice_ids)
    changed = []
    for sid in invoice_ids:
        b = before.get(sid)
        a = after.get(sid)
        b_due = b["due_amount"] if b else None
        a_due = a["due_amount"] if a else None
        if b_due != a_due:
            num = (b or a or {}).get("invoice_number", str(sid))
            cur = (b or a or {}).get("currency_code", "")
            changed.append((num, b_due, a_due, cur, a is None))

    print(f"\nDone. {len(changed)} invoice(s) changed due_amount:")
    for num, b_due, a_due, cur, dropped in changed:
        tail = "  -> now off the worklist (paid/settled)" if dropped else ""
        print(f"  {num:<24} {b_due} -> {a_due} {cur}{tail}")
    if not changed:
        print("  (no worklist due_amount changed — balances were already current)")


if __name__ == "__main__":
    asyncio.run(main())
