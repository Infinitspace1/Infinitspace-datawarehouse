"""
Manual backfill / smoke test for Nexudus coworker invoices and coworkers.

Writes page-by-page so a failure mid-run doesn't lose all progress.
Supports incremental sync via --since to only fetch records updated after
a given datetime (or the last successful run watermark from the DB).

Usage examples:

  # Dry-run with limit
  python sync_nexudus_billing.py --dry-run --limit 5

  # Full initial backfill (page-by-page, resumable on retry)
  python sync_nexudus_billing.py

  # Incremental — only records updated since a date
  python sync_nexudus_billing.py --since 2025-01-01

  # Incremental — auto-detect last successful run from DB
  python sync_nexudus_billing.py --since-last-run
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from shared.azure_clients.bronze_writer import BronzeWriter
from shared.azure_clients.silver_writer_coworker_invoices import SilverCoworkerInvoicesWriter
from shared.azure_clients.silver_writer_coworkers import SilverCoworkersWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient


def _get_last_run_watermark() -> datetime | None:
    """Return the finished_at time of the last successful coworker_invoices bronze run."""
    sql = get_sql_client()
    rows = sql.execute_query(
        """
        SELECT TOP 1 finished_at
        FROM meta.sync_runs
        WHERE source_name = 'nexudus'
          AND entity = 'coworker_invoices'
          AND layer = 'bronze'
          AND status = 'success'
        ORDER BY finished_at DESC
        """
    )
    if rows and rows[0].get("finished_at"):
        ts = rows[0]["finished_at"]
        if isinstance(ts, str):
            return datetime.fromisoformat(ts)
        return ts
    return None


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Fetch only, no DB writes")
    parser.add_argument("--skip-silver", action="store_true", help="Write bronze only")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N invoices")
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only fetch records updated after this date",
    )
    parser.add_argument(
        "--since-last-run",
        action="store_true",
        help="Auto-detect watermark from last successful run in meta.sync_runs",
    )
    args = parser.parse_args()

    # Resolve watermark
    since_dt: datetime | None = None
    if args.since_last_run:
        since_dt = _get_last_run_watermark()
        if since_dt:
            print(f"Using watermark from last run: {since_dt.isoformat()}")
        else:
            print("No previous run found — doing full fetch")
    elif args.since:
        since_dt = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
        print(f"Fetching records updated since: {since_dt.isoformat()}")

    extra_params: dict = {}
    if since_dt:
        extra_params["UpdatedSince"] = since_dt.strftime("%Y-%m-%dT%H:%M:%S")

    sync_run_id = uuid.uuid4()
    token = get_bearer_token()
    bronze = BronzeWriter(sync_run_id) if not args.dry_run else None

    total_invoices = 0
    total_coworker_ids: set[int] = set()

    print(f"sync_run_id={sync_run_id}  dry_run={args.dry_run}")

    async with NexudusClient(token) as client:
        # ── Invoices: page-by-page so a crash doesn't lose all progress ──
        async for page in client.paginate("billing/coworkerinvoices", extra_params=extra_params):
            if args.limit and total_invoices >= args.limit:
                break

            if args.limit:
                remaining = args.limit - total_invoices
                page = page[:remaining]

            total_invoices += len(page)
            new_ids = {int(r["CoworkerId"]) for r in page if r.get("CoworkerId")}
            total_coworker_ids.update(new_ids)

            if not args.dry_run:
                written = bronze.write_coworker_invoices(page)
                print(f"  invoices page: {len(page)} fetched, {written} written (total so far: {total_invoices})")
            else:
                print(f"  invoices page: {len(page)} fetched [dry-run] (total so far: {total_invoices})")

        # ── Coworkers: fetch only the IDs seen in the invoices above ──
        coworker_ids = sorted(total_coworker_ids)
        print(f"Fetching {len(coworker_ids)} distinct coworkers...")

        coworkers_written = 0
        coworkers_failed = 0
        COWORKER_BATCH = 50  # write to bronze every N coworkers to limit memory

        batch_records: list[dict] = []
        tasks = [client.get_coworker(cid) for cid in coworker_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for coworker_id, result in zip(coworker_ids, results):
            if isinstance(result, Exception):
                print(f"  WARNING: coworker {coworker_id} failed: {result}")
                coworkers_failed += 1
                continue
            if result:
                batch_records.append(result)

            if len(batch_records) >= COWORKER_BATCH:
                if not args.dry_run:
                    bronze.write_coworkers(batch_records)
                coworkers_written += len(batch_records)
                batch_records = []

        if batch_records:
            if not args.dry_run:
                bronze.write_coworkers(batch_records)
            coworkers_written += len(batch_records)

    print({
        "sync_run_id": str(sync_run_id),
        "invoice_count": total_invoices,
        "coworker_count": coworkers_written,
        "coworker_failures": coworkers_failed,
        "dry_run": args.dry_run,
    })

    if args.dry_run or args.skip_silver:
        return

    print("Writing silver...")
    print(SilverCoworkerInvoicesWriter(sync_run_id).run())
    print(SilverCoworkersWriter(sync_run_id).run())


if __name__ == "__main__":
    asyncio.run(main())
