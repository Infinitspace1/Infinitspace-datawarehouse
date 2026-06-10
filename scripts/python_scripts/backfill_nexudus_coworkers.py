"""
scripts/python_scripts/backfill_nexudus_coworkers.py

One-off backfill after switching the coworkers sync from per-ID fetches
(invoice CoworkerIds only) to the full GET /spaces/coworkers list endpoint.

The nightly sync is incremental (UpdatedSince watermark), so coworkers that
already existed before the switch — and never get updated again — would
never be fetched. This script runs the REAL production _sync_coworkers with
force_full=True (full list fetch, RunTracker-tracked so the watermark
advances), then the silver coworkers writer.

  default (dry-run): compare the live Nexudus coworker count against
      bronze/silver SQL counts, print a transformed sample. No writes.
  --write: full list fetch -> bronze -> silver.

Usage:
    python scripts/python_scripts/backfill_nexudus_coworkers.py
    python scripts/python_scripts/backfill_nexudus_coworkers.py --write
"""
import argparse
import asyncio
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def _section(title: str):
    print(f"\n{'─'*60}\n  {title}\n{'─'*60}")


class _SafeBlobWriter:
    """Local fallback: blob snapshots are best-effort here (the local identity
    often lacks blob-write RBAC) — the nightly run in Azure still writes real
    snapshots."""

    def __init__(self):
        try:
            from shared.azure_clients.blob_writer import BlobWriter
            self._inner = BlobWriter()
        except Exception as exc:
            print(f"  Blob writer unavailable locally — snapshots skipped ({exc!r})")
            self._inner = None

    def write_snapshot(self, entity, records, run_id):
        if self._inner is None:
            return f"(skipped locally: {entity})"
        try:
            return self._inner.write_snapshot(entity, records, run_id)
        except Exception as exc:
            print(f"  Blob snapshot for {entity} skipped locally ({type(exc).__name__})")
            return f"(skipped locally: {entity})"


def _sql_counts() -> dict[str, int]:
    from shared.azure_clients.sql_client import get_sql_client

    sql = get_sql_client()
    rows = sql.execute_query(
        """
        SELECT
            (SELECT COUNT(DISTINCT source_id) FROM bronze.nexudus_coworkers) AS bronze_total,
            (SELECT COUNT(*) FROM silver.nexudus_coworkers) AS silver_total,
            (SELECT COUNT(*) FROM silver.nexudus_coworkers WHERE is_deleted = 0) AS silver_active
        """
    )
    return rows[0]


async def dry_run():
    _section("Dry run: live count vs SQL (no writes)")
    from shared.nexudus.auth import get_bearer_token
    from shared.nexudus.client import NexudusClient
    from shared.nexudus.transformers.coworkers import transform_coworker

    async with NexudusClient(get_bearer_token()) as client:
        data = await client.get("spaces/coworkers", {"page": 1, "size": 2})
        total = data.get("TotalItems")
        records = data.get("Records", [])
        print(f"\n  Nexudus spaces/coworkers TotalItems = {total}")
        if records:
            row = transform_coworker(records[0], 0, "dry-run")
            for k in ("source_id", "full_name", "email", "company_name",
                      "location_source_id", "location_name", "active"):
                print(f"    {k:<22} = {row[k]}")

    counts = _sql_counts()
    print(f"\n  bronze.nexudus_coworkers distinct = {counts['bronze_total']}")
    print(f"  silver.nexudus_coworkers total    = {counts['silver_total']}")
    print(f"  silver.nexudus_coworkers active   = {counts['silver_active']}")
    gap = (total or 0) - counts["silver_total"]
    print(f"\n  Gap to backfill ≈ {gap} coworkers")


async def run_bronze(run_id):
    _section("1. Bronze (production _sync_coworkers, force_full=True)")
    from functions.bronze_nexudus import _sync_coworkers
    from shared.azure_clients.bronze_writer import BronzeWriter
    from shared.nexudus.auth import get_bearer_token
    from shared.nexudus.client import NexudusClient

    writer = BronzeWriter(run_id)
    async with NexudusClient(get_bearer_token()) as client:
        await _sync_coworkers(client, _SafeBlobWriter(), writer, run_id, force_full=True)


async def run_silver(run_id):
    _section("2. Silver (writer, RunTracker-tracked like the queue worker)")
    from shared.azure_clients.run_tracker import RunTracker
    from shared.azure_clients.silver_writer_coworkers import SilverCoworkersWriter

    async with RunTracker("nexudus", "coworkers", "silver", metadata=str(run_id)) as run:
        result = SilverCoworkersWriter(run_id).run()
        run.rows_read = int(result.get("rows_read") or 0)
        run.rows_written = int(result.get("coworkers") or 0)
        print(f"  coworkers: {result}")


def verify():
    _section("3. Verification")
    counts = _sql_counts()
    print(f"  bronze.nexudus_coworkers distinct = {counts['bronze_total']}")
    print(f"  silver.nexudus_coworkers total    = {counts['silver_total']}")
    print(f"  silver.nexudus_coworkers active   = {counts['silver_active']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true",
                        help="Run the full bronze -> silver backfill against SQL.")
    args = parser.parse_args()

    if args.write:
        run_id = uuid.uuid4()
        asyncio.run(run_bronze(run_id))
        asyncio.run(run_silver(run_id))
        verify()
    else:
        asyncio.run(dry_run())
        print("\n  Dry run complete. Add --write to backfill.")


if __name__ == "__main__":
    main()
