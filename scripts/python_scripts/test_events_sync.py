"""
scripts/python_scripts/test_events_sync.py

Local end-to-end check for the Nexudus events entities
(calendar_events, event_attendees, event_products).

  --write: run the REAL production path — the same _sync_* functions the
      nightly nexudus_to_bronze timer calls (RunTracker-tracked, so the
      incremental watermark advances exactly like production), then the
      three silver writers. Needs Nexudus creds + SQL; blob snapshots are
      skipped gracefully when no Azure credential is available locally.
  default (dry-run): fetch + transform a sample, no SQL writes.

Usage:
    python scripts/python_scripts/test_events_sync.py
    python scripts/python_scripts/test_events_sync.py --write
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


def _get_blob_writer():
    return _SafeBlobWriter()


async def dry_run():
    _section("Dry run: fetch + transform samples (no SQL writes)")
    from shared.nexudus.auth import get_bearer_token
    from shared.nexudus.client import NexudusClient
    from shared.nexudus.transformers.calendar_events import transform_calendar_event
    from shared.nexudus.transformers.event_attendees import transform_event_attendee
    from shared.nexudus.transformers.event_products import transform_event_product

    async with NexudusClient(get_bearer_token()) as client:
        for path, transform in [
            ("content/calendarevents", transform_calendar_event),
            ("content/eventattendees", transform_event_attendee),
            ("content/eventproducts", transform_event_product),
        ]:
            data = await client.get(path, {"page": 1, "size": 2})
            records = data.get("Records", [])
            total = data.get("TotalItems")
            print(f"\n  {path}: {total} total")
            if records:
                row = transform(records[0], 0, "dry-run")
                for k in ("source_id", "name", "calendar_event_source_id",
                          "location_source_id", "email", "price"):
                    if k in row:
                        print(f"    {k:<26} = {row[k]}")


async def run_bronze(run_id):
    _section("1. Bronze (production _sync_* functions)")
    from functions.bronze_nexudus import (
        _sync_calendar_events,
        _sync_event_attendees,
        _sync_event_products,
    )
    from shared.azure_clients.bronze_writer import BronzeWriter
    from shared.nexudus.auth import get_bearer_token
    from shared.nexudus.client import NexudusClient

    blob_writer = _get_blob_writer()
    writer = BronzeWriter(run_id)
    async with NexudusClient(get_bearer_token()) as client:
        await _sync_calendar_events(client, blob_writer, writer, run_id)
        await _sync_event_attendees(client, blob_writer, writer, run_id)
        await _sync_event_products(client, blob_writer, writer, run_id)


async def run_silver(run_id):
    _section("2. Silver (writers, RunTracker-tracked like the queue worker)")
    from shared.azure_clients.run_tracker import RunTracker
    from shared.azure_clients.silver_writer_calendar_events import SilverCalendarEventsWriter
    from shared.azure_clients.silver_writer_event_attendees import SilverEventAttendeesWriter
    from shared.azure_clients.silver_writer_event_products import SilverEventProductsWriter

    for entity, writer_cls in [
        ("calendar_events", SilverCalendarEventsWriter),
        ("event_attendees", SilverEventAttendeesWriter),
        ("event_products", SilverEventProductsWriter),
    ]:
        async with RunTracker("nexudus", entity, "silver", metadata=str(run_id)) as run:
            result = writer_cls(run_id).run()
            run.rows_read = int(result.get("rows_read") or 0)
            run.rows_written = int(result.get(entity) or 0)
            print(f"  {entity}: {result}")


def verify():
    _section("3. Verification (silver row counts + linking)")
    from shared.azure_clients.sql_client import get_sql_client

    sql = get_sql_client()
    rows = sql.execute_query(
        """
        SELECT 'calendar_events' AS entity, COUNT(*) AS total,
               SUM(CASE WHEN is_deleted = 0 THEN 1 ELSE 0 END) AS active
        FROM silver.nexudus_calendar_events
        UNION ALL
        SELECT 'event_attendees', COUNT(*),
               SUM(CASE WHEN is_deleted = 0 THEN 1 ELSE 0 END)
        FROM silver.nexudus_event_attendees
        UNION ALL
        SELECT 'event_products', COUNT(*),
               SUM(CASE WHEN is_deleted = 0 THEN 1 ELSE 0 END)
        FROM silver.nexudus_event_products
        """
    )
    for r in rows:
        print(f"  {r['entity']:<18} total={r['total']:>6}  active={r['active']:>6}")

    links = sql.execute_query(
        """
        SELECT
            (SELECT COUNT(*) FROM silver.nexudus_event_attendees a
             JOIN silver.nexudus_calendar_events e
               ON e.source_id = a.calendar_event_source_id) AS attendees_linked_to_event,
            (SELECT COUNT(*) FROM silver.nexudus_event_products p
             JOIN silver.nexudus_calendar_events e
               ON e.source_id = p.calendar_event_source_id) AS products_linked_to_event,
            (SELECT COUNT(*) FROM silver.nexudus_event_products
             WHERE location_source_id IS NOT NULL) AS products_with_location,
            (SELECT COUNT(*) FROM silver.nexudus_calendar_events ce
             JOIN silver.nexudus_locations l
               ON l.source_id = ce.location_source_id) AS events_linked_to_location
        """
    )
    for k, v in links[0].items():
        print(f"  {k:<28} = {v}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true",
                        help="Run the full bronze -> silver path against SQL.")
    args = parser.parse_args()

    if args.write:
        run_id = uuid.uuid4()
        asyncio.run(run_bronze(run_id))
        asyncio.run(run_silver(run_id))
        verify()
    else:
        asyncio.run(dry_run())
        print("\n  Dry run complete. Add --write to persist.")


if __name__ == "__main__":
    main()
