"""
scripts/python_scripts/test_eventbrite_sync.py

Local end-to-end check for the Eventbrite events sync.

  --dry-run (default): connect to Eventbrite, list accessible organizations,
      fetch all events with expansions, print counts and a transformed sample.
      No SQL writes — needs only EVENTBRITE_PRIVATE_TOKEN.
  --write: full path — Eventbrite -> bronze -> silver -> reconcile. Needs SQL
      too (and the tables from scripts/sql_scripts/eventbrite_events_schema.sql).

Usage:
    python scripts/python_scripts/test_eventbrite_sync.py
    python scripts/python_scripts/test_eventbrite_sync.py --write
"""
import argparse
import sys
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


def fetch():
    _section("1. Connect to Eventbrite + list organizations")
    from shared.eventbrite.client import fetch_events, fetch_organizations

    orgs = fetch_organizations()
    print(f"\n  Organizations: {len(orgs)}")
    for o in orgs:
        print(f"    {o.get('id'):<20} {o.get('name')}")

    _section("2. Fetch events (status=all, with expansions)")
    events = []
    for o in orgs:
        org_events = fetch_events(str(o.get("id")), status="all")
        print(f"    org {o.get('id')}: {len(org_events)} events")
        events.extend(org_events)

    statuses: dict = {}
    for e in events:
        statuses[e.get("status")] = statuses.get(e.get("status"), 0) + 1
    print(f"\n  Events total: {len(events)}")
    for status, count in sorted(statuses.items(), key=lambda kv: -kv[1]):
        print(f"    {status or '?':<20} {count}")

    sample = events[0] if events else None
    if sample:
        print(f"\n  Top-level keys: {sorted(sample.keys())}")
    return events


def dry_run_transform(events):
    _section("3. Transform dry-run (no SQL writes)")
    from shared.eventbrite.transformers.events import transform_event

    sample = next((e for e in events if e.get("venue")), events[0] if events else None)
    if not sample:
        print("\n  No events found.")
        return

    ev = transform_event(sample, 0, "dry-run")
    print(f"\n  Event sample:")
    print(f"    source_id = {ev['source_id']}")
    print(f"    name      = {ev['name']}")
    print(f"    status    = {ev['status']}   online = {ev['online_event']}")
    print(f"    start     = {ev['start_utc']} ({ev['timezone']})")
    print(f"    venue     = {ev['venue_name']}  {ev['venue_address']}")
    print(f"    city      = {ev['venue_city']}   country = {ev['venue_country']}")
    print(f"    capacity  = {ev['capacity']}   sold_out = {ev['is_sold_out']}")
    print(f"    tickets   = {ev['minimum_ticket_price_display']} - "
          f"{ev['maximum_ticket_price_display']}   free = {ev['is_free']}")
    print(f"    url       = {ev['url']}")


def run_sync():
    """Run the real production path (RunTracker rows advance the silver
    watermark exactly like the deployed timer)."""
    import asyncio

    from functions.eventbrite_sync import run_eventbrite_sync

    _section("4. Running Eventbrite sync (bronze -> silver -> reconcile)")
    summary = asyncio.run(run_eventbrite_sync())
    print(f"\n  summary: {summary}")
    _verify()


def _verify():
    _section("5. Verification (silver.eventbrite_events)")
    from shared.azure_clients.sql_client import get_sql_client

    sql = get_sql_client()
    rows = sql.execute_query(
        """
        SELECT status,
               COUNT(*) AS total,
               SUM(CASE WHEN is_deleted = 0 THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN venue_id IS NOT NULL THEN 1 ELSE 0 END) AS with_venue
        FROM silver.eventbrite_events
        GROUP BY status
        ORDER BY total DESC
        """
    )
    print(f"\n  {'Status':<16} {'Total':>7} {'Active':>7} {'w/Venue':>8}")
    print(f"  {'-'*42}")
    for r in rows:
        print(f"  {(r['status'] or '?'):<16} {r['total']:>7} {r['active']:>7} {r['with_venue']:>8}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true",
                        help="Run the sync (bronze + silver + reconcile).")
    args = parser.parse_args()

    if args.write:
        run_sync()
    else:
        events = fetch()
        dry_run_transform(events)
        print("\n  Dry run complete. Add --write to persist.")


if __name__ == "__main__":
    main()
