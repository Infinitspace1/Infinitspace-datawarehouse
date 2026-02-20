"""
scripts/python_scripts/test_locations_silver.py

Tests the full locations pipeline end-to-end locally:
  bronze.nexudus_locations → transform → silver.nexudus_locations + silver.nexudus_location_hours

Steps:
  1. Verify bronze has data
  2. Run the transformer on sample records (no SQL writes) — dry run
  3. Optionally write to silver

Usage:
    python scripts/python_scripts/test_locations_silver.py
    python scripts/python_scripts/test_locations_silver.py --write    # actually write to silver
"""
import argparse
import json
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))


from dotenv import load_dotenv
load_dotenv(ROOT / ".env")


def _section(title: str):
    print(f"\n{'─'*60}\n  {title}\n{'─'*60}")


def test_bronze_has_data(sql):
    _section("1. Check bronze.nexudus_locations")

    count = sql.execute_scalar("SELECT COUNT(*) FROM bronze.nexudus_locations")
    print(f"\n  Rows in bronze: {count}")

    if not count:
        print("  ❌  No data — run test_local.py --step locations first")
        sys.exit(1)

    latest = sql.execute_query("""
        SELECT TOP 3 id, source_id, synced_at
        FROM bronze.nexudus_locations
        ORDER BY synced_at DESC
    """)
    print("\n  Latest bronze rows:")
    for r in latest:
        print(f"    id={r['id']}  source_id={r['source_id']}  synced_at={r['synced_at']}")

    return count


def test_transform(sql):
    _section("2. Transform dry-run (no SQL writes)")
    from shared.nexudus.transformers.locations import transform_location, transform_location_hours

    rows = sql.execute_query("""
        SELECT TOP 11 id, raw_json
        FROM bronze.nexudus_locations
        ORDER BY synced_at DESC
    """)

    ok = 0
    skipped = 0
    errors = 0

    for row in rows:
        raw = json.loads(row["raw_json"])
        source_id = raw.get("Id", "?")

        try:
            loc = transform_location(raw, bronze_id=row["id"], sync_run_id="dry-run")
            hours = transform_location_hours(raw)

            if loc is None:
                print(f"\n  ↷  source_id={source_id} skipped by transformer")
                skipped += 1
                continue

            print(f"\n  ✅  source_id={source_id}  name='{loc['name']}'")
            print(f"      city={loc['city']}  country={loc['country_name']}  currency={loc['currency_code']}")
            print(f"      lat={loc['latitude']}  lng={loc['longitude']}")
            print(f"      email={loc['email']}  phone={loc['phone']}")
            print(f"      description snippet: {str(loc['description'])[:80]}...")

            print(f"      Opening hours ({len(hours)} days):")
            for h in hours:
                if h["is_closed"]:
                    print(f"        {h['day_name']}: CLOSED")
                else:
                    def fmt(mins):
                        if mins is None:
                            return "?"
                        return f"{mins // 60:02d}:{mins % 60:02d}"
                    print(f"        {h['day_name']}: {fmt(h['open_time'])} – {fmt(h['close_time'])}")

            ok += 1
        except Exception as e:
            print(f"\n  ❌  source_id={source_id}: {e}")
            errors += 1

    print(f"\n  Transform results: {ok} ok, {skipped} skipped, {errors} errors")
    return errors == 0


def test_write_silver(sql):
    _section("3. Writing to silver tables")
    from shared.azure_clients.silver_write_locations import SilverLocationsWriter

    run_id = uuid.uuid4()
    writer = SilverLocationsWriter(run_id)
    loc_count, hours_count = writer.run()

    print(f"\n  Written: {loc_count} locations, {hours_count} hours rows")

    # Verify what's in silver
    silver_rows = sql.execute_query("""
        SELECT
            l.source_id, l.name, l.city, l.country_name, l.currency_code,
            COUNT(h.id) AS hours_rows
        FROM silver.nexudus_locations l
        LEFT JOIN silver.nexudus_location_hours h ON h.location_source_id = l.source_id
        GROUP BY l.source_id, l.name, l.city, l.country_name, l.currency_code
        ORDER BY l.name
    """)

    print(f"\n  silver.nexudus_locations ({len(silver_rows)} rows):")
    for r in silver_rows:
        print(f"    [{r['source_id']}] {r['name']} | {r['city']}, {r['country_name']} | {r['currency_code']} | {r['hours_rows']} hour rows")

    # Check hours make sense
    closed_days = sql.execute_query("""
        SELECT l.name, h.day_name, h.open_time, h.close_time
        FROM silver.nexudus_location_hours h
        JOIN silver.nexudus_locations l ON l.source_id = h.location_source_id
        WHERE h.is_closed = 0
          AND (h.open_time IS NULL OR h.close_time IS NULL)
    """)
    if closed_days:
        print(f"\n  ⚠️  Locations with is_closed=0 but missing times ({len(closed_days)} rows):")
        for r in closed_days:
            print(f"    {r['name']} — {r['day_name']}: open={r['open_time']} close={r['close_time']}")
    else:
        print("\n  ✅  All open days have valid times")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="Write to silver tables")
    args = parser.parse_args()

    from shared.azure_clients.sql_client import get_sql_client
    sql = get_sql_client()

    test_bronze_has_data(sql)
    transform_ok = test_transform(sql)

    if not transform_ok:
        print("\n❌ Transform errors found — fix before writing to silver")
        sys.exit(1)

    if args.write:
        test_write_silver(sql)
    else:
        print("\n  ℹ️  Dry run complete. Add --write to push to silver tables.")


if __name__ == "__main__":
    main()
