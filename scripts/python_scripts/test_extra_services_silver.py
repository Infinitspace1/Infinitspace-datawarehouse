"""
scripts/python_scripts/test_extra_services_silver.py

Tests the full extra services pipeline end-to-end locally:
  bronze.nexudus_extra_services → transform → silver.nexudus_extra_services

Usage:
    python scripts/python_scripts/test_extra_services_silver.py            # dry run
    python scripts/python_scripts/test_extra_services_silver.py --write    # write to silver
"""
import argparse
import json
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")


def _section(title: str):
    print(f"\n{'─'*60}\n  {title}\n{'─'*60}")


def test_bronze(sql):
    _section("1. Check bronze.nexudus_extra_services")

    count = sql.execute_scalar("SELECT COUNT(*) FROM bronze.nexudus_extra_services")
    print(f"\n  Rows in bronze: {count}")

    if not count:
        print("  No data — run test_local.py --step extra_services first")
        sys.exit(1)

    latest = sql.execute_query("""
        SELECT TOP 3 id, source_id, location_id, synced_at
        FROM bronze.nexudus_extra_services
        ORDER BY synced_at DESC
    """)
    print("\n  Latest bronze rows:")
    for r in latest:
        print(f"    id={r['id']}  source_id={r['source_id']}  location_id={r['location_id']}  synced_at={r['synced_at']}")

    return count


def test_transform(sql):
    _section("2. Transform dry-run (no SQL writes)")
    from shared.nexudus.transformers.extra_services import transform_extra_service

    rows = sql.execute_query("""
        SELECT TOP 15 b.id, b.raw_json
        FROM bronze.nexudus_extra_services b
        INNER JOIN (
            SELECT source_id, MAX(synced_at) AS latest
            FROM bronze.nexudus_extra_services
            GROUP BY source_id
        ) latest ON b.source_id = latest.source_id AND b.synced_at = latest.latest
        ORDER BY b.source_id
    """)

    ok = errors = 0
    for row in rows:
        raw = json.loads(row["raw_json"])
        try:
            es = transform_extra_service(raw, row["id"], "dry-run")
            default_tag = " [DEFAULT]" if es["is_default_price"] else ""
            members_tag = " [MEMBERS]" if es["only_for_members"] else ""
            print(f"\n  ✅  {es['source_id']}  '{es['name']}'{default_tag}{members_tag}")
            print(f"      location={es['location_source_id']}  currency={es['currency_code']}  price={es['price']}  charge_period={es['charge_period']}")
            if es["resource_type_names"]:
                print(f"      resource_types='{es['resource_type_names'][:80]}'")
            if es["fixed_cost_price"]:
                print(f"      fixed_cost={es['fixed_cost_price']} for {es['fixed_cost_length_minutes']}min")
            if es["apply_from"] or es["apply_to"]:
                print(f"      window: {es['apply_from']} → {es['apply_to']}")
            ok += 1
        except Exception as e:
            print(f"\n  ❌  source_id={raw.get('Id')}: {e}")
            errors += 1

    print(f"\n  {ok} ok, {errors} errors")
    return errors == 0


def test_write_silver(sql):
    _section("3. Writing to silver.nexudus_extra_services")
    from shared.azure_clients.silver_writer_extra_services import SilverExtraServicesWriter

    counts = SilverExtraServicesWriter(uuid.uuid4()).run()
    print(f"\n  extra_services: {counts['extra_services']}  errors: {counts['errors']}")

    _section("4. Verification")
    summary = sql.execute_query("""
        SELECT
            currency_code,
            charge_period,
            COUNT(*)                            AS total,
            COUNT(resource_type_names)          AS with_resource_types,
            COUNT(fixed_cost_price)             AS with_fixed_cost,
            SUM(CAST(is_default_price AS INT))  AS default_prices,
            SUM(CAST(only_for_members AS INT))  AS members_only
        FROM silver.nexudus_extra_services
        GROUP BY currency_code, charge_period
        ORDER BY currency_code, charge_period
    """)
    print(f"\n  {'Currency':<10} {'Period':<8} {'Total':>6} {'w/ResTypes':>12} {'w/FixedCost':>12} {'Default':>8} {'MembersOnly':>12}")
    print(f"  {'─'*72}")
    for r in summary:
        print(f"  {r['currency_code'] or '?':<10} {str(r['charge_period']) or '?':<8} "
              f"{r['total']:>6} {r['with_resource_types']:>12} {r['with_fixed_cost']:>12} "
              f"{r['default_prices']:>8} {r['members_only']:>12}")

    by_location = sql.execute_query("""
        SELECT
            l.name AS location_name,
            COUNT(es.id) AS total_services,
            COUNT(DISTINCT es.currency_code) AS currencies,
            MIN(es.price) AS min_price,
            MAX(es.price) AS max_price
        FROM silver.nexudus_extra_services es
        LEFT JOIN silver.nexudus_locations l ON es.location_source_id = l.source_id
        GROUP BY l.name
        ORDER BY total_services DESC
    """)
    print(f"\n  Extra services by location:")
    for r in by_location:
        print(f"    {r['location_name'] or '(unknown)':<45} services={r['total_services']:>4}  "
              f"price range: {r['min_price']} - {r['max_price']} ({r['currencies']} currency)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="Write to silver tables")
    args = parser.parse_args()

    from shared.azure_clients.sql_client import get_sql_client
    sql = get_sql_client()

    test_bronze(sql)
    ok = test_transform(sql)

    if not ok:
        print("\n  Fix transform errors before writing to silver")
        sys.exit(1)

    if args.write:
        test_write_silver(sql)
    else:
        print("\n  Dry run complete. Add --write to push to silver.")


if __name__ == "__main__":
    main()
