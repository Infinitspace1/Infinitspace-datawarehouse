"""
scripts/python_scripts/test_contracts_silver.py

Tests the full contracts pipeline end-to-end locally:
  bronze.nexudus_contracts → transform → silver.nexudus_contracts

Usage:
    python scripts/python_scripts/test_contracts_silver.py            # dry run
    python scripts/python_scripts/test_contracts_silver.py --write    # write to silver
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
    _section("1. Check bronze.nexudus_contracts")

    count = sql.execute_scalar("SELECT COUNT(*) FROM bronze.nexudus_contracts")
    print(f"\n  Rows in bronze: {count}")

    if not count:
        print("  No data — run test_local.py --step contracts first")
        sys.exit(1)

    latest = sql.execute_query("""
        SELECT TOP 3 id, source_id, synced_at
        FROM bronze.nexudus_contracts
        ORDER BY synced_at DESC
    """)
    print("\n  Latest bronze rows:")
    for r in latest:
        print(f"    id={r['id']}  source_id={r['source_id']}  synced_at={r['synced_at']}")

    return count


def test_transform(sql):
    _section("2. Transform dry-run (no SQL writes)")
    from shared.nexudus.transformers.contracts import transform_contract

    rows = sql.execute_query("""
        SELECT TOP 15 b.id, b.raw_json
        FROM bronze.nexudus_contracts b
        INNER JOIN (
            SELECT source_id, MAX(synced_at) AS latest
            FROM bronze.nexudus_contracts
            GROUP BY source_id
        ) latest ON b.source_id = latest.source_id AND b.synced_at = latest.latest
        ORDER BY b.source_id
    """)

    ok = errors = 0
    for row in rows:
        raw = json.loads(row["raw_json"])
        try:
            c = transform_contract(raw, row["id"], "dry-run")
            status = "ACTIVE" if c["active"] else ("CANCELLED" if c["cancelled"] else "INACTIVE")
            print(f"\n  ✅  [{status}] {c['source_id']}  '{c['coworker_name']}'")
            print(f"      location='{c['location_name']}'  tariff='{c['tariff_name']}'")
            print(f"      price={c['price']} {c['currency_code']}  qty={c['quantity']}  billing_day={c['billing_day']}")
            print(f"      start={c['start_date']}  term={c['contract_term']}  renewal={c['renewal_date']}")
            print(f"      desk_ids='{c['floor_plan_desk_ids']}'  term_months={c['term_duration_months']}")
            ok += 1
        except Exception as e:
            print(f"\n  ❌  source_id={raw.get('Id')}: {e}")
            errors += 1

    print(f"\n  {ok} ok, {errors} errors")
    return errors == 0


def test_write_silver(sql):
    _section("3. Writing to silver.nexudus_contracts")
    from shared.azure_clients.silver_writer_contracts import SilverContractsWriter

    counts = SilverContractsWriter(uuid.uuid4()).run()
    print(f"\n  contracts: {counts['contracts']}  errors: {counts['errors']}")

    _section("4. Verification")
    summary = sql.execute_query("""
        SELECT
            active,
            cancelled,
            COUNT(*)                            AS total,
            COUNT(DISTINCT coworker_id)         AS unique_coworkers,
            COUNT(DISTINCT location_source_id)  AS locations,
            COUNT(floor_plan_desk_ids)          AS with_desk_ids,
            COUNT(contract_term)                AS with_term_date
        FROM silver.nexudus_contracts
        GROUP BY active, cancelled
        ORDER BY active DESC, cancelled
    """)
    print(f"\n  {'Active':<7} {'Canc':<6} {'Total':>6} {'Coworkers':>10} {'Locations':>10} {'w/Desks':>8} {'w/Term':>7}")
    print(f"  {'─'*57}")
    for r in summary:
        print(f"  {r['active']:<7} {r['cancelled']:<6} {r['total']:>6} "
              f"{r['unique_coworkers']:>10} {r['locations']:>10} "
              f"{r['with_desk_ids']:>8} {r['with_term_date']:>7}")

    by_location = sql.execute_query("""
        SELECT
            location_name,
            COUNT(*) AS total,
            SUM(CAST(active AS INT)) AS active
        FROM silver.nexudus_contracts
        GROUP BY location_name
        ORDER BY total DESC
    """)
    print(f"\n  Contracts by location:")
    for r in by_location:
        print(f"    {r['location_name'] or '(no location)':<45} total={r['total']:>4}  active={r['active']:>4}")


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
