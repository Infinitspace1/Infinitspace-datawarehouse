"""
scripts/python_scripts/apply_dayweighted_occupancy.py

Day-weighted occupancy cutover (2026-08-31, approved by Bryan). occupancy_pct
in the landlord book views changes basis from the month-end position rule to
occupied DESK-DAYS / (capacity x days in month) — the Budget Tracker's
formula. Revenue and all desk-count columns keep the original rule.

PAST DATA IS NOT RESTATED (explicit decision 2026-08-31): months before
August 2026 keep their frozen numbers on the old basis — the frozen rows'
occupied_ws_avg stays NULL and readers fall back to the position count.
The new basis applies from August 2026 forward.

What it applies (extracted batches only — the schema files also contain
DROP/backfill sections that must NOT be re-run wholesale):
  1. gold.vw_landlord_membership_book_monthly   (landlord_dashboard_revenue_schema.sql)
  2. gold.vw_landlord_contract_book_monthly     (landlord_dashboard_schema.sql)
  3. ALTER silver.landlord_frozen_monthly_occupancy ADD occupied_ws_avg
  4. gold.vw_landlord_occupancy_combined        (landlord_dashboard_occupancy_freeze_schema.sql)
  5. gold.sp_refresh_finance_dashboard          (core_finance_dashboard_schema.sql)

Then the data ops:
  6. Pre-freeze 2026-08 (source='manual_override') with BOTH the position
     count and the day-weighted average — tomorrow's Sep-1 cron (old deployed
     code, INT-only) then skips it via its WHERE NOT EXISTS guard.
  7. Rematerialize the three t_landlord_* tables (same column lists as
     functions/landlord_materialize_dashboard.py).
  8. EXEC gold.sp_refresh_finance_dashboard.
  9. Verification queries.

Usage:
    python scripts/python_scripts/apply_dayweighted_occupancy.py [--dry-run]
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client

SQL_DIR = ROOT / "scripts" / "sql_scripts"


def split_batches(script: str) -> list[str]:
    batches, current = [], []
    for line in script.splitlines():
        if line.strip().upper() == "GO":
            batch = "\n".join(current).strip()
            if batch:
                batches.append(batch)
            current = []
        else:
            current.append(line)
    batch = "\n".join(current).strip()
    if batch:
        batches.append(batch)
    return batches


def find_batch(sql_file: Path, marker: str) -> str:
    text = sql_file.read_text(encoding="utf-8-sig")
    matches = [b for b in split_batches(text) if marker in b]
    if len(matches) != 1:
        raise SystemExit(
            f"Expected exactly 1 batch containing {marker!r} in {sql_file.name}, "
            f"found {len(matches)}"
        )
    return matches[0]


# (file, unique marker) in apply order — membership book first because the
# combined view references its new occupied_ws_avg column.
BATCHES = [
    (SQL_DIR / "landlord_dashboard_revenue_schema.sql",
     "VIEW gold.vw_landlord_membership_book_monthly"),
    (SQL_DIR / "landlord_dashboard_schema.sql",
     "VIEW gold.vw_landlord_contract_book_monthly"),
    (SQL_DIR / "landlord_dashboard_occupancy_freeze_schema.sql",
     "ADD occupied_ws_avg"),
    (SQL_DIR / "landlord_dashboard_occupancy_freeze_schema.sql",
     "VIEW gold.vw_landlord_occupancy_combined"),
    (SQL_DIR / "core_finance_dashboard_schema.sql",
     "PROCEDURE gold.sp_refresh_finance_dashboard"),
]

PREFREEZE_AUGUST = """
INSERT INTO silver.landlord_frozen_monthly_occupancy
    (location_source_id, period, occupied_workstations, occupied_ws_avg, source, notes)
SELECT
    mb.location_source_id,
    mb.period,
    mb.occupied_workstations,
    mb.occupied_ws_avg,
    N'manual_override',
    N'Pre-frozen 2026-08-31 at day-weighted cutover (both bases); Sep-1 cron skips via NOT EXISTS'
FROM gold.vw_landlord_membership_book_monthly mb
WHERE mb.period = '2026-08'
  AND mb.location_source_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM silver.landlord_frozen_monthly_occupancy f
      WHERE f.location_source_id = mb.location_source_id
        AND f.period = mb.period
  )
"""

# Mirrors functions/landlord_materialize_dashboard.py _REFRESHES exactly.
REFRESHES = [
    ("gold.vw_landlord_contract_book_monthly", "gold.t_landlord_contract_book_monthly", [
        "period", "month_start_date", "location_source_id", "location_name",
        "location_city", "location_country_name", "total_workstation_capacity",
        "active_contract_count", "occupied_workstations", "vacant_workstations",
        "occupancy_pct", "sold_monthly_revenue", "list_monthly_revenue",
        "avg_sold_price_per_ws", "avg_list_price_per_ws", "avg_discount_pct",
        "discount_monthly_value", "private_office_contract_count",
        "private_office_capacity", "private_office_sold_revenue",
        "private_office_list_revenue", "new_workstations_starting",
        "workstations_cancelling", "net_workstation_change",
        "contracts_missing_list_price", "adjustment_contract_count",
        "adjustment_monthly_value", "calculation_basis",
    ]),
    ("gold.vw_landlord_membership_book_monthly", "gold.t_landlord_membership_book_monthly", [
        "period", "month_start_date", "location_source_id", "location_name",
        "location_city", "location_country_name", "total_workstation_capacity",
        "active_contract_count", "occupied_workstations", "vacant_workstations",
        "occupancy_pct", "sold_monthly_revenue", "list_monthly_revenue",
        "avg_sold_price_per_ws", "avg_list_price_per_ws",
        "adjustment_contract_count", "adjustment_monthly_value",
        "calculation_basis",
    ]),
    ("gold.vw_landlord_revenue_past_location_monthly", "gold.t_landlord_revenue_past_location_monthly", [
        "period", "month_start_date", "location_source_id", "location_name",
        "currency_code", "sold_monthly_revenue", "line_count",
        "negative_line_count", "member_count",
    ]),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="print the batches that would run, change nothing")
    args = parser.parse_args()

    resolved = [(f.name, marker, find_batch(f, marker)) for f, marker in BATCHES]

    if args.dry_run:
        for name, marker, batch in resolved:
            print(f"-- from {name} ({marker}): {len(batch)} chars")
        print("-- plus: pre-freeze 2026-08, rematerialize x3, EXEC finance proc")
        print("-- NOTE: no restatement of months before 2026-08 (explicit decision)")
        return

    sql = get_sql_client()

    for name, marker, batch in resolved:
        print(f"applying [{name}] {marker} ...")
        sql.execute_non_query(batch)
    print("schema batches applied.\n")

    n = sql.execute_non_query(PREFREEZE_AUGUST)
    print(f"pre-froze 2026-08: {n} rows")

    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat(sep=" ")
    for source, target, cols in REFRESHES:
        collist = ", ".join(cols)
        sql.execute_non_query(f"""
            BEGIN TRANSACTION;
            TRUNCATE TABLE {target};
            INSERT INTO {target} ({collist}, refreshed_at)
            SELECT {collist}, CAST(? AS DATETIME2(0))
            FROM {source};
            COMMIT TRANSACTION;
        """, (now_iso,))
        c = sql.execute_query(f"SELECT COUNT(*) AS c FROM {target}")[0]["c"]
        print(f"rematerialized {target}: {c} rows")

    sql.execute_non_query("EXEC gold.sp_refresh_finance_dashboard")
    print("finance dashboard snapshot refreshed.\n")

    # -- Verification -------------------------------------------------------
    print("=== Aug-2026 membership book: new day-weighted vs old month-end pct ===")
    for r in sql.execute_query("""
        SELECT location_name, occupancy_pct, occupancy_pct_eom,
               occupied_workstations, occupied_ws_avg, total_workstation_capacity
        FROM gold.vw_landlord_membership_book_monthly
        WHERE period = '2026-08' AND total_workstation_capacity > 0
        ORDER BY location_name
    """):
        print(f"  {r['location_name'][:45]:<45} new={r['occupancy_pct']} old={r['occupancy_pct_eom']} "
              f"pos={r['occupied_workstations']} avg={r['occupied_ws_avg']} cap={r['total_workstation_capacity']}")

    print("=== frozen table 2026-06..2026-08 (Jun/Jul must have avg=None) ===")
    for r in sql.execute_query("""
        SELECT f.period, loc.name AS location_name, f.occupied_workstations,
               f.occupied_ws_avg, f.source
        FROM silver.landlord_frozen_monthly_occupancy f
        LEFT JOIN silver.nexudus_locations loc ON loc.source_id = f.location_source_id
        WHERE f.period IN ('2026-06','2026-07','2026-08')
        ORDER BY f.period, loc.name
    """):
        print(f"  {r['period']} {str(r['location_name'])[:40]:<40} pos={r['occupied_workstations']} "
              f"avg={r['occupied_ws_avg']} src={r['source']}")

    print("=== t_landlord_contract_book_monthly Aug-2026 (dashboard reads this) ===")
    for r in sql.execute_query("""
        SELECT location_name, occupancy_pct, occupied_workstations, total_workstation_capacity
        FROM gold.t_landlord_contract_book_monthly
        WHERE period = '2026-08' AND total_workstation_capacity > 0
        ORDER BY location_name
    """):
        print(f"  {r['location_name'][:45]:<45} pct={r['occupancy_pct']} "
              f"pos={r['occupied_workstations']}/{r['total_workstation_capacity']}")

    print("=== finance snapshot (today) ===")
    for r in sql.execute_query("""
        SELECT location_name, occupancy_pct, occupied_workstations, total_workstation_capacity
        FROM gold.finance_dashboard_revenue_occupancy
        WHERE as_of_date_utc = CAST(GETUTCDATE() AS DATE) AND total_workstation_capacity > 0
        ORDER BY location_name
    """):
        print(f"  {r['location_name'][:45]:<45} pct={r['occupancy_pct']} "
              f"pos={r['occupied_workstations']}/{r['total_workstation_capacity']}")


if __name__ == "__main__":
    main()
