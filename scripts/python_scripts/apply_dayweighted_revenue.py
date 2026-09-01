"""
scripts/python_scripts/apply_dayweighted_revenue.py

Day-weighted REVENUE (2026-09-01, requested by Bryan). Companion to
apply_dayweighted_occupancy.py, which moved occupancy_pct to desk-days on
2026-08-31 and deliberately left revenue on the month-end rule.

The month-end rule prices a month by the contracts alive on its LAST DAY, each
at a full monthly fee. That is wrong at both ends, and beyond The Bower hit both
in Aug-2026:
  * Faculty Science cancelled on the 19th (GBP 70,975/mo) -> scored 0, despite
    holding the space 19 of 31 days.
  * Capi Money started on the 23rd (GBP 40,500/mo, 90 desks) -> scored a FULL
    month for 8 days.
Day-weighting gives 70,975 x 19/31 = 43,500.81 and 40,500 x 8/31 = 10,451.61,
matching what was actually invoiced to the penny. Net effect on that month:
160,353.06 -> 173,925.00.

ADDITIVE, NOT A RESTATEMENT. Every existing column keeps its current value and
meaning; the new basis arrives as a SEPARATE column that only the landlord
dashboard reads:
  * gold.vw_landlord_contract_book_monthly    + sold_revenue_day_weighted
  * gold.vw_landlord_membership_book_monthly  + sold_revenue_day_weighted
  * gold.vw_landlord_company_type_book_monthly
        + rev_{private_office,dedicated_desk,hot_desk,additional}_day_weighted
        + mmrf_day_weighted / marv_day_weighted / total_monthly_fee_day_weighted
Verified by dry-run before writing this script: all 32 / 22 / 18 pre-existing
columns come back byte-identical on every row.

gold.sp_refresh_finance_dashboard and silver.* are deliberately NOT touched -
the separate finance dashboard keeps reporting month-end revenue until someone
decides to move it too.

ALSO RECOVERS A LOST HOTFIX. The deployed
gold.vw_landlord_company_type_book_monthly had drifted from the repo: prod tests
future-signed contracts with CAST(DATEADD(HOUR, 4, start_date) AS DATE) >=
GETUTCDATE(), the committed file still had the pre-+4h CAST(start_date) >
GETUTCDATE(). Applying the repo file as it stood would have silently reverted
that and moved 139 company-months. The prod version is now in the file.

What it does:
  1. gold.vw_landlord_contract_book_monthly     (landlord_dashboard_schema.sql)
  2. gold.vw_landlord_membership_book_monthly   (landlord_dashboard_revenue_schema.sql)
  3. gold.vw_landlord_company_type_book_monthly (landlord_company_type_book_schema.sql)
  4. ALTER the two t_landlord_* tables ADD sold_revenue_day_weighted (idempotent;
     ALTER rather than re-running landlord_dashboard_materialized.sql, which
     DROPs the tables)
  5. Rematerialize the three t_landlord_* tables (column lists mirror
     functions/landlord_materialize_dashboard.py, including the new column)
  6. Verification queries

Usage:
    python scripts/python_scripts/apply_dayweighted_revenue.py [--dry-run]
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


# (file, unique marker) in apply order. Order is not load-bearing here - none of
# the three views reference each other - but it matches the read order above.
BATCHES = [
    (SQL_DIR / "landlord_dashboard_schema.sql",
     "VIEW gold.vw_landlord_contract_book_monthly"),
    (SQL_DIR / "landlord_dashboard_revenue_schema.sql",
     "VIEW gold.vw_landlord_membership_book_monthly"),
    (SQL_DIR / "landlord_company_type_book_schema.sql",
     "VIEW gold.vw_landlord_company_type_book_monthly"),
]

# Idempotent so a re-run after a partial failure is safe. ALTER rather than
# re-running landlord_dashboard_materialized.sql, which DROPs these tables and
# would leave the dashboard empty until the next refresh.
ADD_COLUMNS = [
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('gold.t_landlord_contract_book_monthly')
          AND name = 'sold_revenue_day_weighted'
    )
    ALTER TABLE gold.t_landlord_contract_book_monthly
        ADD sold_revenue_day_weighted DECIMAL(18,2) NULL;
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID('gold.t_landlord_membership_book_monthly')
          AND name = 'sold_revenue_day_weighted'
    )
    ALTER TABLE gold.t_landlord_membership_book_monthly
        ADD sold_revenue_day_weighted DECIMAL(18,2) NULL;
    """,
]

# Mirrors functions/landlord_materialize_dashboard.py _REFRESHES exactly.
REFRESHES = [
    ("gold.vw_landlord_contract_book_monthly", "gold.t_landlord_contract_book_monthly", [
        "period", "month_start_date", "location_source_id", "location_name",
        "location_city", "location_country_name", "total_workstation_capacity",
        "active_contract_count", "occupied_workstations", "vacant_workstations",
        "occupancy_pct", "sold_monthly_revenue", "sold_revenue_day_weighted",
        "list_monthly_revenue",
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
        "occupancy_pct", "sold_monthly_revenue", "sold_revenue_day_weighted",
        "list_monthly_revenue",
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
        print("-- plus: ADD sold_revenue_day_weighted x2, rematerialize x3")
        print("-- additive only: no existing column changes value")
        return

    sql = get_sql_client()

    for name, marker, batch in resolved:
        print(f"applying [{name}] {marker} ...")
        sql.execute_non_query(batch)
    print("view batches applied.\n")

    for stmt in ADD_COLUMNS:
        sql.execute_non_query(stmt)
    print("materialized tables carry sold_revenue_day_weighted.\n")

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

    # -- Verification -------------------------------------------------------
    print("\n=== Aug-2026 contract book: month-end vs day-weighted revenue ===")
    for r in sql.execute_query("""
        SELECT location_name, sold_monthly_revenue, sold_revenue_day_weighted,
               occupancy_pct, occupancy_pct_eom
        FROM gold.vw_landlord_contract_book_monthly
        WHERE period = '2026-08' AND total_workstation_capacity > 0
        ORDER BY location_name
    """):
        eom = float(r["sold_monthly_revenue"] or 0)
        dw = float(r["sold_revenue_day_weighted"] or 0)
        print(f"  {str(r['location_name'])[:42]:<42} eom={eom:>12,.2f} dw={dw:>12,.2f} "
              f"delta={dw - eom:>+11,.2f}  occ={r['occupancy_pct']}/{r['occupancy_pct_eom']}")

    # These views are NOT expected to hold equal values - they run different
    # contract universes (the membership book applies the membership-fee account
    # filter; the type view uses a different future-signed test, see below). An
    # equality assertion here reports a dozen false alarms. What must hold is
    # that the new basis carries the SAME gap as the old one: anything else means
    # the day-weighted window disagrees with the month-end universe.
    print("\n=== contract vs membership book: gap must be identical on both bases ===")
    bad = sql.execute_query("""
        SELECT c.period, c.location_name,
               c.sold_monthly_revenue      - m.sold_monthly_revenue      AS gap_eom,
               c.sold_revenue_day_weighted - m.sold_revenue_day_weighted AS gap_dw
        FROM gold.vw_landlord_contract_book_monthly c
        JOIN gold.vw_landlord_membership_book_monthly m
          ON m.location_source_id = c.location_source_id AND m.period = c.period
        WHERE c.period BETWEEN '2026-08' AND '2026-10'
          AND ABS((c.sold_monthly_revenue      - m.sold_monthly_revenue)
                - (c.sold_revenue_day_weighted - m.sold_revenue_day_weighted)) > 1.00
    """)
    for r in bad:
        print(f"  GAP MOVED {r['period']} {r['location_name']}: "
              f"eom={r['gap_eom']} dw={r['gap_dw']}")
    print(f"  {len(bad)} rows where the gap moved (0 = the new basis tracks the old)")

    print("\n=== per-type view vs contract book: gap must be identical on both bases ===")
    bad = sql.execute_query("""
        SELECT t.period, c.location_name,
               SUM(t.total_monthly_fee)              - MAX(c.sold_monthly_revenue)      AS gap_eom,
               SUM(t.total_monthly_fee_day_weighted) - MAX(c.sold_revenue_day_weighted) AS gap_dw
        FROM gold.vw_landlord_company_type_book_monthly t
        JOIN gold.vw_landlord_contract_book_monthly c
          ON c.location_source_id = t.location_source_id AND c.period = t.period
        WHERE t.period BETWEEN '2026-08' AND '2026-10'
        GROUP BY t.period, c.location_name
        HAVING ABS((SUM(t.total_monthly_fee)              - MAX(c.sold_monthly_revenue))
                 - (SUM(t.total_monthly_fee_day_weighted) - MAX(c.sold_revenue_day_weighted))) > 1.00
    """)
    for r in bad:
        print(f"  GAP MOVED {r['period']} {r['location_name']}: "
              f"eom={r['gap_eom']} dw={r['gap_dw']}")
    print(f"  {len(bad)} rows where the gap moved (0 = the new basis tracks the old)")

    # PRE-EXISTING, not caused by this change: the type view tests future-signed
    # contracts with CAST(DATEADD(HOUR, 4, start_date) AS DATE) >= GETUTCDATE()
    # while both book views still use CAST(start_date AS DATE) > GETUTCDATE().
    # So they disagree on FUTURE months only (up to 11,420 at Fox Court, -6,300
    # at Heidestrasse) and agree on closed ones. Surfaced so it is not mistaken
    # for fallout from the day-weighting.
    print("\n=== pre-existing: type view vs book on FUTURE months (filter mismatch) ===")
    for r in sql.execute_query("""
        SELECT t.period, c.location_name,
               SUM(t.total_monthly_fee) - MAX(c.sold_monthly_revenue) AS gap
        FROM gold.vw_landlord_company_type_book_monthly t
        JOIN gold.vw_landlord_contract_book_monthly c
          ON c.location_source_id = t.location_source_id AND c.period = t.period
        WHERE t.period BETWEEN '2026-08' AND '2026-10'
        GROUP BY t.period, c.location_name
        HAVING ABS(SUM(t.total_monthly_fee) - MAX(c.sold_monthly_revenue)) > 1.00
        ORDER BY c.location_name, t.period
    """):
        print(f"  {r['period']} {str(r['location_name'])[:42]:<42} gap={float(r['gap']):>+12,.2f}")

    print("\n=== t_landlord_contract_book_monthly (what the dashboard reads) ===")
    for r in sql.execute_query("""
        SELECT period, location_name, sold_monthly_revenue, sold_revenue_day_weighted
        FROM gold.t_landlord_contract_book_monthly
        WHERE period = '2026-08' AND sold_revenue_day_weighted IS NOT NULL
          AND total_workstation_capacity > 0
        ORDER BY location_name
    """):
        print(f"  {str(r['location_name'])[:42]:<42} eom={float(r['sold_monthly_revenue'] or 0):>12,.2f} "
              f"dw={float(r['sold_revenue_day_weighted'] or 0):>12,.2f}")


if __name__ == "__main__":
    main()
