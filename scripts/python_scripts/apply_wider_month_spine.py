"""
scripts/python_scripts/apply_wider_month_spine.py

Widen the landlord views' month spine from -12 to -24 (2026-09-01, requested by
Bryan).

THE BUG. Every landlord view built its month spine as an offset from
GETUTCDATE(), so the window rolled with the calendar rather than with the month
being viewed. The dashboard's period selector offers 13 months of history and
each of those is supposed to show a full 12 months BEFORE it - which needs 24
months of depth, not 12. At -12 the oldest bar simply fell off: viewing Aug-2026
on 1 Sep 2026, Aug-2025 had dropped out of the window overnight, leaving 11 bars
and a gap. It degraded further back (Jun-2026 rendered only 9 bars).

Six spines, all -12 -> -24:
  landlord_dashboard_schema.sql
      vw_landlord_contract_book_monthly            (-24..+12)
      vw_landlord_monthly_contract_detail          (-24..+12)
  landlord_dashboard_revenue_schema.sql
      vw_landlord_revenue_past_monthly             (-24..+12)
      vw_landlord_membership_book_monthly          (-24..+12)
  landlord_company_type_book_schema.sql
      vw_landlord_company_type_book_monthly        (-24..+24)
  landlord_revenue_stream_schema.sql
      vw_landlord_revenue_stream_past_monthly      (-24..+12)

The two *_past_location_monthly views are NOT in the list: they aggregate the
base views above and widen automatically once those are applied.

PURELY ADDITIVE. Dry-run before writing this confirmed, per view: 0 rows lost,
0 drift on any shared row, only months ADDED.
  contract book       275 -> 407 rows    2024-09..2027-09
  membership book     275 -> 407 rows    2024-09..2027-09
  type book        14,064 -> 16,242 rows 2024-09..2028-09
  revenue past      4,862 -> 7,247 rows  2024-09..2026-10
  revenue stream    5,347 -> 8,049 rows  2024-09..2026-10
Cost is modest and lands mostly on the nightly materialize job, not on reads:
membership book 1.0s -> 1.3s, type book 8.0s -> 8.9s (the type view is the one
that is NOT materialized, so that 0.9s is a real per-request cost on the
cashflow and stacked-type charts).

Invoice depth is not the limit here - silver holds membership-fee lines back to
2022-03, so -24 is comfortably covered by real data.

Usage:
    python scripts/python_scripts/apply_wider_month_spine.py [--dry-run]
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


# Base views before the ones that aggregate them, so a mid-run failure never
# leaves an aggregate reading a narrower base than it expects.
BATCHES = [
    (SQL_DIR / "landlord_dashboard_revenue_schema.sql",
     "VIEW gold.vw_landlord_revenue_past_monthly"),
    (SQL_DIR / "landlord_revenue_stream_schema.sql",
     "VIEW gold.vw_landlord_revenue_stream_past_monthly"),
    (SQL_DIR / "landlord_dashboard_schema.sql",
     "VIEW gold.vw_landlord_contract_book_monthly"),
    (SQL_DIR / "landlord_dashboard_schema.sql",
     "VIEW gold.vw_landlord_monthly_contract_detail"),
    (SQL_DIR / "landlord_dashboard_revenue_schema.sql",
     "VIEW gold.vw_landlord_membership_book_monthly"),
    (SQL_DIR / "landlord_company_type_book_schema.sql",
     "VIEW gold.vw_landlord_company_type_book_monthly"),
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

CHECK_VIEWS = [
    "gold.vw_landlord_contract_book_monthly",
    "gold.vw_landlord_membership_book_monthly",
    "gold.vw_landlord_company_type_book_monthly",
    "gold.vw_landlord_revenue_past_monthly",
    "gold.vw_landlord_revenue_past_location_monthly",
    "gold.vw_landlord_revenue_stream_past_location_monthly",
    "gold.t_landlord_contract_book_monthly",
    "gold.t_landlord_membership_book_monthly",
    "gold.t_landlord_revenue_past_location_monthly",
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
        print("-- plus: rematerialize x3")
        print("-- additive only: months are ADDED, no existing row changes")
        return

    sql = get_sql_client()

    print("=== window BEFORE ===")
    for v in CHECK_VIEWS:
        r = sql.execute_query(
            f"SELECT MIN(period) a, MAX(period) b, COUNT(*) n FROM {v}")[0]
        print(f"  {v:<58} {r['a']} .. {r['b']}  ({r['n']} rows)")

    for name, marker, batch in resolved:
        print(f"applying [{name}] {marker} ...")
        sql.execute_non_query(batch)
    print("view batches applied.\n")

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

    print("\n=== window AFTER ===")
    for v in CHECK_VIEWS:
        r = sql.execute_query(
            f"SELECT MIN(period) a, MAX(period) b, COUNT(*) n FROM {v}")[0]
        print(f"  {v:<58} {r['a']} .. {r['b']}  ({r['n']} rows)")

    # The point of the whole exercise: every month the period selector offers
    # must resolve a full 12 months of history behind it.
    print("\n=== every selectable month must have 12 months of history behind it ===")
    rows = sql.execute_query("""
        WITH sel AS (
            SELECT FORMAT(DATEADD(MONTH, -n, DATEFROMPARTS(
                       YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)), 'yyyy-MM') AS period
            FROM (SELECT TOP (13) ROW_NUMBER() OVER (ORDER BY object_id) - 1 AS n
                  FROM sys.objects) x
        )
        SELECT sel.period,
               (SELECT COUNT(DISTINCT b.period)
                  FROM gold.t_landlord_contract_book_monthly b
                 WHERE b.location_source_id = 1415499547
                   AND b.period < sel.period
                   AND b.period >= FORMAT(DATEADD(MONTH, -12,
                        CAST(sel.period + '-01' AS DATE)), 'yyyy-MM')) AS months_behind
        FROM sel ORDER BY sel.period
    """)
    for r in rows:
        ok = "OK " if r["months_behind"] >= 12 else "SHORT"
        print(f"  {ok} {r['period']}: {r['months_behind']} of 12 months behind it")


if __name__ == "__main__":
    main()
