"""
Drill into the finance-dashboard side: what view/SP computes contracted_monthly_revenue,
and how does it treat each Cainiao contract.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


OUT = ROOT / "cainiao_finance_investigation.txt"
_buf = []


def out(line=""):
    _buf.append(line)


def section(title):
    out("")
    out("=" * 100)
    out(title)
    out("=" * 100)


def run(sql, params=None, label=None):
    if label:
        out(f"\n--- {label} ---")
    rows = get_sql_client().execute_query(sql, params)
    if not rows:
        out("(no rows)")
        return []
    cols = list(rows[0].keys())
    widths = {c: max(len(c), max((len(str(r[c])) for r in rows), default=0)) for c in cols}
    out(" | ".join(c.ljust(widths[c]) for c in cols))
    out("-+-".join("-" * widths[c] for c in cols))
    for r in rows:
        out(" | ".join(str(r[c]).ljust(widths[c]) for c in cols))
    out(f"({len(rows)} rows)")
    return rows


out("CAINIAO -- FINANCE DASHBOARD DEEP-DIVE")


section("1. List the relevant gold finance objects and their types")
run("""
    SELECT name, type_desc
    FROM sys.objects
    WHERE SCHEMA_NAME(schema_id) = 'gold'
      AND (
        name LIKE '%finance%revenue%'
        OR name LIKE '%finance%membership%'
        OR name LIKE '%finance_dashboard%'
      )
    ORDER BY name
""")


section("2. Definition of gold.vw_finance_dashboard_membership_schedule")
run("""
    SELECT definition
    FROM sys.sql_modules
    WHERE object_id = OBJECT_ID('gold.vw_finance_dashboard_membership_schedule')
""")


section("3. Definition of gold.finance_dashboard_revenue_occupancy (table or view)")
run("""
    SELECT definition
    FROM sys.sql_modules
    WHERE object_id = OBJECT_ID('gold.finance_dashboard_revenue_occupancy')
""")


section("4. Find SPs that refresh finance_dashboard_revenue_occupancy")
run("""
    SELECT
        OBJECT_SCHEMA_NAME(object_id) AS schema_name,
        OBJECT_NAME(object_id)         AS sp_name
    FROM sys.sql_modules
    WHERE definition LIKE '%finance_dashboard_revenue_occupancy%'
      AND OBJECTPROPERTY(object_id, 'IsProcedure') = 1
""")


section("5. Cainiao contracts in vw_finance_dashboard_membership_schedule (full columns)")
run("""
    SELECT *
    FROM gold.vw_finance_dashboard_membership_schedule
    WHERE member_company_name LIKE '%Cainiao%'
""")


section("6. Sum the schedule fees per company at Taurusavenue (active+cap!=0 vs all)")
out("Two aggregations side-by-side to show what changes with the active+capacity filter.")
run("""
    SELECT
        'all_rows'                              AS filter_mode,
        COUNT(*)                                AS row_count,
        SUM(latest_monthly_fee)                 AS sum_fee,
        SUM(CASE WHEN latest_monthly_fee > 0 THEN latest_monthly_fee ELSE 0 END) AS sum_positive,
        SUM(CASE WHEN latest_monthly_fee < 0 THEN latest_monthly_fee ELSE 0 END) AS sum_negative
    FROM gold.vw_finance_dashboard_membership_schedule
    WHERE location_source_id = 1414964753
      AND member_company_name LIKE '%Cainiao%'
    UNION ALL
    SELECT
        'active_AND_capacity_nonzero',
        COUNT(*),
        SUM(latest_monthly_fee),
        SUM(CASE WHEN latest_monthly_fee > 0 THEN latest_monthly_fee ELSE 0 END),
        SUM(CASE WHEN latest_monthly_fee < 0 THEN latest_monthly_fee ELSE 0 END)
    FROM gold.vw_finance_dashboard_membership_schedule
    WHERE location_source_id = 1414964753
      AND member_company_name LIKE '%Cainiao%'
      AND active = 1
      AND ISNULL(capacity, 0) <> 0
""")


section("7. Cross-check: sum across whole Taurusavenue location (all and filtered)")
run("""
    SELECT
        'all'                                   AS filter_mode,
        COUNT(*)                                AS row_count,
        SUM(latest_monthly_fee)                 AS sum_fee
    FROM gold.vw_finance_dashboard_membership_schedule
    WHERE location_source_id = 1414964753
    UNION ALL
    SELECT
        'active=1',
        COUNT(*),
        SUM(latest_monthly_fee)
    FROM gold.vw_finance_dashboard_membership_schedule
    WHERE location_source_id = 1414964753
      AND active = 1
    UNION ALL
    SELECT
        'active=1 AND capacity!=0',
        COUNT(*),
        SUM(latest_monthly_fee)
    FROM gold.vw_finance_dashboard_membership_schedule
    WHERE location_source_id = 1414964753
      AND active = 1
      AND ISNULL(capacity, 0) <> 0
""")


OUT.write_text("\n".join(_buf), encoding="utf-8")
print(f"Wrote {OUT} ({OUT.stat().st_size:,} bytes, {len(_buf)} lines)")
