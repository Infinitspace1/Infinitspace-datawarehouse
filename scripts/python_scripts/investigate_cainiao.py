"""
Investigate Cainiao (Netherlands) B.V. (Alibaba):
  - what's in silver.nexudus_contracts
  - what each gold view exposes for revenue / occupancy
  - what the *other* (finance) dashboard sees
  - month-by-month activity for every Cainiao contract

This isolates *why* the strategic-partnership dashboard counts Cainiao in revenue
while the other application does not.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


OUT = ROOT / "cainiao_investigation.txt"
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


out("CAINIAO (NETHERLANDS) B.V. (ALIBABA) -- DATA AUDIT")
out("Current month: 2026-05")


# ----------------------------------------------------------------------------
section("1. silver.nexudus_contracts -- every Cainiao row (active or not)")
run("""
    SELECT
        c.source_id                         AS contract_id,
        loc.name                            AS location_name,
        c.coworker_id, c.coworker_name,
        c.coworker_company,
        c.active, c.cancelled, c.in_paused_period,
        CAST(c.start_date AS DATE)          AS start_date,
        CAST(c.cancellation_date AS DATE)   AS cancellation_date,
        CAST(c.contract_term AS DATE)       AS contract_end,
        c.price, c.price_with_products, c.tariff_price,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS effective_price,
        c.floor_plan_desk_ids,
        c.term_duration_months              AS term_months,
        c.cancellation_limit_days           AS notice_days,
        c.is_deleted
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.coworker_company LIKE '%Cainiao%'
       OR c.coworker_name LIKE '%Cainiao%'
    ORDER BY c.start_date
""")


# ----------------------------------------------------------------------------
section("2. gold.vw_landlord_current_contracts -- Cainiao rows that pass the filter")
out("Filter is: active=1 OR (cancelled=1 AND cancellation_date > end-of-month).")
out("AND (capacity > 0 OR effective_price < 0).")
run("""
    SELECT
        location_name,
        member_company_name,
        contract_source_id,
        capacity,
        sold_monthly_fee,
        list_monthly_fee,
        is_negative_adjustment,
        status,
        start_date,
        cancellation_date,
        contract_end_date
    FROM gold.vw_landlord_current_contracts
    WHERE member_company_name LIKE '%Cainiao%'
       OR coworker_name LIKE '%Cainiao%'
    ORDER BY start_date
""")


# ----------------------------------------------------------------------------
section("3. gold.vw_landlord_current_companies -- the schedule row (1 row per company)")
run("""
    SELECT
        location_name,
        member_company_name,
        capacity,
        sold_monthly_fee,
        list_monthly_fee,
        status,
        start_date,
        cancellation_date
    FROM gold.vw_landlord_current_companies
    WHERE member_company_name LIKE '%Cainiao%'
""")


# ----------------------------------------------------------------------------
section("4. gold.vw_landlord_contract_book_monthly -- per-contract activity at Taurusavenue")
out("Shows, for every Cainiao contract and every month, whether it's counted in the book.")
run("""
    WITH cf AS (
        SELECT
            c.source_id                              AS contract_id,
            CAST(c.start_date AS DATE)               AS start_date,
            CAST(c.cancellation_date AS DATE)        AS cancellation_date,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price,
            c.active, c.cancelled, c.floor_plan_desk_ids
        FROM silver.nexudus_contracts c
        WHERE c.is_deleted = 0
          AND c.coworker_company LIKE '%Cainiao%'
    ),
    months AS (
        SELECT DATEADD(MONTH, n, DATEFROMPARTS(2026, 4, 1)) AS month_start
        FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7)) v(n)
    )
    SELECT
        FORMAT(m.month_start, 'yyyy-MM') AS period,
        cf.contract_id,
        cf.start_date,
        cf.cancellation_date,
        cf.price,
        cf.active, cf.cancelled,
        cf.floor_plan_desk_ids,
        CASE
            WHEN cf.start_date <= EOMONTH(m.month_start)
             AND (cf.cancellation_date IS NULL OR cf.cancellation_date > EOMONTH(m.month_start))
            THEN 1 ELSE 0
        END AS is_active_in_month,
        CASE
            WHEN cf.price < 0 THEN 'negative-adjustment'
            ELSE 'positive'
        END AS contract_type
    FROM months m
    CROSS JOIN cf
    ORDER BY period, cf.contract_id
""")


# ----------------------------------------------------------------------------
section("5. gold.vw_landlord_contract_book_monthly -- LOCATION TOTALS (Taurusavenue)")
out("Net revenue per month at Taurusavenue, which already includes Cainiao adjustments.")
run("""
    SELECT
        period,
        active_contract_count, adjustment_contract_count,
        occupied_workstations, sold_monthly_revenue,
        adjustment_monthly_value
    FROM gold.vw_landlord_contract_book_monthly
    WHERE location_source_id = 1414964753
      AND period BETWEEN '2026-04' AND '2026-11'
    ORDER BY period
""")


# ----------------------------------------------------------------------------
section("6. OTHER DASHBOARD: gold.vw_finance_dashboard_membership_schedule")
out("This is the membership schedule the FINANCE dashboard reads.")
out("Filter (from sql_client.py _sql_schedule): active=1 AND capacity != 0.")
run("""
    SELECT
        s.contract_source_id,
        s.location_source_id,
        s.coworker_name,
        s.member_company_name,
        s.tariff_name,
        s.capacity,
        s.active,
        s.currency_code,
        s.latest_monthly_fee,
        s.latest_monthly_fee_per_workstation,
        s.start_date,
        s.term_months,
        s.notice_period_months
    FROM gold.vw_finance_dashboard_membership_schedule s
    WHERE s.member_company_name LIKE '%Cainiao%'
       OR s.coworker_name LIKE '%Cainiao%'
    ORDER BY s.start_date
""")


# ----------------------------------------------------------------------------
section("7. OTHER DASHBOARD: gold.finance_dashboard_revenue_occupancy (Taurusavenue)")
out("Snapshot the other dashboard reads. Shows contracted_monthly_revenue, etc.")
run("""
    SELECT TOP 5
        as_of_date_utc, location_name,
        active_contract_count, active_member_count,
        occupied_workstations, total_workstation_capacity, occupancy_pct,
        contracted_monthly_revenue,
        monthly_revenue_per_occupied_workstation
    FROM gold.finance_dashboard_revenue_occupancy
    WHERE location_source_id = 1414964753
    ORDER BY as_of_date_utc DESC
""")


# ----------------------------------------------------------------------------
section("8. SIDE-BY-SIDE: what each view says about each Cainiao contract")
out("Joins silver to every gold view to show inclusion / exclusion in one table.")
run("""
    WITH cainiao AS (
        SELECT
            c.source_id      AS contract_id,
            c.active, c.cancelled,
            CAST(c.start_date AS DATE)        AS start_date,
            CAST(c.cancellation_date AS DATE) AS cancellation_date,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price,
            c.floor_plan_desk_ids
        FROM silver.nexudus_contracts c
        WHERE c.is_deleted = 0
          AND c.coworker_company LIKE '%Cainiao%'
    )
    SELECT
        ca.contract_id,
        ca.active, ca.cancelled,
        ca.start_date, ca.cancellation_date, ca.price,
        CASE WHEN ca.floor_plan_desk_ids IS NULL THEN 'no' ELSE 'yes' END AS has_desk_ids,
        CASE WHEN lcc.contract_source_id IS NOT NULL THEN 'YES' ELSE '-' END AS in_landlord_current,
        CASE WHEN fms.contract_source_id IS NOT NULL THEN 'YES' ELSE '-' END AS in_finance_schedule,
        fms.latest_monthly_fee   AS finance_monthly_fee,
        lcc.sold_monthly_fee     AS landlord_monthly_fee
    FROM cainiao ca
    LEFT JOIN gold.vw_landlord_current_contracts lcc
        ON lcc.contract_source_id = ca.contract_id
    LEFT JOIN gold.vw_finance_dashboard_membership_schedule fms
        ON fms.contract_source_id = ca.contract_id
    ORDER BY ca.start_date
""")


OUT.write_text("\n".join(_buf), encoding="utf-8")
print(f"Wrote {OUT} ({OUT.stat().st_size:,} bytes, {len(_buf)} lines)")
