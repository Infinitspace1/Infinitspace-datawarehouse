"""Part 2: Allianz monthly book, Better Home Care, and future-contract overview."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


def section(title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def run_query(sql, params=None, label=None):
    if label:
        print(f"\n--- {label} ---")
    rows = get_sql_client().execute_query(sql, params)
    if not rows:
        print("(no rows)")
        return []
    cols = list(rows[0].keys())
    widths = {c: max(len(c), max((len(str(r[c])) for r in rows), default=0)) for c in cols}
    header = " | ".join(c.ljust(widths[c]) for c in cols)
    print(header)
    print("-" * len(header))
    for r in rows:
        print(" | ".join(str(r[c]).ljust(widths[c]) for c in cols))
    print(f"({len(rows)} rows)")
    return rows


# Allianz's location ID is 1414964753 (Taurusavenue 3) per the prior output
ALLIANZ_LOC_ID = 1414964753

section("ALLIANZ — contract book by month for Taurusavenue 3 location")
run_query("""
    SELECT
        period, location_name,
        active_contract_count,
        adjustment_contract_count,
        occupied_workstations,
        sold_monthly_revenue,
        adjustment_monthly_value,
        new_workstations_starting,
        workstations_cancelling
    FROM gold.vw_landlord_contract_book_monthly
    WHERE location_source_id = ?
      AND period BETWEEN
            FORMAT(DATEADD(MONTH, -2, GETUTCDATE()), 'yyyy-MM')
        AND FORMAT(DATEADD(MONTH,  8, GETUTCDATE()), 'yyyy-MM')
    ORDER BY period
""", (ALLIANZ_LOC_ID,))


section("ALLIANZ — month-by-month activity for each of its 3 contracts")
run_query("""
    WITH af AS (
        SELECT
            c.source_id              AS contract_source_id,
            CAST(c.start_date AS DATE)        AS start_date,
            CAST(c.cancellation_date AS DATE) AS cancellation_date,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS sold_monthly_fee,
            c.active, c.cancelled,
            c.floor_plan_desk_ids
        FROM silver.nexudus_contracts c
        WHERE c.is_deleted = 0
          AND c.coworker_company LIKE '%Allianz%'
    ),
    months AS (
        SELECT DATEADD(MONTH, n, DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)) AS month_start
        FROM (VALUES (-3),(-2),(-1),(0),(1),(2),(3),(4),(5),(6),(7),(8)) v(n)
    )
    SELECT
        FORMAT(m.month_start, 'yyyy-MM')   AS period,
        af.contract_source_id,
        af.start_date,
        af.cancellation_date,
        af.sold_monthly_fee,
        af.active, af.cancelled,
        CASE
            WHEN af.start_date <= EOMONTH(m.month_start)
             AND (af.cancellation_date IS NULL OR af.cancellation_date > EOMONTH(m.month_start))
            THEN 1 ELSE 0
        END AS is_active_this_month,
        af.floor_plan_desk_ids
    FROM months m
    CROSS JOIN af
    ORDER BY period, contract_source_id
""")


section("BETTER HOME CARE SERVICES — silver contracts")
run_query("""
    SELECT
        c.source_id              AS contract_source_id,
        loc.name                 AS location_name,
        c.coworker_name,
        c.coworker_company,
        c.coworker_billing_name,
        c.active, c.cancelled, c.in_paused_period,
        CAST(c.start_date AS DATE)         AS start_date,
        CAST(c.cancellation_date AS DATE)  AS cancellation_date,
        CAST(c.contract_term AS DATE)      AS contract_end_date,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS effective_price,
        c.floor_plan_desk_ids,
        c.is_deleted
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE
        c.coworker_company LIKE '%Better%home%care%'
     OR c.coworker_name    LIKE '%Better%home%care%'
     OR c.coworker_billing_name LIKE '%Better%home%care%'
    ORDER BY c.start_date
""")


section("BETTER HOME CARE — vw_landlord_current_contracts")
run_query("""
    SELECT
        location_name, member_company_name, contract_source_id,
        status, capacity, sold_monthly_fee,
        start_date, cancellation_date
    FROM gold.vw_landlord_current_contracts
    WHERE member_company_name LIKE '%Better%home%care%'
       OR coworker_name       LIKE '%Better%home%care%'
""")


section("BETTER HOME CARE — vw_landlord_current_companies")
run_query("""
    SELECT
        location_name, member_company_name, capacity, sold_monthly_fee,
        status, start_date, cancellation_date, contract_end_date
    FROM gold.vw_landlord_current_companies
    WHERE member_company_name LIKE '%Better%home%care%'
""")


section("FUTURE-SIGNED CONTRACTS — start_date > today")
run_query("""
    SELECT
        c.source_id, loc.name AS location_name, c.coworker_company,
        c.active, c.cancelled,
        CAST(c.start_date AS DATE)        AS start_date,
        CAST(c.cancellation_date AS DATE) AS cancellation_date,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS effective_price
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.is_deleted = 0
      AND c.start_date IS NOT NULL
      AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
    ORDER BY c.start_date
""")


section("FUTURE-SIGNED CONTRACTS — how many also appear in current_companies (should be 0)")
run_query("""
    WITH future_contracts AS (
        SELECT
            c.source_id,
            COALESCE(NULLIF(c.coworker_company, ''), c.coworker_billing_name, c.coworker_name) AS company_name,
            c.location_source_id
        FROM silver.nexudus_contracts c
        WHERE c.is_deleted = 0
          AND c.start_date IS NOT NULL
          AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
    )
    SELECT
        fc.company_name,
        cc.member_company_name AS in_current_companies,
        m.period AS earliest_forecast_period
    FROM future_contracts fc
    LEFT JOIN gold.vw_landlord_current_companies cc
        ON cc.location_source_id = fc.location_source_id
       AND cc.member_company_name = fc.company_name
    OUTER APPLY (
        SELECT MIN(period) AS period
        FROM gold.vw_landlord_contract_book_monthly mb
        WHERE mb.location_source_id = fc.location_source_id
          AND mb.active_contract_count > 0
    ) m
    ORDER BY fc.company_name
""")


print("\nDone.")
