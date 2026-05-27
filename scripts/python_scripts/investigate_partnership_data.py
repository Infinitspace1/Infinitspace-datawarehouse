"""
Investigation script for strategic partnership dashboard data quality.

Three checks:
  1. Negative-fee contracts and how they roll up into pricing summary
  2. Allianz Direct Versicherungs-AG multi-contract handling
  3. Better Home Care Services Limited multi-contract handling
"""
import os
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
    client = get_sql_client()
    rows = client.execute_query(sql, params)
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


# =============================================================================
# 1. NEGATIVE-FEE CONTRACTS
# =============================================================================
section("1. NEGATIVE-FEE CONTRACTS IN silver.nexudus_contracts")

run_query("""
    SELECT TOP 50
        c.source_id          AS contract_source_id,
        loc.name             AS location_name,
        c.coworker_name,
        c.coworker_company,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS effective_price,
        c.price, c.price_with_products, c.tariff_price,
        c.floor_plan_desk_ids,
        c.active, c.cancelled,
        CAST(c.start_date AS DATE)        AS start_date,
        CAST(c.cancellation_date AS DATE) AS cancellation_date
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.is_deleted = 0
      AND COALESCE(c.price_with_products, c.price, c.tariff_price, 0) < 0
    ORDER BY effective_price ASC
""", label="All negative-fee contracts (lowest first)")


section("2. NEGATIVE CONTRACTS — are they in vw_landlord_current_contracts?")

run_query("""
    SELECT
        location_name,
        contract_source_id,
        member_company_name,
        capacity,
        sold_monthly_fee,
        list_monthly_fee,
        is_negative_adjustment,
        status,
        start_date,
        cancellation_date
    FROM gold.vw_landlord_current_contracts
    WHERE is_negative_adjustment = 1
    ORDER BY sold_monthly_fee ASC
""", label="Negative-adjustment rows in current_contracts view")


section("3. PRICING SUMMARY — net revenue per location with adjustments visible")

run_query("""
    SELECT
        location_name,
        sold_monthly_revenue,
        active_contract_count,
        adjustment_contract_count,
        adjustment_monthly_value
    FROM gold.vw_landlord_pricing_summary
    WHERE adjustment_contract_count > 0
    ORDER BY adjustment_monthly_value ASC
""", label="Locations with negative-fee adjustments — net revenue impact")


# =============================================================================
# 2. ALLIANZ DIRECT VERSICHERUNGS-AG
# =============================================================================
section("4. ALLIANZ DIRECT VERSICHERUNGS-AG — all contracts in silver")

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
        c.coworker_company LIKE '%Allianz%'
     OR c.coworker_name    LIKE '%Allianz%'
     OR c.coworker_billing_name LIKE '%Allianz%'
    ORDER BY c.start_date
""", label="silver.nexudus_contracts rows mentioning Allianz")


run_query("""
    SELECT
        location_name,
        member_company_name,
        contract_source_id,
        status,
        capacity,
        sold_monthly_fee,
        start_date,
        cancellation_date
    FROM gold.vw_landlord_current_contracts
    WHERE member_company_name LIKE '%Allianz%'
       OR coworker_name       LIKE '%Allianz%'
    ORDER BY start_date
""", label="Allianz in vw_landlord_current_contracts (current month only)")


run_query("""
    SELECT
        location_name,
        member_company_name,
        capacity,
        sold_monthly_fee,
        status,
        start_date,
        cancellation_date,
        contract_end_date
    FROM gold.vw_landlord_current_companies
    WHERE member_company_name LIKE '%Allianz%'
""", label="Allianz in vw_landlord_current_companies (1 row per company)")


# Also check what appears in the monthly book for Allianz's location
# (Taurusavenue 3 = 1414964753 — derived from the silver query above)
run_query("""
    SELECT
        period,
        location_name,
        active_contract_count,
        adjustment_contract_count,
        occupied_workstations,
        sold_monthly_revenue,
        adjustment_monthly_value,
        new_workstations_starting,
        workstations_cancelling
    FROM gold.vw_landlord_contract_book_monthly
    WHERE location_source_id IN (
        SELECT DISTINCT location_source_id
        FROM silver.nexudus_contracts
        WHERE coworker_company LIKE '%Allianz%'
    )
      AND period BETWEEN
            FORMAT(DATEADD(MONTH, -3, GETUTCDATE()), 'yyyy-MM')
        AND FORMAT(DATEADD(MONTH,  6, GETUTCDATE()), 'yyyy-MM')
    ORDER BY location_name, period
""", label="Allianz location(s) — contract book by month")


# Check what appears in the monthly book that's specifically attributable to Allianz contracts
run_query("""
    WITH allianz_facts AS (
        SELECT
            c.source_id              AS contract_source_id,
            CAST(c.start_date AS DATE)        AS start_date,
            CAST(c.cancellation_date AS DATE) AS cancellation_date,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS sold_monthly_fee,
            c.active, c.cancelled
        FROM silver.nexudus_contracts c
        WHERE c.is_deleted = 0
          AND c.coworker_company LIKE '%Allianz%'
    ),
    months AS (
        SELECT TOP 12
            DATEADD(MONTH, n, DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)) AS month_start
        FROM (
            VALUES (-3),(-2),(-1),(0),(1),(2),(3),(4),(5),(6),(7),(8)
        ) v(n)
    )
    SELECT
        FORMAT(m.month_start, 'yyyy-MM')   AS period,
        af.contract_source_id,
        af.start_date,
        af.cancellation_date,
        af.sold_monthly_fee,
        CASE
            WHEN af.start_date <= EOMONTH(m.month_start)
             AND (af.cancellation_date IS NULL OR af.cancellation_date > EOMONTH(m.month_start))
            THEN 1 ELSE 0
        END AS is_active_this_month
    FROM months m
    CROSS JOIN allianz_facts af
    ORDER BY period, contract_source_id
""", label="Allianz contracts — month-by-month activity matrix")


# =============================================================================
# 3. BETTER HOME CARE SERVICES LIMITED
# =============================================================================
section("5. BETTER HOME CARE SERVICES LIMITED — all contracts in silver")

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
""", label="silver.nexudus_contracts rows mentioning Better Home Care")


run_query("""
    SELECT
        location_name,
        member_company_name,
        contract_source_id,
        status,
        capacity,
        sold_monthly_fee,
        start_date,
        cancellation_date
    FROM gold.vw_landlord_current_contracts
    WHERE member_company_name LIKE '%Better%home%care%'
       OR coworker_name       LIKE '%Better%home%care%'
    ORDER BY start_date
""", label="Better Home Care in vw_landlord_current_contracts")


run_query("""
    SELECT
        location_name,
        member_company_name,
        capacity,
        sold_monthly_fee,
        status,
        start_date,
        cancellation_date
    FROM gold.vw_landlord_current_companies
    WHERE member_company_name LIKE '%Better%home%care%'
""", label="Better Home Care in vw_landlord_current_companies")


# =============================================================================
# 4. SANITY CHECKS: Future-signed contracts and how they appear
# =============================================================================
section("6. FUTURE-SIGNED CONTRACTS — how many and what they look like")

run_query("""
    SELECT
        c.source_id           AS contract_source_id,
        loc.name              AS location_name,
        c.coworker_name,
        c.coworker_company,
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
""", label="All future-signed contracts (start_date > today)")


print("\nDone.")
