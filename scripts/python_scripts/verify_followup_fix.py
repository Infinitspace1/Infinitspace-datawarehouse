"""
Verify the follow-up + cutover fix:
  - 4 companies from the screenshot now expose follow-up info correctly
  - Taurusavenue forecast no longer double-counts cutover months
  - Cainiao (prior fix) still works
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


def run(label, sql):
    print(f"\n=== {label} ===")
    rows = get_sql_client().execute_query(sql)
    if not rows:
        print("(no rows)"); return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), max((len(str(r[c])) for r in rows), default=0)) for c in cols}
    print(" | ".join(c.ljust(widths[c]) for c in cols))
    print("-+-".join("-" * widths[c] for c in cols))
    for r in rows:
        print(" | ".join(str(r[c]).ljust(widths[c]) for c in cols))


run("4 SCREENSHOT COMPANIES -- follow-up surfacing", """
    SELECT
        member_company_name,
        cancellation_date,
        has_followup_contract     AS has_fu,
        followup_contract_count   AS fu_n,
        followup_total_monthly_fee AS fu_fee,
        latest_followup_end_date,
        effective_end_date
    FROM gold.vw_landlord_current_companies
    WHERE member_company_name IN (
        'ADP Nederland B.V.',
        'Allianz Direct Versicherungs-AG',
        'A2Z-CM NV',
        'RxSight BV'
    )
    ORDER BY member_company_name
""")

run("Taurusavenue forecast -- cutover months should no longer double-count", """
    SELECT
        period,
        active_contract_count   AS pos_n,
        occupied_workstations    AS desks,
        sold_monthly_revenue     AS net_rev,
        adjustment_monthly_value AS adj
    FROM gold.vw_landlord_contract_book_monthly
    WHERE location_source_id = 1414964753
      AND period BETWEEN '2026-04' AND '2026-10'
    ORDER BY period
""")

run("Allianz per-contract in-month (should be 1 row each month, not 2)", """
    WITH cf AS (
        SELECT
            c.source_id                                   AS contract_id,
            CAST(c.start_date AS DATE)                    AS start_date,
            CAST(DATEADD(HOUR, 4, c.start_date) AS DATE)  AS effective_start_date,
            CAST(c.cancellation_date AS DATE)             AS cancellation_date,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price
        FROM silver.nexudus_contracts c
        WHERE c.is_deleted = 0
          AND c.coworker_company LIKE '%Allianz%'
    ),
    months AS (
        SELECT DATEADD(MONTH, n, DATEFROMPARTS(2026, 4, 1)) AS month_start
        FROM (VALUES (0),(1),(2),(3),(4)) v(n)
    )
    SELECT
        FORMAT(m.month_start, 'yyyy-MM') AS period,
        cf.contract_id,
        cf.start_date,
        cf.effective_start_date,
        cf.cancellation_date,
        cf.price,
        CASE
            WHEN cf.effective_start_date <= EOMONTH(m.month_start)
             AND (cf.cancellation_date IS NULL OR cf.cancellation_date >= EOMONTH(m.month_start))
            THEN 'YES' ELSE 'no'
        END AS active_in_month
    FROM months m
    CROSS JOIN cf
    WHERE cf.price > 0  -- skip discount lines for clarity
    ORDER BY period, cf.contract_id
""")

run("Cainiao -- prior fix still working (should show €0 net in May)", """
    SELECT
        contract_source_id, sold_monthly_fee, capacity,
        is_negative_adjustment, status, cancellation_date
    FROM gold.vw_landlord_current_contracts
    WHERE member_company_name LIKE '%Cainiao%'
    ORDER BY sold_monthly_fee DESC
""")

run("Per-location pricing summary -- watching for regression", """
    SELECT
        location_name,
        sold_monthly_revenue AS net_rev,
        adjustment_contract_count AS adj_n,
        adjustment_monthly_value  AS adj_val,
        active_contract_count     AS pos_n
    FROM gold.vw_landlord_pricing_summary
    ORDER BY location_name
""")

run("32-list -- companies with follow-ups (should show effective_end_date when known)", """
    SELECT TOP 10
        location_name,
        member_company_name,
        cancellation_date,
        has_followup_contract,
        followup_contract_count,
        latest_followup_end_date,
        effective_end_date
    FROM gold.vw_landlord_current_companies
    WHERE has_followup_contract = 1
    ORDER BY cancellation_date, member_company_name
""")
