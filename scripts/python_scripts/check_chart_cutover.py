"""
Sanity-check: does vw_landlord_contract_book_monthly double-count contracts
when a future contract starts on the same day the current one ends?

Example: Allianz current ends 2026-06-30; Allianz future starts 2026-06-30.
After the >= fix, both might be flagged as active in June.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


_buf = []


def out(line=""):
    _buf.append(line)
    print(line)


def run(sql, params=None):
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


print("\n=== Allianz: per-contract active-in-month flag (Apr-Aug 2026) ===")
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
        cf.cancellation_date,
        cf.price,
        cf.active,
        CASE
            WHEN cf.start_date <= EOMONTH(m.month_start)
             AND (cf.cancellation_date IS NULL OR cf.cancellation_date >= EOMONTH(m.month_start))
            THEN 1 ELSE 0
        END AS is_active_in_month_now,
        CASE
            WHEN cf.start_date <= EOMONTH(m.month_start)
             AND (cf.cancellation_date IS NULL OR cf.cancellation_date >  EOMONTH(m.month_start))
            THEN 1 ELSE 0
        END AS is_active_in_month_pre_fix
    FROM months m
    CROSS JOIN cf
    ORDER BY period, cf.contract_id
""")


print("\n=== Same-day cutovers: how many contracts have cancellation = follow-up's start_date? ===")
out("If a row count > 0 → double-counting is happening in the forecast for these months.")
run("""
    SELECT
        loc.name AS location,
        COALESCE(NULLIF(a.coworker_company,''), a.coworker_billing_name) AS company,
        a.source_id    AS ending_contract,
        a.cancellation_date,
        COALESCE(a.price_with_products, a.price, a.tariff_price, 0) AS ending_fee,
        b.source_id    AS replacement_contract,
        b.start_date   AS replacement_start,
        COALESCE(b.price_with_products, b.price, b.tariff_price, 0) AS replacement_fee
    FROM silver.nexudus_contracts a
    INNER JOIN silver.nexudus_locations loc ON loc.source_id = a.location_source_id
    INNER JOIN silver.nexudus_contracts b
        ON  b.location_source_id = a.location_source_id
        AND COALESCE(NULLIF(b.coworker_company,''), b.coworker_billing_name, b.coworker_name)
          = COALESCE(NULLIF(a.coworker_company,''), a.coworker_billing_name, a.coworker_name)
        AND CAST(b.start_date AS DATE) = CAST(a.cancellation_date AS DATE)
        AND b.is_deleted = 0
        AND COALESCE(b.price_with_products, b.price, b.tariff_price, 0) > 0
        AND b.source_id <> a.source_id
    WHERE a.is_deleted = 0
      AND a.cancellation_date IS NOT NULL
      AND COALESCE(a.price_with_products, a.price, a.tariff_price, 0) > 0
      AND a.cancellation_date > GETUTCDATE()
    ORDER BY a.cancellation_date, company
""")


print("\n=== Forecast totals for Taurusavenue (current state of the chart) ===")
run("""
    SELECT
        period,
        active_contract_count AS pos_n,
        occupied_workstations  AS desks,
        sold_monthly_revenue   AS sold_rev,
        adjustment_monthly_value AS adj
    FROM gold.vw_landlord_contract_book_monthly
    WHERE location_source_id = 1414964753
      AND period BETWEEN '2026-04' AND '2026-09'
    ORDER BY period
""")
