"""
Consolidated readable report for strategic partnership data audit.

Writes to: partnership_data_findings.txt at the repo root.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


OUT = ROOT / "partnership_data_findings.txt"
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


# ============================================================================
out("PARTNERSHIP DASHBOARD DATA AUDIT")
out(f"Generated against live DB at {ROOT}")
out("Current month: 2026-05 (May)")

section("1. NEGATIVE-FEE IMPACT PER LOCATION (current month)")
out("These rows show how much negative-fee 'discount/credit' contracts net out of revenue.")
out("`sold_monthly_revenue` already includes the netted negatives. Gross = sold - adjustment.")
run("""
    SELECT
        location_name,
        sold_monthly_revenue                            AS net_revenue,
        adjustment_contract_count                       AS neg_count,
        adjustment_monthly_value                        AS neg_total,
        CAST(sold_monthly_revenue - adjustment_monthly_value AS DECIMAL(18,2)) AS gross_revenue,
        active_contract_count                           AS positive_count
    FROM gold.vw_landlord_pricing_summary
    WHERE adjustment_contract_count > 0
    ORDER BY adjustment_monthly_value ASC
""")

section("2. TOP 30 NEGATIVE-FEE CONTRACTS BY IMPACT")
run("""
    SELECT TOP 30
        location_name,
        member_company_name,
        contract_source_id,
        sold_monthly_fee,
        start_date,
        cancellation_date,
        status
    FROM gold.vw_landlord_current_contracts
    WHERE is_negative_adjustment = 1
    ORDER BY sold_monthly_fee ASC
""")

# ============================================================================
section("3. ALLIANZ DIRECT VERSICHERUNGS-AG -- silver contracts (all)")
run("""
    SELECT
        c.source_id              AS contract_id,
        c.active, c.cancelled,
        CAST(c.start_date AS DATE)         AS start_date,
        CAST(c.cancellation_date AS DATE)  AS cancellation_date,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price,
        c.floor_plan_desk_ids
    FROM silver.nexudus_contracts c
    WHERE c.is_deleted = 0
      AND c.coworker_company LIKE '%Allianz%'
    ORDER BY c.start_date
""")

section("3a. ALLIANZ -- what the dashboard 'Membership Schedule' currently shows")
out("(comes from gold.vw_landlord_current_companies - one row per company)")
run("""
    SELECT
        location_name, member_company_name, capacity, sold_monthly_fee,
        status, start_date, cancellation_date
    FROM gold.vw_landlord_current_companies
    WHERE member_company_name LIKE '%Allianz%'
""")

section("3b. ALLIANZ -- what the dashboard 'Forecast' currently shows")
out("(comes from gold.vw_landlord_contract_book_monthly for Taurusavenue 3 location)")
run("""
    SELECT
        period,
        active_contract_count                                                                 AS pos_contracts,
        adjustment_contract_count                                                             AS neg_contracts,
        occupied_workstations                                                                 AS desks_occ,
        sold_monthly_revenue                                                                  AS net_rev,
        adjustment_monthly_value                                                              AS adj_value,
        new_workstations_starting                                                             AS new_desks,
        workstations_cancelling                                                               AS lost_desks
    FROM gold.vw_landlord_contract_book_monthly
    WHERE location_source_id = 1414964753
      AND period BETWEEN '2026-04' AND '2026-12'
    ORDER BY period
""")

# ============================================================================
section("4. BETTER HOME CARE SERVICES LIMITED -- silver contracts (all 15)")
run("""
    SELECT
        c.source_id              AS contract_id,
        c.active, c.cancelled,
        CAST(c.start_date AS DATE)         AS start_date,
        CAST(c.cancellation_date AS DATE)  AS cancellation,
        CAST(c.contract_term AS DATE)      AS contract_end,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price,
        c.floor_plan_desk_ids,
        CASE
            WHEN c.cancellation_date IS NOT NULL
             AND CAST(c.cancellation_date AS DATE) < CAST(c.start_date AS DATE)
            THEN 'BAD: cancel < start'
            ELSE ''
        END AS data_quality
    FROM silver.nexudus_contracts c
    WHERE c.is_deleted = 0
      AND c.coworker_company LIKE '%Better%home%care%'
    ORDER BY c.start_date
""")

section("4a. BETTER HOME CARE -- what dashboard 'Membership Schedule' shows (1 row)")
run("""
    SELECT
        location_name, member_company_name, capacity, sold_monthly_fee,
        status, start_date, cancellation_date
    FROM gold.vw_landlord_current_companies
    WHERE member_company_name LIKE '%Better%home%care%'
""")

section("4b. BETTER HOME CARE -- their actual forward revenue trajectory")
out("Computed directly from silver -- shows what the dashboard *should* be telegraphing.")
run("""
    WITH months AS (
        SELECT DATEADD(MONTH, n, DATEFROMPARTS(2026, 5, 1)) AS month_start
        FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)) v(n)
    ),
    bh AS (
        SELECT
            c.source_id,
            CAST(c.start_date AS DATE)        AS start_date,
            CAST(c.cancellation_date AS DATE) AS cancellation,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price
        FROM silver.nexudus_contracts c
        WHERE c.is_deleted = 0
          AND c.coworker_company LIKE '%Better%home%care%'
          -- exclude rows where cancellation precedes start (data errors)
          AND NOT (
              c.cancellation_date IS NOT NULL
              AND CAST(c.cancellation_date AS DATE) < CAST(c.start_date AS DATE)
          )
    )
    SELECT
        FORMAT(m.month_start, 'yyyy-MM') AS period,
        COUNT(bh.source_id)              AS active_contracts,
        ISNULL(SUM(bh.price), 0)         AS sum_monthly_revenue
    FROM months m
    LEFT JOIN bh
        ON  bh.start_date <= EOMONTH(m.month_start)
        AND (bh.cancellation IS NULL OR bh.cancellation > EOMONTH(m.month_start))
    GROUP BY m.month_start
    ORDER BY m.month_start
""")

# ============================================================================
section("5. DATA QUALITY: silver contracts where cancellation_date < start_date")
out("These cannot exist physically -- Nexudus garbage rows that should be cleaned up.")
run("""
    SELECT
        c.source_id,
        loc.name AS location_name,
        c.coworker_company,
        CAST(c.start_date AS DATE)         AS start_date,
        CAST(c.cancellation_date AS DATE)  AS cancellation_date,
        DATEDIFF(DAY, CAST(c.start_date AS DATE), CAST(c.cancellation_date AS DATE)) AS days_gap
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.is_deleted = 0
      AND c.cancellation_date IS NOT NULL
      AND c.start_date IS NOT NULL
      AND CAST(c.cancellation_date AS DATE) < CAST(c.start_date AS DATE)
    ORDER BY days_gap
""")

# ============================================================================
section("6. COMPANIES WITH FUTURE-SIGNED CONTRACTS THAT ARE INVISIBLE IN SCHEDULE")
out("Companies with positive-price (>=1000) future contracts AND already have an")
out("active contract today. Schedule shows only current; future is hidden.")
run("""
    WITH future_meaningful AS (
        SELECT
            c.location_source_id,
            COALESCE(NULLIF(c.coworker_company, ''), c.coworker_billing_name, c.coworker_name) AS company,
            CAST(c.start_date AS DATE)        AS start_date,
            CAST(c.cancellation_date AS DATE) AS cancellation,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price
        FROM silver.nexudus_contracts c
        WHERE c.is_deleted = 0
          AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
          AND COALESCE(c.price_with_products, c.price, c.tariff_price, 0) >= 1000
    )
    SELECT
        cc.location_name,
        fm.company,
        cc.sold_monthly_fee AS current_monthly_fee,
        cc.cancellation_date AS current_cancellation,
        fm.start_date       AS future_start,
        fm.cancellation     AS future_cancellation,
        fm.price            AS future_monthly_fee
    FROM future_meaningful fm
    INNER JOIN gold.vw_landlord_current_companies cc
        ON cc.location_source_id   = fm.location_source_id
       AND cc.member_company_name  = fm.company
    ORDER BY cc.location_name, fm.company, fm.start_date
""")


# write
OUT.write_text("\n".join(_buf), encoding="utf-8")
print(f"Wrote {OUT} ({OUT.stat().st_size:,} bytes, {len(_buf)} lines)")
