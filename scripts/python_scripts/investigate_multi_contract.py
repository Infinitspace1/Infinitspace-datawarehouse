"""
Investigate multi-contract aggregation and gap distribution.

Q1: Gap pattern distribution -- is the 7-day threshold realistic?
Q2: Multiple concurrent contracts per member -- what does the view show?
    Does it include or exclude ancillary (parking, meeting room, storage,
    business address) contracts? Are discounts netted correctly?
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


def show(label, sql):
    print(f"\n=== {label} ===")
    rows = get_sql_client().execute_query(sql)
    if not rows:
        print("(none)"); return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), max((len(str(r[c])) for r in rows), default=0)) for c in cols}
    print(" | ".join(c.ljust(widths[c]) for c in cols))
    print("-+-".join("-" * widths[c] for c in cols))
    for r in rows:
        print(" | ".join(str(r[c]).ljust(widths[c]) for c in cols))


# ── Q1: gap distribution ─────────────────────────────────────────────────────
show("Q1A: full gap distribution across all follow-ups (bucketed)", """
    WITH gaps AS (
        SELECT
            DATEDIFF(DAY, b.cancellation_date, CAST(c.start_date AS DATE)) AS gap_days
        FROM gold.vw_landlord_current_companies b
        INNER JOIN silver.nexudus_contracts c
            ON  c.location_source_id = b.location_source_id
            AND COALESCE(NULLIF(c.coworker_company, ''), c.coworker_billing_name, c.coworker_name)
              = b.member_company_name
            AND c.is_deleted = 0
            AND COALESCE(c.price_with_products, c.price, c.tariff_price, 0) > 0
            AND CAST(c.start_date AS DATE) >= b.cancellation_date
            AND (
                c.active = 1
                OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
                OR (
                    c.active = 0
                    AND c.cancelled = 0
                    AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
                )
            )
        WHERE b.cancellation_date IS NOT NULL
    )
    SELECT
        CASE
            WHEN gap_days = 0 THEN '0 (same day)'
            WHEN gap_days BETWEEN 1 AND 7   THEN '1-7'
            WHEN gap_days BETWEEN 8 AND 31  THEN '8-31'
            WHEN gap_days BETWEEN 32 AND 90 THEN '32-90'
            WHEN gap_days BETWEEN 91 AND 180 THEN '91-180'
            ELSE '> 180'
        END AS gap_bucket,
        COUNT(*) AS follow_up_count
    FROM gaps
    GROUP BY
        CASE
            WHEN gap_days = 0 THEN '0 (same day)'
            WHEN gap_days BETWEEN 1 AND 7   THEN '1-7'
            WHEN gap_days BETWEEN 8 AND 31  THEN '8-31'
            WHEN gap_days BETWEEN 32 AND 90 THEN '32-90'
            WHEN gap_days BETWEEN 91 AND 180 THEN '91-180'
            ELSE '> 180'
        END
    ORDER BY MIN(gap_days)
""")


# ── Q2: multi-contract examples ──────────────────────────────────────────────
show("Q2A: companies with the most contracts at the same location", """
    SELECT TOP 10
        loc.name                                              AS location,
        COALESCE(NULLIF(c.coworker_company, ''), c.coworker_billing_name, c.coworker_name)
                                                              AS company,
        COUNT(*)                                              AS contract_count,
        SUM(CASE WHEN COALESCE(c.price_with_products, c.price, c.tariff_price, 0) > 0
                  AND c.floor_plan_desk_ids IS NOT NULL
                 THEN 1 ELSE 0 END)                           AS desk_contracts,
        SUM(CASE WHEN COALESCE(c.price_with_products, c.price, c.tariff_price, 0) > 0
                  AND c.floor_plan_desk_ids IS NULL
                 THEN 1 ELSE 0 END)                           AS ancillary_pos_contracts,
        SUM(CASE WHEN COALESCE(c.price_with_products, c.price, c.tariff_price, 0) < 0
                 THEN 1 ELSE 0 END)                           AS discount_contracts
    FROM silver.nexudus_contracts c
    INNER JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.is_deleted = 0
      AND (
          c.active = 1
          OR (c.active = 0 AND c.cancelled = 0
              AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE))
      )
    GROUP BY loc.name,
             COALESCE(NULLIF(c.coworker_company, ''), c.coworker_billing_name, c.coworker_name)
    ORDER BY contract_count DESC
""")


show("Q2B: Cainiao all contracts -- positive ancillary lines silently excluded?", """
    SELECT
        c.source_id     AS contract_id,
        c.tariff_name,
        c.active, c.cancelled,
        CAST(c.start_date AS DATE)         AS start_date,
        CAST(c.cancellation_date AS DATE)  AS cancellation_date,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS monthly_fee,
        CASE WHEN c.floor_plan_desk_ids IS NOT NULL THEN 'desks' ELSE 'no-desks' END
                                          AS desk_link,
        CASE WHEN EXISTS (
            SELECT 1 FROM gold.vw_landlord_current_contracts lcc
            WHERE lcc.contract_source_id = c.source_id
        ) THEN 'YES' ELSE 'NO' END        AS in_current_contracts_view
    FROM silver.nexudus_contracts c
    WHERE c.is_deleted = 0
      AND c.coworker_company LIKE '%Cainiao%'
    ORDER BY c.start_date, monthly_fee DESC
""")


show("Q2C: per-company aggregation in vw_landlord_current_companies for Cainiao", """
    SELECT
        capacity, sold_monthly_fee, list_monthly_fee,
        has_open_ended_current_contract, has_followup_contract, effective_end_date
    FROM gold.vw_landlord_current_companies
    WHERE member_company_name LIKE '%Cainiao%'
""")


show("Q2D: how much positive ancillary revenue is being hidden across the dataset?", """
    SELECT
        COUNT(*)                                                  AS hidden_ancillary_rows,
        SUM(COALESCE(c.price_with_products, c.price, c.tariff_price, 0)) AS hidden_revenue_per_month
    FROM silver.nexudus_contracts c
    WHERE c.is_deleted = 0
      AND c.active = 1
      AND c.floor_plan_desk_ids IS NULL
      AND COALESCE(c.price_with_products, c.price, c.tariff_price, 0) > 0
""")


show("Q2E: ancillary contract types missing from current_contracts", """
    SELECT
        c.tariff_name,
        COUNT(*) AS contract_count,
        SUM(COALESCE(c.price_with_products, c.price, c.tariff_price, 0)) AS monthly_revenue
    FROM silver.nexudus_contracts c
    WHERE c.is_deleted = 0
      AND c.active = 1
      AND c.floor_plan_desk_ids IS NULL
      AND COALESCE(c.price_with_products, c.price, c.tariff_price, 0) > 0
    GROUP BY c.tariff_name
    ORDER BY monthly_revenue DESC
""")
