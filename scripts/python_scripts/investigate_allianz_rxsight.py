"""
Investigate Allianz + RxSight at Hoofddorp Beyond (Taurusavenue) for the
July 2026 forecast.

User report: ~€12,300 missing from the July 2026 prediction.
  - Allianz       — suspected future contract not appearing
  - RxSight       — suspected step-up (capacity expansion) not tracked

This script walks every layer (silver → gold) and prints, side-by-side,
whether each contract is included in:
  - gold.vw_landlord_current_contracts          (current-month dashboard KPIs)
  - gold.vw_landlord_contract_book_monthly     (12-month forecast chart)
  - gold.vw_landlord_monthly_contract_detail   (per-month drill-down, added 2026-05)
  - gold.vw_finance_dashboard_membership_schedule (the other dashboard's source)

It also runs the filter logic from the monthly view by hand so we can see WHICH
filter step drops a contract (status filter, capacity filter, active-in-month).

Output: allianz_rxsight_investigation.txt at the repo root.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client

OUT = ROOT / "allianz_rxsight_investigation.txt"
_buf = []

# Locations to scope queries. Hoofddorp Beyond is the brand at Taurusavenue.
# If your Nexudus name differs, edit LOCATION_NAME_FILTER.
LOCATION_NAME_FILTER = "%Hoofddorp%"   # matches "Amsterdam - Hoofddorp - Taurusavenue 3"
COMPANY_FILTERS = [
    ("Allianz", "%Allianz%"),
    ("RxSight", "%RxSight%"),
]
FORECAST_MONTH = "2026-07"


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


out("ALLIANZ + RXSIGHT @ HOOFDDORP BEYOND -- DATA AUDIT")
out(f"Target forecast month: {FORECAST_MONTH}")
out(f"Location filter:       LIKE '{LOCATION_NAME_FILTER}'")
out("Today (UTC):           see GETUTCDATE() in each query")


# ── 0. Resolve the location ─────────────────────────────────────────────────
section("0. Location lookup")
out("Confirm which Nexudus location_source_id we'll be querying.")
loc_rows = run("""
    SELECT
        source_id  AS location_source_id,
        name       AS location_name,
        city
    FROM silver.nexudus_locations
    WHERE name LIKE ?
      AND is_deleted = 0
    ORDER BY name
""", (LOCATION_NAME_FILTER,))

if not loc_rows:
    out("\nNo matching location — adjust LOCATION_NAME_FILTER at the top of this file.")
    OUT.write_text("\n".join(_buf), encoding="utf-8")
    print(f"Wrote {OUT}")
    sys.exit(1)

LOCATION_IDS = [r["location_source_id"] for r in loc_rows]
out(f"\nUsing location_source_id(s): {LOCATION_IDS}")


# ── For each company, run a thorough audit ──────────────────────────────────
for company_label, company_filter in COMPANY_FILTERS:
    section(f"########## COMPANY: {company_label}  (LIKE '{company_filter}') ##########")

    # 1. Every silver row for this company (active, cancelled, future, deleted)
    section(f"1. [{company_label}] silver.nexudus_contracts — every row")
    out("Includes future-signed (active=0, cancelled=0, start>today) and deleted rows.")
    run("""
        SELECT
            c.source_id                         AS contract_id,
            loc.name                            AS location_name,
            c.coworker_name,
            c.coworker_company,
            c.tariff_name,
            c.active, c.cancelled, c.in_paused_period, c.is_deleted,
            CAST(c.start_date AS DATE)          AS start_date,
            CAST(c.cancellation_date AS DATE)   AS cancellation_date,
            CAST(c.contract_term AS DATE)       AS contract_term,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS effective_price,
            c.floor_plan_desk_ids,
            c.cancellation_limit_days           AS notice_days,
            c.term_duration_months              AS term_months,
            c.last_synced_at
        FROM silver.nexudus_contracts c
        LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
        WHERE (c.coworker_company LIKE ? OR c.coworker_name LIKE ?)
          AND loc.name LIKE ?
        ORDER BY c.start_date, c.source_id
    """, (company_filter, company_filter, LOCATION_NAME_FILTER))

    # 2. Status of each contract against the contract_book filter
    section(f"2. [{company_label}] Filter trace — why each contract is/isn't in the forecast")
    out("Reproduces gold.vw_landlord_contract_book_monthly's WHERE clauses one by one.")
    out("Each row shows: which OR-branch matched (or none), capacity, and final inclusion.")
    run("""
        WITH product_link AS (
            SELECT
                c.source_id AS contract_source_id,
                SUM(
                    CASE
                        WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                        WHEN p.item_type IN (2, 3) THEN 1
                        ELSE 0
                    END
                ) AS capacity,
                STRING_AGG(CAST(p.item_type AS VARCHAR(2)), ',') AS product_item_types,
                STRING_AGG(p.name, ' | ') AS product_names
            FROM silver.nexudus_contracts c
            CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
            INNER JOIN silver.nexudus_products p
                ON  p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
                AND p.is_deleted = 0
            WHERE TRIM(s.value) <> N''
              AND c.is_deleted = 0
            GROUP BY c.source_id
        )
        SELECT
            c.source_id                                AS contract_id,
            CAST(c.start_date AS DATE)                 AS start_date,
            CAST(c.cancellation_date AS DATE)          AS cancellation_date,
            CAST(c.contract_term AS DATE)              AS contract_term,
            c.active,
            c.cancelled,
            c.is_deleted,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price,
            CASE WHEN c.floor_plan_desk_ids IS NULL OR c.floor_plan_desk_ids = '' THEN 'no' ELSE 'yes' END AS has_desk_ids,
            pl.capacity,
            pl.product_item_types,
            -- Which OR branch in the contract_book filter does this row match?
            CASE
                WHEN c.is_deleted = 1 THEN 'EXCLUDED: deleted'
                WHEN c.start_date IS NULL THEN 'EXCLUDED: no start_date'
                WHEN c.active = 1 THEN 'IN: active=1'
                WHEN c.cancelled = 1 AND c.cancellation_date IS NOT NULL THEN 'IN: cancelled+date'
                WHEN c.active = 0 AND c.cancelled = 0
                 AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE) THEN 'IN: future-signed'
                ELSE 'EXCLUDED: abandoned (active=0, cancelled=0, start<=today)'
            END AS status_filter_branch,
            -- Capacity-or-negative filter
            CASE
                WHEN pl.capacity > 0 THEN 'IN: has capacity'
                WHEN COALESCE(c.price_with_products, c.price, c.tariff_price, 0) < 0 THEN 'IN: negative-fee'
                ELSE 'EXCLUDED: no capacity & not negative'
            END AS capacity_filter_branch,
            -- Active in July 2026?
            CASE
                WHEN CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) > EOMONTH(DATEFROMPARTS(2026, 7, 1)) THEN 'no (starts after July)'
                WHEN c.cancellation_date IS NOT NULL
                 AND CAST(c.cancellation_date AS DATE) < EOMONTH(DATEFROMPARTS(2026, 7, 1)) THEN 'no (cancelled before EOM July)'
                ELSE 'yes'
            END AS active_in_2026_07
        FROM silver.nexudus_contracts c
        LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
        LEFT JOIN product_link pl ON pl.contract_source_id = c.source_id
        WHERE (c.coworker_company LIKE ? OR c.coworker_name LIKE ?)
          AND loc.name LIKE ?
        ORDER BY c.start_date, c.source_id
    """, (company_filter, company_filter, LOCATION_NAME_FILTER))

    # 3. What gold.vw_landlord_contract_book_monthly actually shows for July 2026
    section(f"3. [{company_label}] gold.vw_landlord_contract_book_monthly — July 2026 totals")
    out("If the missing contracts WERE included, occupied_workstations and sold_monthly_revenue would be higher.")
    out("This is the location-level row the forecast chart reads.")
    run("""
        SELECT
            location_name,
            period,
            active_contract_count,
            occupied_workstations,
            sold_monthly_revenue,
            new_workstations_starting,
            workstations_cancelling,
            net_workstation_change
        FROM gold.vw_landlord_contract_book_monthly
        WHERE location_name LIKE ?
          AND period BETWEEN '2026-05' AND '2026-10'
        ORDER BY period
    """, (LOCATION_NAME_FILTER,))

    # 4. Per-contract activity for each month around July 2026
    section(f"4. [{company_label}] Per-contract activity, May→Oct 2026")
    out("Reproduces active_by_month's join condition. is_active_in_month=1 means the")
    out("contract appears in that month's bar.")
    run("""
        WITH cf AS (
            SELECT
                c.source_id   AS contract_id,
                CAST(c.start_date AS DATE)        AS start_date,
                CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) AS effective_start_date,
                CAST(c.cancellation_date AS DATE) AS cancellation_date,
                COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price,
                c.active, c.cancelled, c.is_deleted,
                c.floor_plan_desk_ids
            FROM silver.nexudus_contracts c
            LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
            WHERE (c.coworker_company LIKE ? OR c.coworker_name LIKE ?)
              AND loc.name LIKE ?
              AND c.is_deleted = 0
        ),
        months AS (
            SELECT DATEADD(MONTH, n, DATEFROMPARTS(2026, 5, 1)) AS month_start
            FROM (VALUES (0),(1),(2),(3),(4),(5)) v(n)
        )
        SELECT
            FORMAT(m.month_start, 'yyyy-MM') AS period,
            cf.contract_id,
            cf.start_date,
            cf.effective_start_date,
            cf.cancellation_date,
            cf.price,
            cf.active, cf.cancelled,
            CASE WHEN cf.floor_plan_desk_ids IS NULL OR cf.floor_plan_desk_ids = '' THEN 'no' ELSE 'yes' END AS has_desks,
            CASE
                WHEN cf.effective_start_date <= EOMONTH(m.month_start)
                 AND (cf.cancellation_date IS NULL OR cf.cancellation_date >= EOMONTH(m.month_start))
                THEN 1 ELSE 0
            END AS is_active_in_month
        FROM months m
        CROSS JOIN cf
        ORDER BY period, cf.contract_id
    """, (company_filter, company_filter, LOCATION_NAME_FILTER))

    # 5. Side-by-side: presence in each gold view
    section(f"5. [{company_label}] Side-by-side: presence in every gold view")
    run("""
        WITH co AS (
            SELECT
                c.source_id  AS contract_id,
                c.active, c.cancelled, c.is_deleted,
                CAST(c.start_date AS DATE)        AS start_date,
                CAST(c.cancellation_date AS DATE) AS cancellation_date,
                COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS price
            FROM silver.nexudus_contracts c
            LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
            WHERE (c.coworker_company LIKE ? OR c.coworker_name LIKE ?)
              AND loc.name LIKE ?
        )
        SELECT
            co.contract_id,
            co.active, co.cancelled, co.is_deleted,
            co.start_date, co.cancellation_date, co.price,
            CASE WHEN lcc.contract_source_id IS NOT NULL THEN 'YES' ELSE '-' END AS in_landlord_current,
            CASE WHEN d.contract_source_id   IS NOT NULL THEN 'YES' ELSE '-' END AS in_monthly_detail_2026_07,
            CASE WHEN fms.contract_source_id IS NOT NULL THEN 'YES' ELSE '-' END AS in_finance_schedule
        FROM co
        LEFT JOIN gold.vw_landlord_current_contracts lcc
            ON lcc.contract_source_id = co.contract_id
        LEFT JOIN gold.vw_landlord_monthly_contract_detail d
            ON d.contract_source_id = co.contract_id
           AND d.period = '2026-07'
        LEFT JOIN gold.vw_finance_dashboard_membership_schedule fms
            ON fms.contract_source_id = co.contract_id
        ORDER BY co.start_date, co.contract_id
    """, (company_filter, company_filter, LOCATION_NAME_FILTER))


# ── Bonus: bronze freshness check ───────────────────────────────────────────
section("99. Bronze sync freshness")
out("If a brand-new future contract is missing from silver, the bronze sync may not")
out("have pulled it yet. Last successful Nexudus contract sync:")
run("""
    SELECT TOP 5
        entity, layer, status, started_at, finished_at, rows_written
    FROM meta.sync_runs
    WHERE source_name = 'nexudus'
      AND entity = 'contracts'
    ORDER BY finished_at DESC
""")


OUT.write_text("\n".join(_buf), encoding="utf-8")
print(f"Wrote {OUT} ({OUT.stat().st_size:,} bytes, {len(_buf)} lines)")
