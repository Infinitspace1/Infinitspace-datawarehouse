"""
Scan every location for future-signed positive-fee contracts that are missing
floor_plan_desk_ids — the renewal-handover gap that's been silently dropping
revenue from the forecast (e.g. Allianz #1418600394 and RxSight #1418433597
at Hoofddorp Beyond, May 2026).

After the schema fix (vw_landlord_contract_book_monthly's capacity filter now
has an "unlinked-future-contract" branch), these contracts ARE counted in the
forecast revenue, but their occupancy contribution is 0 until ops links the
desks. This scan tells ops where to clean up.

Writes scan_unlinked_future_contracts.txt at the repo root.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client

OUT = ROOT / "scan_unlinked_future_contracts.txt"
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


out("UNLINKED-FUTURE-CONTRACT SCAN -- all locations")
out("Definition: contract has start_date > today, active=0, cancelled=0,")
out("            price > 0, and floor_plan_desk_ids is NULL or empty.")
out("These now flow into the forecast revenue (after the schema fix), but")
out("their occupancy contribution is 0 until ops links the desks.")


# ── 1. The contracts themselves ─────────────────────────────────────────────
section("1. Every unlinked future contract, location by location")
run("""
    SELECT
        loc.name                            AS location_name,
        c.source_id                         AS contract_id,
        c.coworker_company,
        c.coworker_name,
        c.tariff_name,
        CAST(c.start_date AS DATE)          AS start_date,
        CAST(c.contract_term AS DATE)       AS contract_term,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS monthly_fee,
        c.term_duration_months              AS term_months,
        c.cancellation_limit_days           AS notice_days,
        c.last_synced_at
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.is_deleted = 0
      AND c.active = 0
      AND c.cancelled = 0
      AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
      AND COALESCE(c.price_with_products, c.price, c.tariff_price, 0) > 0
      AND (c.floor_plan_desk_ids IS NULL OR c.floor_plan_desk_ids = '')
    ORDER BY loc.name, c.start_date, c.coworker_company
""")


# ── 2. Aggregate impact per location ────────────────────────────────────────
section("2. Per-location impact: how many contracts and how much monthly revenue")
out("This is the revenue that was silently missing from the forecast before")
out("the schema fix landed.")
run("""
    SELECT
        loc.name                                                      AS location_name,
        COUNT(*)                                                      AS unlinked_contracts,
        SUM(COALESCE(c.price_with_products, c.price, c.tariff_price, 0)) AS monthly_revenue_recovered,
        MIN(CAST(c.start_date AS DATE))                               AS earliest_start,
        MAX(CAST(c.start_date AS DATE))                               AS latest_start
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.is_deleted = 0
      AND c.active = 0
      AND c.cancelled = 0
      AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
      AND COALESCE(c.price_with_products, c.price, c.tariff_price, 0) > 0
      AND (c.floor_plan_desk_ids IS NULL OR c.floor_plan_desk_ids = '')
    GROUP BY loc.name
    ORDER BY monthly_revenue_recovered DESC
""")


# ── 3. Suggested matches: does the company have a CURRENT contract WITH desks ─
section("3. Renewal candidates: same company has an existing contract with desks linked")
out("If a previous contract by the same company has floor_plan_desk_ids, the")
out("new one is almost certainly a renewal — ops can copy the desk assignment.")
run("""
    WITH unlinked AS (
        SELECT
            c.source_id     AS contract_id,
            c.location_source_id,
            c.coworker_company,
            c.coworker_name,
            CAST(c.start_date AS DATE) AS start_date,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS monthly_fee
        FROM silver.nexudus_contracts c
        WHERE c.is_deleted = 0
          AND c.active = 0
          AND c.cancelled = 0
          AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
          AND COALESCE(c.price_with_products, c.price, c.tariff_price, 0) > 0
          AND (c.floor_plan_desk_ids IS NULL OR c.floor_plan_desk_ids = '')
    )
    SELECT
        loc.name                                          AS location_name,
        u.contract_id                                     AS unlinked_contract_id,
        u.coworker_company,
        u.start_date                                      AS unlinked_start_date,
        u.monthly_fee                                     AS unlinked_fee,
        -- Find the most recent CURRENT contract by the same company that HAS desks
        prev.source_id                                    AS predecessor_contract_id,
        CAST(prev.start_date AS DATE)                     AS predecessor_start,
        CAST(prev.cancellation_date AS DATE)              AS predecessor_cancellation,
        prev.floor_plan_desk_ids                          AS desks_to_copy,
        COALESCE(prev.price_with_products, prev.price, prev.tariff_price, 0) AS predecessor_fee
    FROM unlinked u
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = u.location_source_id
    OUTER APPLY (
        SELECT TOP 1 *
        FROM silver.nexudus_contracts p
        WHERE p.location_source_id = u.location_source_id
          AND COALESCE(NULLIF(p.coworker_company, ''), p.coworker_billing_name, p.coworker_name)
              = COALESCE(NULLIF(u.coworker_company, ''), u.coworker_name)
          AND p.is_deleted = 0
          AND p.floor_plan_desk_ids IS NOT NULL
          AND p.floor_plan_desk_ids <> ''
          AND CAST(p.start_date AS DATE) < u.start_date
        ORDER BY p.start_date DESC
    ) prev
    ORDER BY loc.name, u.start_date
""")


# ── 4. Sanity check: confirm Allianz + RxSight are in the list ──────────────
section("4. Confirm Allianz #1418600394 and RxSight #1418433597 are listed above")
run("""
    SELECT
        loc.name                                              AS location_name,
        c.source_id                                           AS contract_id,
        c.coworker_company,
        CAST(c.start_date AS DATE)                            AS start_date,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS monthly_fee
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.source_id IN (1418600394, 1418433597)
""")


OUT.write_text("\n".join(_buf), encoding="utf-8")
print(f"Wrote {OUT} ({OUT.stat().st_size:,} bytes, {len(_buf)} lines)")
