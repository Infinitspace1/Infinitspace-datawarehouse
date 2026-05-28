"""
Confirm that the 6 unlinked future contracts identified by
scan_unlinked_future_contracts.py really do have NULL / empty
floor_plan_desk_ids in silver.nexudus_contracts.

Side-by-side with each contract's predecessor (when one exists), so it's
visible at a glance: predecessor has desks → renewal does not.

Writes verify_unlinked_desk_ids.txt at the repo root.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client

OUT = ROOT / "verify_unlinked_desk_ids.txt"
_buf = []

# The 6 contract IDs from scan_unlinked_future_contracts.py
UNLINKED_IDS = [
    1418600394,  # Allianz @ Hoofddorp
    1418433597,  # RxSight @ Hoofddorp
    1418651881,  # GemZ @ Noord
    1418651575,  # Synagen @ Berlin
    1418531725,  # Plantsalt @ Berlin
    1418607797,  # ebblo @ Berlin
]


def out(line=""):
    _buf.append(line)


def section(title):
    out("")
    out("=" * 100)
    out(title)
    out("=" * 100)


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


out("VERIFY UNLINKED DESK IDS")
out("Showing the raw floor_plan_desk_ids column for each contract.")
out("NULL or empty string == no desks linked == invisible to the forecast")
out("until the schema fix lands (or until ops links them in Nexudus).")


# ── 1. The unlinked contracts themselves ────────────────────────────────────
section("1. Unlinked contracts — raw floor_plan_desk_ids column")
out("Look at the floor_plan_desk_ids column. Anything that is None or empty")
out("means no desk is assigned in Nexudus.")
placeholders = ",".join(["?"] * len(UNLINKED_IDS))
run(f"""
    SELECT
        c.source_id                         AS contract_id,
        loc.name                            AS location,
        c.coworker_company,
        c.tariff_name,
        c.active,
        c.cancelled,
        CAST(c.start_date AS DATE)          AS start_date,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS monthly_fee,
        -- The smoking gun: this is the field that links to products
        CASE
            WHEN c.floor_plan_desk_ids IS NULL THEN '<<NULL>>'
            WHEN c.floor_plan_desk_ids = '' THEN '<<empty>>'
            ELSE c.floor_plan_desk_ids
        END                                 AS floor_plan_desk_ids,
        CASE
            WHEN c.floor_plan_desk_names IS NULL THEN '<<NULL>>'
            WHEN c.floor_plan_desk_names = '' THEN '<<empty>>'
            ELSE c.floor_plan_desk_names
        END                                 AS floor_plan_desk_names,
        c.last_synced_at
    FROM silver.nexudus_contracts c
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
    WHERE c.source_id IN ({placeholders})
    ORDER BY loc.name, c.start_date
""", tuple(UNLINKED_IDS))


# ── 2. Predecessors (where they exist) for comparison ───────────────────────
section("2. The predecessor contracts — for comparison, these DO have desks")
out("Same companies, same locations, but the EXISTING active contract.")
out("Look how the floor_plan_desk_ids column is populated on these.")
run(f"""
    WITH unlinked AS (
        SELECT
            c.source_id          AS unlinked_id,
            c.location_source_id,
            COALESCE(NULLIF(c.coworker_company, ''), c.coworker_billing_name, c.coworker_name) AS company_key,
            CAST(c.start_date AS DATE) AS unlinked_start
        FROM silver.nexudus_contracts c
        WHERE c.source_id IN ({placeholders})
    )
    SELECT
        loc.name                            AS location,
        u.unlinked_id                       AS unlinked_contract_id,
        u.company_key                       AS company,
        p.source_id                         AS predecessor_id,
        p.tariff_name                       AS predecessor_tariff,
        CAST(p.start_date AS DATE)          AS predecessor_start,
        CAST(p.cancellation_date AS DATE)   AS predecessor_cancellation,
        COALESCE(p.price_with_products, p.price, p.tariff_price, 0) AS predecessor_fee,
        CASE
            WHEN p.floor_plan_desk_ids IS NULL THEN '<<NULL>>'
            WHEN p.floor_plan_desk_ids = ''   THEN '<<empty>>'
            ELSE p.floor_plan_desk_ids
        END                                 AS predecessor_floor_plan_desk_ids,
        CASE
            WHEN p.floor_plan_desk_names IS NULL THEN '<<NULL>>'
            WHEN p.floor_plan_desk_names = ''   THEN '<<empty>>'
            ELSE p.floor_plan_desk_names
        END                                 AS predecessor_floor_plan_desk_names
    FROM unlinked u
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = u.location_source_id
    OUTER APPLY (
        SELECT TOP 1 *
        FROM silver.nexudus_contracts p
        WHERE p.location_source_id = u.location_source_id
          AND COALESCE(NULLIF(p.coworker_company, ''), p.coworker_billing_name, p.coworker_name)
              = u.company_key
          AND p.source_id <> u.unlinked_id
          AND p.is_deleted = 0
          AND p.floor_plan_desk_ids IS NOT NULL
          AND p.floor_plan_desk_ids <> ''
          AND CAST(p.start_date AS DATE) < u.unlinked_start
        ORDER BY p.start_date DESC
    ) p
    ORDER BY loc.name, u.unlinked_start
""", tuple(UNLINKED_IDS))


# ── 3. For each contract that DOES have a predecessor, resolve the desk products ──
section("3. What the predecessor's desks resolve to (so ops knows what to copy)")
out("Joins predecessor floor_plan_desk_ids to silver.nexudus_products to show")
out("the actual product names + capacities that ops should re-assign.")
run(f"""
    WITH unlinked AS (
        SELECT
            c.source_id          AS unlinked_id,
            c.location_source_id,
            COALESCE(NULLIF(c.coworker_company, ''), c.coworker_billing_name, c.coworker_name) AS company_key,
            CAST(c.start_date AS DATE) AS unlinked_start
        FROM silver.nexudus_contracts c
        WHERE c.source_id IN ({placeholders})
    ),
    predecessor AS (
        SELECT
            u.unlinked_id,
            p.source_id   AS predecessor_id,
            p.floor_plan_desk_ids
        FROM unlinked u
        OUTER APPLY (
            SELECT TOP 1 *
            FROM silver.nexudus_contracts p
            WHERE p.location_source_id = u.location_source_id
              AND COALESCE(NULLIF(p.coworker_company, ''), p.coworker_billing_name, p.coworker_name)
                  = u.company_key
              AND p.source_id <> u.unlinked_id
              AND p.is_deleted = 0
              AND p.floor_plan_desk_ids IS NOT NULL
              AND p.floor_plan_desk_ids <> ''
              AND CAST(p.start_date AS DATE) < u.unlinked_start
            ORDER BY p.start_date DESC
        ) p
    )
    SELECT
        u.unlinked_id,
        u.predecessor_id,
        TRIM(s.value)                       AS desk_product_id,
        prod.name                           AS product_name,
        prod.item_type,
        CASE prod.item_type
            WHEN 1 THEN 'Private Office'
            WHEN 2 THEN 'Dedicated Desk'
            WHEN 3 THEN 'Hot Desk'
            ELSE 'Other'
        END                                 AS product_type,
        prod.capacity                       AS desks_in_product,
        prod.price                          AS product_list_price
    FROM predecessor u
    CROSS APPLY STRING_SPLIT(ISNULL(u.floor_plan_desk_ids, ''), ',') s
    LEFT JOIN silver.nexudus_products prod
        ON prod.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
       AND prod.is_deleted = 0
    WHERE TRIM(s.value) <> ''
      AND u.floor_plan_desk_ids IS NOT NULL
    ORDER BY u.unlinked_id, prod.name
""", tuple(UNLINKED_IDS))


OUT.write_text("\n".join(_buf), encoding="utf-8")
print(f"Wrote {OUT} ({OUT.stat().st_size:,} bytes, {len(_buf)} lines)")
