"""Confirm two suspected design bugs in shared/location_scraper/activities/persist.py.

Issue 1 — contacts are only linked on the new-building path: update-path listing
snapshots (first_time_extract IS NULL) should show exactly 0 contact links, and
some buildings should have no contact at all despite scraper emails in raw.

Issue 2 — Decimal (SQL) vs float (Python) price comparison: unchanged buildings
should still accumulate listing snapshots whose price/status are identical to
the previous snapshot ("phantom updates").
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.azure_clients.sql_client import get_db

db = get_db()


def show(title: str, rows: list) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("(no rows)")
        return
    for r in rows:
        print(dict(r) if not isinstance(r, dict) else r)


# --- Issue 2: phantom update snapshots -------------------------------------
show(
    "Issue 2 — update snapshots identical to the previous snapshot (phantoms)",
    db.fetch_all(
        """
        WITH s AS (
            SELECT
                building_id, run_id, price_monthly, price_per_m2, status,
                first_time_extract, last_seen_date,
                ROW_NUMBER() OVER (PARTITION BY building_id ORDER BY last_seen_date, run_id) AS rn,
                LAG(price_monthly) OVER (PARTITION BY building_id ORDER BY last_seen_date, run_id) AS prev_price,
                LAG(price_per_m2)  OVER (PARTITION BY building_id ORDER BY last_seen_date, run_id) AS prev_ppm2,
                LAG(status)        OVER (PARTITION BY building_id ORDER BY last_seen_date, run_id) AS prev_status
            FROM bronze.n8n_location_scraper_listings
        )
        SELECT
            COUNT(*) AS snapshots_after_first,
            SUM(CASE WHEN (price_monthly = prev_price OR (price_monthly IS NULL AND prev_price IS NULL))
                      AND (price_per_m2  = prev_ppm2  OR (price_per_m2  IS NULL AND prev_ppm2  IS NULL))
                      AND (status        = prev_status OR (status       IS NULL AND prev_status IS NULL))
                THEN 1 ELSE 0 END) AS phantom_snapshots
        FROM s
        WHERE rn > 1
        """
    ),
)

show(
    "Issue 2 — phantoms by run (top 10 runs)",
    db.fetch_all(
        """
        WITH s AS (
            SELECT
                building_id, run_id, price_monthly, price_per_m2, status, last_seen_date,
                ROW_NUMBER() OVER (PARTITION BY building_id ORDER BY last_seen_date, run_id) AS rn,
                LAG(price_monthly) OVER (PARTITION BY building_id ORDER BY last_seen_date, run_id) AS prev_price,
                LAG(price_per_m2)  OVER (PARTITION BY building_id ORDER BY last_seen_date, run_id) AS prev_ppm2,
                LAG(status)        OVER (PARTITION BY building_id ORDER BY last_seen_date, run_id) AS prev_status
            FROM bronze.n8n_location_scraper_listings
        )
        SELECT TOP 10 run_id,
            COUNT(*) AS update_snapshots,
            SUM(CASE WHEN (price_monthly = prev_price OR (price_monthly IS NULL AND prev_price IS NULL))
                      AND (price_per_m2  = prev_ppm2  OR (price_per_m2  IS NULL AND prev_ppm2  IS NULL))
                      AND (status        = prev_status OR (status       IS NULL AND prev_status IS NULL))
                THEN 1 ELSE 0 END) AS phantom_snapshots
        FROM s
        WHERE rn > 1
        GROUP BY run_id
        ORDER BY COUNT(*) DESC
        """
    ),
)

# --- Issue 1: contacts only linked on the new-building path -----------------
show(
    "Issue 1 — contact links on first vs update snapshots (update path should be 0)",
    db.fetch_all(
        """
        SELECT
            CASE WHEN l.first_time_extract IS NULL THEN 'update_snapshot' ELSE 'first_snapshot' END AS snapshot_kind,
            COUNT(DISTINCT l.id) AS listings,
            COUNT(DISTINCT lc.listing_id) AS listings_with_contact_link
        FROM bronze.n8n_location_scraper_listings l
        LEFT JOIN bronze.n8n_location_scraper_listing_contacts lc ON lc.listing_id = l.id
        GROUP BY CASE WHEN l.first_time_extract IS NULL THEN 'update_snapshot' ELSE 'first_snapshot' END
        """
    ),
)

show(
    "Issue 1 — buildings without any contact, by city/source",
    db.fetch_all(
        """
        SELECT b.city, b.source,
            COUNT(DISTINCT b.id) AS buildings,
            COUNT(DISTINCT CASE WHEN lc.contact_id IS NOT NULL THEN b.id END) AS buildings_with_contact
        FROM bronze.n8n_location_scraper_buildings b
        LEFT JOIN bronze.n8n_location_scraper_listings l ON l.building_id = b.id
        LEFT JOIN bronze.n8n_location_scraper_listing_contacts lc ON lc.listing_id = l.id
        GROUP BY b.city, b.source
        ORDER BY b.city, b.source
        """
    ),
)

show(
    "Issue 1 — London/LoopNet: latest run raw brokerEmail coverage",
    db.fetch_all(
        """
        SELECT TOP 3 run_id,
            COUNT(*) AS raw_items,
            SUM(CASE WHEN JSON_VALUE(payload_json, '$.brokerEmail') IS NOT NULL
                      AND JSON_VALUE(payload_json, '$.brokerEmail') <> '' THEN 1 ELSE 0 END) AS with_broker_email
        FROM bronze.location_scraper_raw
        WHERE city = 'london'
        GROUP BY run_id
        ORDER BY MAX(inserted_at) DESC
        """
    ),
)
