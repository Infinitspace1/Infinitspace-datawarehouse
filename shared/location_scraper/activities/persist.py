"""
Activity: upsert_sql

Replaces the n8n nodes:
  - SQL Read existing buildings
  - Split into Insert / Update / Log   (logic ported to Python)
  - Sanitize for SQL                   (dropped — parameterized queries handle escaping)
  - Insert building in Microsoft SQL   (MERGE with OUTPUT)
  - Insert listing                     (INSERT with OUTPUT)
  - Insert contact                     (MERGE per contact source)
  - Update building in Microsoft SQL   (INSERT listing for price/status changes)

Schema (tables assumed to exist in bronze schema):
  bronze.n8n_location_scraper_buildings
  bronze.n8n_location_scraper_listings
  bronze.n8n_location_scraper_contacts
  bronze.n8n_location_scraper_listing_contacts
  bronze.n8n_location_scraper_logs
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Optional

from shared.azure_clients.sql_client import get_sql_client
from shared.location_scraper.models import ContactBundle, Listing, RunStats

logger = logging.getLogger(__name__)

_MERGE_BUILDING = """
MERGE bronze.n8n_location_scraper_buildings WITH (HOLDLOCK) AS target
USING (
    SELECT
        CAST(? AS NUMERIC(9,6)) AS latitude,
        CAST(? AS NUMERIC(9,6)) AS longitude,
        CAST(? AS SMALLINT)     AS floor
) AS src
ON (
    target.latitude  = src.latitude
    AND target.longitude = src.longitude
    AND (
        (target.floor IS NULL AND src.floor IS NULL)
        OR target.floor = src.floor
    )
)
WHEN NOT MATCHED THEN
    INSERT (
        source, external_id, web_link, link_to_gmap,
        latitude, longitude, address, postal_code, district, city,
        floor, floor_raw, is_exterior, has_lift, has_air_conditioning,
        match_confidence
    )
    VALUES (
        ?, ?, ?, ?,
        CAST(? AS NUMERIC(9,6)), CAST(? AS NUMERIC(9,6)),
        ?, ?, ?, ?,
        CAST(? AS SMALLINT), ?, ?, ?, ?,
        'inferred'
    )
WHEN MATCHED THEN
    UPDATE SET target.updated_at = GETDATE()
OUTPUT $action AS action, COALESCE(inserted.id, deleted.id) AS id;
"""

_INSERT_LISTING = """
INSERT INTO bronze.n8n_location_scraper_listings (
    id, building_id, run_id, status,
    surface_m2, surface_display, surface_unit,
    price_monthly, price_per_m2, currency,
    energy_class, days_on_market, first_listed_date,
    last_updated_date, first_time_extract, last_seen_date
)
OUTPUT inserted.id
VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_MERGE_CONTACT = """
MERGE bronze.n8n_location_scraper_contacts WITH (HOLDLOCK) AS target
USING (SELECT ? AS email) AS src
ON (target.email = src.email)
WHEN NOT MATCHED THEN
    INSERT (id, name, phone, email, contact_type, title, confidence, linkedin, source)
    VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?)
WHEN MATCHED THEN
    UPDATE SET target.updated_at = GETDATE()
OUTPUT COALESCE(inserted.id, deleted.id) AS id;
"""

_LINK_CONTACT = """
INSERT INTO bronze.n8n_location_scraper_listing_contacts (listing_id, contact_id)
SELECT ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM bronze.n8n_location_scraper_listing_contacts
    WHERE listing_id = ? AND contact_id = ?
)
"""

_READ_EXISTING = """
SELECT
    b.id,
    b.latitude,
    b.longitude,
    b.floor,
    ISNULL((
        SELECT TOP 1 l.price_monthly
        FROM bronze.n8n_location_scraper_listings l
        WHERE l.building_id = b.id
        ORDER BY l.last_seen_date DESC
    ), NULL) AS price_monthly,
    ISNULL((
        SELECT TOP 1 l.price_per_m2
        FROM bronze.n8n_location_scraper_listings l
        WHERE l.building_id = b.id
        ORDER BY l.last_seen_date DESC
    ), NULL) AS price_per_m2,
    ISNULL((
        SELECT TOP 1 l.status
        FROM bronze.n8n_location_scraper_listings l
        WHERE l.building_id = b.id
        ORDER BY l.last_seen_date DESC
    ), NULL) AS status,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM bronze.n8n_location_scraper_listings l
            JOIN bronze.n8n_location_scraper_listing_contacts lc ON l.id = lc.listing_id
            JOIN bronze.n8n_location_scraper_contacts c ON lc.contact_id = c.id
            WHERE l.building_id = b.id AND c.source = 'lusha'
        ) THEN 1 ELSE 0
    END AS has_lusha_contacts
FROM bronze.n8n_location_scraper_buildings b
WHERE b.city = ?
"""


def _building_key(lat: Optional[float], lon: Optional[float], floor: Optional[str]) -> Optional[str]:
    if lat is None or lon is None:
        return None
    lat_r = f"{lat:.4f}"
    lon_r = f"{lon:.4f}"
    floor_r = floor if floor is not None else "null"
    return f"{lat_r}_{lon_r}_{floor_r}"


def _floor_int(floor: Optional[str]) -> Optional[int]:
    if floor is None:
        return None
    try:
        return int(floor)
    except (ValueError, TypeError):
        return None


def upsert_sql(payload: dict) -> dict:
    """
    Main SQL persistence activity.

    payload = {
        "listings": [Listing.to_dict(), ...],
        "bundles": [ContactBundle.to_dict(), ...],   # consolidated agency contacts
        "run_id": str,
        "city": str,
    }

    Returns RunStats.to_dict().
    """
    listings = [Listing.from_dict(d) for d in payload["listings"]]
    bundles = {b["agency_name"]: ContactBundle.from_dict(b) for b in payload.get("bundles", [])}
    run_id: str = payload["run_id"]
    city: str = payload["city"]
    today = date.today().isoformat()

    sql = get_sql_client()

    # 1. Read existing buildings for this city
    existing_rows = sql.execute_query(_READ_EXISTING, (city,))
    existing_map: dict[str, dict] = {}
    for row in existing_rows:
        key = _building_key(
            float(row["latitude"]) if row["latitude"] is not None else None,
            float(row["longitude"]) if row["longitude"] is not None else None,
            str(row["floor"]) if row["floor"] is not None else None,
        )
        if key:
            existing_map[key] = row

    buildings_found = len(listings)
    buildings_new = 0
    buildings_updated = 0
    seen_in_run: set[str] = set()

    with sql.get_connection() as conn:
        cursor = conn.cursor()

        for listing in listings:
            if listing.latitude is None or listing.longitude is None:
                continue

            key = _building_key(listing.latitude, listing.longitude, listing.floor)
            if not key:
                continue

            existing = existing_map.get(key)

            if existing is None:
                # New building — skip intra-run duplicates
                if key in seen_in_run:
                    continue
                seen_in_run.add(key)
                buildings_new += 1

                # MERGE building
                floor_int = _floor_int(listing.floor)
                cursor.execute(
                    _MERGE_BUILDING,
                    (
                        listing.latitude, listing.longitude, floor_int,
                        listing.source, listing.external_id, listing.web_link, listing.link_to_gmap,
                        listing.latitude, listing.longitude,
                        listing.address, listing.postal_code, listing.district, listing.city,
                        floor_int, listing.floor,
                        1 if listing.is_exterior else (0 if listing.is_exterior is False else None),
                        1 if listing.has_lift else (0 if listing.has_lift is False else None),
                        1 if listing.has_air_conditioning else (0 if listing.has_air_conditioning is False else None),
                    ),
                )
                merge_row = cursor.fetchone()
                building_id = merge_row[1] if merge_row else None

                if not building_id:
                    logger.warning("MERGE returned no building_id for key=%s", key)
                    continue

                # INSERT listing (new building)
                cursor.execute(
                    _INSERT_LISTING,
                    (
                        str(building_id), run_id, listing.status,
                        listing.surface_m2, listing.surface_display, listing.surface_unit,
                        listing.price_monthly,
                        listing.price_per_m2, listing.currency,
                        listing.energy_class, listing.days_on_market,
                        listing.first_listed_date, listing.last_updated_date,
                        today, today,
                    ),
                )
                listing_row = cursor.fetchone()
                listing_id = listing_row[0] if listing_row else None

                if not listing_id:
                    continue

                # MERGE scraper contact (if email present)
                _upsert_scraper_contact(cursor, listing, str(listing_id))

                # MERGE Lusha contacts (from consolidated bundle)
                matching = listing.matching_name()
                if matching and matching in bundles:
                    _upsert_lusha_contacts(cursor, bundles[matching], str(listing_id))

            else:
                # Existing building — check if price/status changed
                price_changed = listing.price_monthly != existing.get("price_monthly")
                price_m2_changed = listing.price_per_m2 != existing.get("price_per_m2")
                status_changed = listing.status != existing.get("status")

                if not (price_changed or price_m2_changed or status_changed):
                    continue

                buildings_updated += 1

                # INSERT listing snapshot (update path — no first_time_extract)
                cursor.execute(
                    _INSERT_LISTING,
                    (
                        str(existing["id"]), run_id, listing.status,
                        listing.surface_m2, listing.surface_display, listing.surface_unit,
                        listing.price_monthly,
                        listing.price_per_m2, listing.currency,
                        listing.energy_class, listing.days_on_market,
                        listing.first_listed_date, listing.last_updated_date,
                        None, today,
                    ),
                )

        conn.commit()

    return RunStats(
        run_id=run_id,
        city=city,
        source=listings[0].source if listings else "",
        buildings_found=buildings_found,
        buildings_new=buildings_new,
        buildings_updated=buildings_updated,
    ).to_dict()


def _upsert_scraper_contact(cursor, listing: Listing, listing_id: str) -> None:
    email = listing.email or ""
    if not email or email == "null":
        return

    cursor.execute(
        _MERGE_CONTACT,
        (
            email,
            listing.contact_name, listing.phone, email,
            listing.contact_type, None, None, None, "scraper",
        ),
    )
    row = cursor.fetchone()
    contact_id = row[0] if row else None
    if contact_id:
        cursor.execute(_LINK_CONTACT, (listing_id, str(contact_id), listing_id, str(contact_id)))


def _upsert_lusha_contacts(cursor, bundle: ContactBundle, listing_id: str) -> None:
    slots = [
        (bundle.email_1, bundle.email_1_contact, bundle.email_1_title, bundle.email_1_confidence, bundle.email_1_linkedin),
        (bundle.email_2, bundle.email_2_contact, bundle.email_2_title, bundle.email_2_confidence, bundle.email_2_linkedin),
        (bundle.email_3, bundle.email_3_contact, bundle.email_3_title, bundle.email_3_confidence, bundle.email_3_linkedin),
    ]
    for email, name, title, confidence, linkedin in slots:
        if not email:
            continue
        try:
            conf_float = float(confidence) if confidence else None
        except ValueError:
            conf_float = None

        cursor.execute(
            _MERGE_CONTACT,
            (email, name, None, email, None, title, conf_float, linkedin, "lusha"),
        )
        row = cursor.fetchone()
        contact_id = row[0] if row else None
        if contact_id:
            cursor.execute(_LINK_CONTACT, (listing_id, str(contact_id), listing_id, str(contact_id)))
