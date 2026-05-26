"""
Materialize globe read model rows from bronze.location_scraper_raw into
silver.location_scraper_globe_v2.

This keeps raw -> silver transformation in Function App code (Python),
not in SQL JSON parsing.
"""
from __future__ import annotations

import json
import logging
import os
from decimal import Decimal
from typing import Any

import pyodbc

from shared.gmaps.geocoding import GeocodingCache
from shared.azure_clients.sql_client import get_sql_client
from shared.location_scraper.config import get_country_code_for_city
from shared.location_scraper.geocoding import geocode_missing_coordinates

logger = logging.getLogger(__name__)

_READ_RAW = """
SELECT run_id, source, city, item_index, payload_json, inserted_at
FROM bronze.location_scraper_raw
WHERE run_id = ?
ORDER BY item_index
"""

_DELETE_RUN = "DELETE FROM silver.location_scraper_globe_v2 WHERE run_id = ?"

_READ_HUBSPOT_EXPORTS = """
SELECT item_index, hubspot_exported, hubspot_re_location_id
FROM silver.location_scraper_globe_v2
WHERE run_id = ?
"""

_REFRESH_QUALITY = "EXEC silver.sp_refresh_location_scraper_globe_quality @run_id = ?"
_REFRESH_GOLD_MAP_MARKERS = "EXEC gold.sp_refresh_location_scraper_map_markers"
_UPDATE_GOLD_MAP_MARKER_CURRENCY = """
;WITH latest_silver AS (
    SELECT
        source,
        run_city,
        external_id,
        latitude,
        longitude,
        currency,
        ROW_NUMBER() OVER (
            PARTITION BY
                source,
                run_city,
                COALESCE(external_id, CONCAT(latitude, N'|', longitude))
            ORDER BY inserted_at DESC, run_id DESC, item_index DESC
        ) AS rn
    FROM silver.location_scraper_globe_v2
    WHERE currency IS NOT NULL
)
UPDATE gold_markers
SET currency = latest_silver.currency
FROM gold.location_scraper_map_markers AS gold_markers
JOIN latest_silver
    ON latest_silver.rn = 1
   AND latest_silver.source = gold_markers.source
   AND latest_silver.run_city = gold_markers.run_city
   AND (
        (latest_silver.external_id IS NOT NULL AND latest_silver.external_id = gold_markers.external_id)
        OR (
            latest_silver.external_id IS NULL
            AND gold_markers.external_id IS NULL
            AND latest_silver.latitude = gold_markers.latitude
            AND latest_silver.longitude = gold_markers.longitude
        )
   )
"""

_INSERT_ROW = """
INSERT INTO silver.location_scraper_globe_v2 (
    run_id, item_index, source, run_city, inserted_at,
    external_id, listing_url,
    latitude, longitude,
    address, postal_code, district, city, country_code,
    price_monthly, price_per_m2, surface_m2, currency,
    contact_name, company_name, phone,
    lusha_email_1, lusha_contact_1, lusha_title_1, lusha_confidence_1,
    lusha_email_2, lusha_contact_2, lusha_title_2, lusha_confidence_2,
    lusha_email_3, lusha_contact_3, lusha_title_3, lusha_confidence_3,
    hubspot_exported, hubspot_re_location_id,
    refreshed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE())
"""

_READ_LUSHA_CONTACTS_FOR_SOURCE = """
SELECT DISTINCT
    b.latitude,
    b.longitude,
    b.floor,
    c.email,
    c.name,
    c.title,
    c.confidence
FROM bronze.n8n_location_scraper_buildings AS b
JOIN bronze.n8n_location_scraper_listings AS l
    ON l.building_id = b.id
JOIN bronze.n8n_location_scraper_listing_contacts AS lc
    ON lc.listing_id = l.id
JOIN bronze.n8n_location_scraper_contacts AS c
    ON c.id = lc.contact_id
WHERE b.source = ?
  AND c.source = 'lusha'
  AND c.email IS NOT NULL
ORDER BY b.latitude, b.longitude, b.floor, c.confidence DESC, c.email
"""


def _get_path(obj: Any, path: str) -> Any:
    cur = obj
    for token in path.split("."):
        if cur is None:
            return None
        if "[" in token and token.endswith("]"):
            key = token[: token.index("[")]
            idx = token[token.index("[") + 1 : -1]
            if key:
                if not isinstance(cur, dict):
                    return None
                cur = cur.get(key)
            if not isinstance(cur, list):
                return None
            try:
                i = int(idx)
            except ValueError:
                return None
            if i < 0 or i >= len(cur):
                return None
            cur = cur[i]
        else:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(token)
    return cur


def _pick_str(obj: dict[str, Any], *paths: str) -> str | None:
    for path in paths:
        val = _get_path(obj, path)
        if val is None:
            continue
        s = str(val).strip()
        if s:
            return s
    return None


def _pick_float(obj: dict[str, Any], *paths: str) -> float | None:
    for path in paths:
        val = _get_path(obj, path)
        if val is None or val == "":
            continue
        try:
            return float(str(val).replace(",", "."))
        except ValueError:
            continue
    return None


def _pick_decimal(obj: dict[str, Any], *paths: str) -> Decimal | None:
    for path in paths:
        val = _get_path(obj, path)
        if val is None or val == "":
            continue
        try:
            return Decimal(str(val).replace(",", "."))
        except Exception:
            continue
    return None


def _parse_floor(raw_floor: Any) -> str | None:
    if raw_floor is None or raw_floor == "":
        return None
    value = str(raw_floor).strip().lower()
    if value in {"parter", "eg", "erdgeschoss"}:
        return "0"
    import re
    match = re.search(r"-?\d+", value)
    return match.group(0) if match else None


def _pick_floor(payload: dict[str, Any], source: str) -> str | None:
    if source == "otodom":
        return _parse_floor(_get_path(payload, "floor"))
    if source == "idealista":
        return _parse_floor(_get_path(payload, "basicInfo.floor"))
    return _parse_floor(
        _pick_str(
            payload,
            "normalized.floor.current",
            "adTargetingParameters.obj_floor",
            "obj_floor",
            "floor",
        )
    )


def _pick_currency(payload: dict[str, Any], source: str) -> str | None:
    currency = _pick_str(payload, "priceCurrency", "normalized.price.currency", "basicInfo.currency", "currency")
    if currency:
        return currency.upper()
    return {"idealista": "EUR", "immobilienscout": "EUR", "otodom": "PLN"}.get(source)


def _first_seller_phone(payload: dict[str, Any]) -> str | None:
    phones = payload.get("sellerPhones")
    if isinstance(phones, dict):
        for _, value in phones.items():
            s = str(value).strip() if value is not None else ""
            if s:
                return s
    return None


def _otodom_individual_contact_name(payload: dict[str, Any]) -> str | None:
    company_name = (payload.get("agencyName") or "").strip().lower()
    phones = payload.get("sellerPhones")
    if not isinstance(phones, dict):
        return None
    for raw_name in phones:
        name = str(raw_name).strip()
        if name and name.lower() != company_name:
            return name
    return None


def _otodom_phone_for_contact(payload: dict[str, Any], contact_name: str | None) -> str | None:
    phones = payload.get("sellerPhones")
    if isinstance(phones, dict) and contact_name:
        value = phones.get(contact_name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return _pick_str(payload, "sellerPhone") or _first_seller_phone(payload)


def _building_key(latitude: Any, longitude: Any, floor: Any) -> tuple[str, str, str] | None:
    if latitude is None or longitude is None:
        return None
    floor_key = str(floor) if floor is not None else "null"
    return (f"{float(latitude):.4f}", f"{float(longitude):.4f}", floor_key)


def _contact_slots(contacts: list[dict[str, Any]]) -> tuple:
    slots: list[Any] = []
    for idx in range(3):
        contact = contacts[idx] if idx < len(contacts) else {}
        slots.extend(
            [
                contact.get("email"),
                contact.get("name"),
                contact.get("title"),
                contact.get("confidence"),
            ]
        )
    return tuple(slots)


def _load_lusha_contacts_by_building(sql, source: str) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    rows = sql.execute_query(_READ_LUSHA_CONTACTS_FOR_SOURCE, (source,))
    by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    by_geo_key: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    seen_by_key: dict[tuple[str, str, str], set[str]] = {}
    seen_by_geo_key: dict[tuple[str, str, str], set[str]] = {}

    for row in rows:
        contact = {
            "email": row.get("email"),
            "name": row.get("name"),
            "title": row.get("title"),
            "confidence": row.get("confidence"),
        }
        email = (contact["email"] or "").strip().lower()
        if not email:
            continue

        key = _building_key(row.get("latitude"), row.get("longitude"), row.get("floor"))
        geo_key = _building_key(row.get("latitude"), row.get("longitude"), None)
        for target_key, bucket_map, seen_map in (
            (key, by_key, seen_by_key),
            (geo_key, by_geo_key, seen_by_geo_key),
        ):
            if target_key is None:
                continue
            seen = seen_map.setdefault(target_key, set())
            if email in seen:
                continue
            seen.add(email)
            bucket_map.setdefault(target_key, []).append(contact)

    by_key.update({k: v for k, v in by_geo_key.items() if k not in by_key})
    return by_key


def _map_row(
    row: dict[str, Any],
    contacts_by_key: dict[tuple[str, str, str], list[dict[str, Any]]],
    hubspot_exports_by_item: dict[int, dict[str, Any]],
    geocode_cache: GeocodingCache | None = None,
) -> tuple:
    payload = json.loads(row["payload_json"])
    source = row["source"]

    country_default = get_country_code_for_city(row["city"]) or {"otodom": "PL", "immobilienscout": "DE"}.get(source)
    latitude = _pick_float(payload, "basicInfo.address.lat", "sections[3].location.lat", "ubication.latitude", "latitude", "geo_wgs84Lat", "normalized.address.latitude")
    longitude = _pick_float(payload, "basicInfo.address.lon", "sections[3].location.lng", "ubication.longitude", "longitude", "geo_wgs84Lon", "normalized.address.longitude")
    external_id = _pick_str(payload, "normalized.listingId", "header.id", "basicInfo.id", "adid", "id")
    listing_url = _pick_str(payload, "normalized.url", "detailWebLink", "propertyUrl", "basicInfo.url", "url")
    address = _pick_str(payload, "basicInfo.address.line", "normalized.address.formatted", "ubication.title", "street", "location", "basicInfo.address")
    postal_code = _pick_str(payload, "contactInfo.address.postalCode", "normalized.address.zip", "adTargetingParameters.obj_zipCode")
    district = _pick_str(payload, "district", "basicInfo.district", "geo_ot", "adTargetingParameters.obj_regio4", "subdistrict", "basicInfo.neighborhood", "ubication.administrativeAreaLevel3")
    city = _pick_str(payload, "basicInfo.municipality", "normalized.address.region", "normalized.address.city", "city", "ubication.administrativeAreaLevel2") or row["city"]
    country_code = (_pick_str(payload, "countryCode", "normalized.countryCode", "country", "basicInfo.country") or country_default or "").upper()

    if latitude is None or longitude is None:
        geocoded = geocode_missing_coordinates(
            cache=geocode_cache,
            source=source,
            run_city=row["city"],
            address=address,
            postal_code=postal_code,
            district=district,
            city=city,
            external_id=external_id,
        )
        if geocoded:
            latitude = geocoded["latitude"]
            longitude = geocoded["longitude"]
            if not address:
                address = geocoded.get("formatted_address")

    floor = _pick_floor(payload, source)
    contacts = (
        contacts_by_key.get(_building_key(latitude, longitude, floor))
        or contacts_by_key.get(_building_key(latitude, longitude, None))
        or []
    )
    contact_name = (
        _otodom_individual_contact_name(payload)
        if source == "otodom"
        else _pick_str(payload, "contactInfo.contactName", "normalized.contact.name", "contact.contactData.agent.name", "sellerName", "contactName", "advertiserName")
    )
    company_name = _pick_str(payload, "contactInfo.commercialName", "normalized.contact.company", "contact.contactData.agent.company", "agencyName")
    phone = (
        _otodom_phone_for_contact(payload, contact_name)
        if source == "otodom"
        else _pick_str(payload, "contactInfo.phone1.phoneNumberForMobileDialing", "normalized.contact.phone", "contact.phoneNumbers[0].text", "sellerPhone", "obj_phoneNumber") or _first_seller_phone(payload)
    )

    item_index = int(row["item_index"])
    hubspot_export = hubspot_exports_by_item.get(item_index, {})

    return (
        row["run_id"],
        item_index,
        source,
        row["city"],
        row["inserted_at"],
        external_id,
        listing_url,
        latitude,
        longitude,
        address,
        postal_code,
        district,
        city,
        country_code,
        _pick_decimal(payload, "price", "priceInfo.amount", "basicInfo.price", "normalized.price.amount", "adTargetingParameters.obj_rentPerMonth", "obj_totalRent"),
        _pick_decimal(payload, "pricePerM2", "priceByArea", "adTargetingParameters.obj_rentPerSqM", "obj_baseRent", "basicInfo.priceByArea"),
        _pick_decimal(payload, "area", "moreCharacteristics.constructedArea", "basicInfo.size", "adTargetingParameters.obj_mainFloorSpace", "normalized.area.livingSpace", "obj_netFloorSpace"),
        _pick_currency(payload, source),
        contact_name,
        company_name,
        phone,
        *_contact_slots(contacts),
        1 if hubspot_export.get("hubspot_exported") else 0,
        hubspot_export.get("hubspot_re_location_id"),
    )


def materialize_globe_run(payload: dict[str, Any]) -> dict[str, int]:
    run_id = payload["run_id"]
    sql = get_sql_client()
    rows = sql.execute_query(_READ_RAW, (run_id,))
    try:
        hubspot_export_rows = sql.execute_query(_READ_HUBSPOT_EXPORTS, (run_id,))
    except pyodbc.ProgrammingError as exc:
        sqlstate = exc.args[0] if exc.args else ""
        if sqlstate != "42S22":
            raise
        logger.warning(
            "location_scraper globe HubSpot export preservation skipped; run the globe v2 SQL migration. run_id=%s error=%s",
            run_id,
            exc,
        )
        hubspot_export_rows = []
    hubspot_exports_by_item = {int(row["item_index"]): row for row in hubspot_export_rows}

    sql.execute_non_query(_DELETE_RUN, (run_id,))
    if not rows:
        return {"rows_written": 0}

    contacts_by_key = _load_lusha_contacts_by_building(sql, rows[0]["source"])
    geocode_cache = GeocodingCache() if os.getenv("GOOGLE_MAPS_API_KEY") else None
    insert_rows = [_map_row(r, contacts_by_key, hubspot_exports_by_item, geocode_cache) for r in rows]
    written = sql.execute_many(_INSERT_ROW, insert_rows)
    logger.info("location_scraper globe materialized run_id=%s rows=%d", run_id, len(insert_rows))
    return {"rows_written": len(insert_rows) if written == -1 else int(written)}


def refresh_globe_quality(payload: dict[str, Any]) -> dict[str, int | str]:
    """Refresh materialized quality metrics for one scraper run."""
    run_id = payload["run_id"]
    sql = get_sql_client()
    try:
        sql.execute_non_query(_REFRESH_QUALITY, (run_id,))
    except pyodbc.ProgrammingError as exc:
        sqlstate = exc.args[0] if exc.args else ""
        if sqlstate == "42S02":
            logger.warning(
                "location_scraper globe quality refresh skipped; required quality SQL objects are missing. run_id=%s error=%s",
                run_id,
                exc,
            )
            return {"run_id": run_id, "status": "skipped_missing_sql_object"}
        raise
    logger.info("location_scraper globe quality refreshed run_id=%s", run_id)
    return {"run_id": run_id, "status": "refreshed"}


def refresh_gold_map_markers(payload: dict[str, Any] | None = None) -> dict[str, str]:
    """Refresh the gold building-level map marker table used by the RE dashboard."""
    run_id = (payload or {}).get("run_id", "")
    sql = get_sql_client()
    try:
        sql.execute_non_query(_REFRESH_GOLD_MAP_MARKERS)
        sql.execute_non_query(_UPDATE_GOLD_MAP_MARKER_CURRENCY)
    except pyodbc.ProgrammingError as exc:
        sqlstate = exc.args[0] if exc.args else ""
        if sqlstate in {"42S02", "42000", "42S22"}:
            logger.warning(
                "location_scraper gold map marker refresh skipped; required gold SQL objects are missing. run_id=%s error=%s",
                run_id,
                exc,
            )
            return {"run_id": run_id, "status": "skipped_missing_sql_object"}
        raise
    logger.info("location_scraper gold map markers refreshed run_id=%s", run_id)
    return {"run_id": run_id, "status": "refreshed"}
