"""
Immobilienscout24 adapter for Germany office rentals.

Input source:
  - Apify actor: memo23/immobilienscout24-scraper
  - Actor id: ciTdHfgOkkwfEzTE9
"""
from __future__ import annotations

import re
from typing import Any, Optional

from shared.location_scraper.config import IMMOBILIENSCOUT_ACTOR_ID, get_actor_max_items
from shared.location_scraper.models import Listing


def _dig(obj: Any, path: str):
    """Traverse slash paths; numeric segments step into lists (Apify nested JSON vs flat CSV export)."""
    cur: Any = obj
    for part in path.split("/"):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list) and part.isdigit():
            idx = int(part)
            cur = cur[idx] if 0 <= idx < len(cur) else None
        else:
            return None
    if cur in ("", None):
        return None
    return cur


def _pick(obj: dict, *keys: str):
    """Prefer literal flat keys (Apify CSV / flatten), else nested path traversal."""
    for key in keys:
        if isinstance(obj, dict) and key in obj and obj[key] not in (None, ""):
            return obj[key]
        if "/" in key:
            val = _dig(obj, key)
            if val not in (None, ""):
                return val
    return None


def _parse_num(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    cleaned = str(val).replace("\xa0", " ")
    match = re.search(r"(\d[\d.,]*)", cleaned.replace(" ", ""))
    if not match:
        return None
    token = match.group(1)
    # Handle common EU/US thousand separators in scraped strings.
    if "," in token and "." not in token:
        parts = token.split(",")
        if len(parts) == 2 and len(parts[1]) == 3:
            token = "".join(parts)  # 1,850 -> 1850
        else:
            token = token.replace(",", ".")
    elif "." in token and "," not in token:
        parts = token.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            token = "".join(parts)  # 1.850 -> 1850
    elif "." in token and "," in token:
        token = token.replace(".", "").replace(",", ".")
    try:
        return float(token)
    except ValueError:
        return None


def _parse_floor(raw_floor) -> Optional[str]:
    if raw_floor in (None, ""):
        return None
    value = str(raw_floor).strip().lower()
    if value in {"eg", "erdgeschoss"}:
        return "0"
    match = re.search(r"-?\d+", value)
    return match.group(0) if match else None


class ImmobilienscoutAdapter:
    actor_id: str = IMMOBILIENSCOUT_ACTOR_ID

    def build_input(self, start_url: str, max_items: int | None | str = "default") -> dict:
        payload = {
            "startUrls": [{"url": start_url}],
            "maxConcurrency": 10,
            "minConcurrency": 1,
            "maxRequestRetries": 100,
            "proxyConfiguration": {"useApifyProxy": True},
        }
        if max_items != "default":
            if max_items is not None:
                payload["maxItems"] = max_items
            return payload
        payload["maxItems"] = get_actor_max_items(default=100)
        return payload

    def normalize(self, raw_item: dict, city: str) -> Optional[Listing]:
        area = _parse_num(
            _pick(
                raw_item,
                "normalized/area/livingSpace",
                "normalized/area/usableArea",
                "adTargetingParameters/obj_mainFloorSpace",
                "obj_netFloorSpace",
                "obj_livingSpace",
                "netFloorSpace",
                "livingSpace",
                "area",
            )
        )
        # Hard guardrail in code in addition to URL filter.
        if area is None or area < 1500:
            return None

        lat = _parse_num(
            _pick(
                raw_item,
                "normalized/address/latitude",
                "basicInfo/address/lat",
                "geo_wgs84Lat",
                "latitude",
                "lat",
            )
        )
        lon = _parse_num(
            _pick(
                raw_item,
                "normalized/address/longitude",
                "basicInfo/address/lon",
                "geo_wgs84Lon",
                "longitude",
                "lng",
                "lon",
            )
        )
        # normalized/address/region = actual city ("München"); normalized/address/city = neighborhood ("Schwabing-Ost")
        city_value = _pick(raw_item, "normalized/address/region", "normalized/address/city", "adTargetingParameters/geo_krs", "geo_city", "city") or city
        district = _pick(raw_item, "normalized/address/city", "geo_quarter", "district", "geo_ot")
        postal_code = _pick(raw_item, "normalized/address/zip", "geo_plz", "postalCode", "zipCode")
        street = _pick(raw_item, "normalized/address/street", "geo_street", "street")
        # IS24 hides some addresses with a German placeholder text — discard those
        if street and ("vollständige adresse" in str(street).lower() or "vom anbieter" in str(street).lower()):
            street = None
        house_number = _pick(raw_item, "normalized/address/houseNumber", "geo_houseNumber", "houseNumber")
        address = f"{street} {house_number}".strip() if street else _pick(raw_item, "address", "location")
        if not address:
            address = _pick(raw_item, "normalized/address/formatted", "basicInfo/address/line")

        price_monthly = _parse_num(
            _pick(
                raw_item,
                "normalized/price/amount",
                "adTargetingParameters/obj_rentPerMonth",
                "obj_totalRent",
                "totalRent",
                "price",
            )
        )
        price_per_m2 = _parse_num(
            _pick(
                raw_item,
                "adTargetingParameters/obj_rentPerSqM",
                "obj_baseRent",
                "pricePerSqm",
                "pricePerM2",
            )
        )
        web_link = _pick(raw_item, "normalized/url", "basicInfo/url", "url", "obj_url")
        if web_link and isinstance(web_link, str) and web_link.startswith("/"):
            web_link = f"https://www.immobilienscout24.de{web_link}"

        contact_name = _pick(
            raw_item,
            "normalized/contact/name",
            "contact/contactData/agent/name",
            "basicInfo/realtor/fullName",
            "obj_contactName",
            "contactName",
            "providerContact",
        )
        company_name = _pick(
            raw_item,
            "normalized/contact/company",
            "contact/contactData/agent/company",
            "sections/0/company",
            "obj_company",
            "companyName",
            "realtorCompanyName",
            "obj_realtorCompanyName",
        )
        phone = _pick(
            raw_item,
            "normalized/contact/phone",
            "contact/phoneNumbers/0/text",
            "sections/0/phoneNumbers/0/text",
            "obj_phoneNumber",
            "phoneNumber",
            "phone",
        )
        status = _pick(raw_item, "header/publicationState", "publicationState", "status")
        floor = _parse_floor(_pick(raw_item, "normalized/floor/current", "adTargetingParameters/obj_floor", "obj_floor", "floor"))
        currency = _pick(raw_item, "normalized/price/currency", "currency") or "EUR"
        external_id = str(_pick(raw_item, "normalized/listingId", "header/id", "basicInfo/id", "id", "obj_scoutId", "realEstateId") or "")

        gmap_link = (
            f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
            if lat is not None and lon is not None
            else None
        )

        return Listing(
            source="immobilienscout",
            city=str(city_value).lower().strip(),
            external_id=external_id,
            web_link=web_link,
            link_to_gmap=gmap_link,
            latitude=lat,
            longitude=lon,
            district=district,
            postal_code=postal_code,
            address=address,
            surface_m2=area,
            surface_display=area,
            surface_unit="m2",
            floor=floor,
            status=status,
            is_exterior=None,
            has_lift=None,
            has_air_conditioning=None,
            price_monthly=price_monthly,
            price_per_m2=price_per_m2,
            currency=currency,
            energy_class=None,
            first_listed_date=None,
            last_updated_date=None,
            days_on_market=None,
            contact_name=contact_name,
            company_name=company_name,
            phone=phone,
            contact_type=None,
            email="",
            agency_comment=None,
        )
