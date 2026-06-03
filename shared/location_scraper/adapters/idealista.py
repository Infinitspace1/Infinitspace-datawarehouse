"""
Ported from the n8n "Format idealista" code node.
Handles both Spain (idealista.com) and Italy (idealista.it) — same actor, same schema.
"""
from __future__ import annotations

import math
from typing import Optional

from shared.location_scraper.config import IDEALISTA_ACTOR_ID, get_actor_max_items
from shared.location_scraper.models import Listing


def _get(obj: dict, path: str, fallback=None):
    """Traverse a nested dict with a slash-separated path."""
    for key in path.split("/"):
        if not isinstance(obj, dict):
            return fallback
        obj = obj.get(key)
        if obj is None or obj == "":
            return fallback
    return obj


def _parse_num(val) -> Optional[float]:
    if val is None:
        return None
    cleaned = str(val).replace("€", "").replace("$", "").replace("£", "").replace(" ", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_date(val) -> Optional[str]:
    if not val:
        return None
    from datetime import datetime, timezone
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


class IdealistaAdapter:
    actor_id: str = IDEALISTA_ACTOR_ID

    def build_input(self, start_url: str, max_items: int | None | str = "default") -> dict:
        payload = {
            "monitoringMode": False,
            "proxy": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"],
            },
            "startUrls": [start_url],
        }
        if max_items != "default":
            if max_items is not None:
                payload["maxItems"] = max_items
            return payload
        payload["maxItems"] = get_actor_max_items(default=100)
        return payload

    def normalize(self, raw_item: dict, city: str) -> Optional[Listing]:
        lat = _get(raw_item, "ubication/latitude")
        lon = _get(raw_item, "ubication/longitude")

        gmap_link = (
            f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
            if lat and lon
            else None
        )

        first_listed_str = _get(raw_item, "basicInfo/firstActivationDate")
        last_updated_str = _get(raw_item, "modificationDate/value")

        first_listed_date = _parse_date(first_listed_str)
        last_updated_date = _parse_date(last_updated_str)

        days_on_market: Optional[int] = None
        if first_listed_date:
            from datetime import date
            try:
                delta = date.today() - date.fromisoformat(first_listed_date)
                days_on_market = delta.days
            except ValueError:
                pass

        raw_energy = _get(raw_item, "energyCertification/energyConsumption/type")
        energy_class = None if raw_energy in (None, "", "inProcess") else raw_energy

        area = _parse_num(_get(raw_item, "basicInfo/size"))

        return Listing(
            source="idealista",
            city=city,
            external_id=str(_get(raw_item, "adid") or ""),
            web_link=_get(raw_item, "detailWebLink"),
            link_to_gmap=gmap_link,
            latitude=float(lat) if lat is not None else None,
            longitude=float(lon) if lon is not None else None,
            district=_get(raw_item, "basicInfo/district"),
            postal_code=_get(raw_item, "contactInfo/address/postalCode"),
            address=_get(raw_item, "ubication/title"),
            surface_m2=area,
            surface_display=area,
            surface_unit="m2",
            floor=str(_get(raw_item, "basicInfo/floor")) if _get(raw_item, "basicInfo/floor") is not None else None,
            status=_get(raw_item, "basicInfo/status"),
            is_exterior=_get(raw_item, "basicInfo/exterior"),
            has_lift=_get(raw_item, "basicInfo/hasLift"),
            has_air_conditioning=_get(raw_item, "basicInfo/features/hasAirConditioning"),
            price_monthly=_parse_num(_get(raw_item, "basicInfo/price")),
            price_per_m2=_parse_num(_get(raw_item, "basicInfo/priceByArea")),
            currency="EUR",
            energy_class=energy_class,
            first_listed_date=first_listed_date,
            last_updated_date=last_updated_date,
            days_on_market=days_on_market,
            contact_name=_get(raw_item, "contactInfo/commercialName"),
            company_name=_get(raw_item, "contactInfo/commercialName"),
            phone=_get(raw_item, "contactInfo/phone1/phoneNumberForMobileDialing"),
            contact_type=_get(raw_item, "contactInfo/userType"),
            email="",
            agency_comment=_get(raw_item, "comments/0/propertyComment"),
        )
