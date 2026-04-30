"""
Ported from the n8n "Format Otodom" code node.
Handles Poland (otodom.pl).

Note: the n8n node filters to Warsaw only (res.city.lower() == 'warszawa').
We preserve this filter — the city field from Otodom's dataset is in Polish.
"""
from __future__ import annotations

from typing import Optional

from shared.location_scraper.config import OTODOM_ACTOR_ID
from shared.location_scraper.models import Listing


def _parse_num(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", ".").replace(" ", ""))
    except ValueError:
        return None


def _parse_date(val) -> Optional[str]:
    if not val:
        return None
    from datetime import datetime
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


class OtodomAdapter:
    actor_id: str = OTODOM_ACTOR_ID

    def build_input(self, start_url: str) -> dict:
        return {
            "startUrls": [{"url": start_url}],
            "maxItems": 200,
            "extractDetails": True,
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"],
            },
        }

    def normalize(self, raw_item: dict, city: str) -> Optional[Listing]:
        # The Otodom dataset city field is in Polish; filter to Warsaw only.
        item_city = raw_item.get("city", "")
        if not item_city or item_city.lower() != "warszawa":
            return None

        lat = raw_item.get("latitude")
        lon = raw_item.get("longitude")
        gmap_link = (
            f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
            if lat and lon
            else None
        )

        raw_floor = raw_item.get("floor")
        floor = "0" if raw_floor == "parter" else (str(raw_floor) if raw_floor is not None else None)

        additional_features: list[str] = raw_item.get("additionalFeatures") or []
        lift_entry = next((f for f in additional_features if f.startswith("lift:")), None)
        has_lift: Optional[bool] = (lift_entry.find("::y") != -1) if lift_entry else None

        features: list[str] = raw_item.get("features") or []
        has_air_conditioning: Optional[bool] = (
            True if any("klimatyzacja" in f.lower() for f in features) else None
        )

        price_monthly = _parse_num(raw_item.get("price"))
        area = _parse_num(raw_item.get("area"))
        price_per_m2 = _parse_num(raw_item.get("pricePerM2"))
        if price_per_m2 is None and price_monthly and area and area > 0:
            price_per_m2 = round(price_monthly / area, 2)

        last_updated_date = _parse_date(raw_item.get("dateModified"))

        address = raw_item.get("street")
        if address and raw_item.get("district"):
            address = f"{address}, {raw_item['district']}"
        elif not address:
            address = raw_item.get("location")

        # Extract individual agent from sellerPhones (dict of name → phone)
        company_name: Optional[str] = raw_item.get("agencyName")
        contact_name: Optional[str] = None
        phone: Optional[str] = None

        seller_phones = raw_item.get("sellerPhones")
        if isinstance(seller_phones, dict):
            individual_names = [n for n in seller_phones if n != company_name]
            if individual_names:
                contact_name = individual_names[0]
                phone = seller_phones[contact_name]

        # Fallbacks for listings where sellerPhones has no per-person key.
        if not contact_name:
            contact_name = (
                raw_item.get("sellerName")
                or raw_item.get("contactName")
                or raw_item.get("advertiserName")
            )

        if not phone:
            phone = raw_item.get("sellerPhone")

        return Listing(
            source="otodom",
            city=city,
            external_id=str(raw_item.get("id", "")),
            web_link=raw_item.get("propertyUrl"),
            link_to_gmap=gmap_link,
            latitude=float(lat) if lat is not None else None,
            longitude=float(lon) if lon is not None else None,
            district=raw_item.get("district"),
            postal_code=None,
            address=address,
            available_surface_m2=area,
            floor=floor,
            status=raw_item.get("condition"),
            is_exterior=None,
            has_lift=has_lift,
            has_air_conditioning=has_air_conditioning,
            price_monthly=price_monthly,
            price_per_m2=price_per_m2,
            currency=raw_item.get("priceCurrency"),
            energy_class=None,
            first_listed_date=None,
            last_updated_date=last_updated_date,
            days_on_market=None,
            company_name=company_name,
            contact_name=contact_name,
            phone=phone,
            contact_type=raw_item.get("sellerType"),
            email="",
            agency_comment=None,
        )
