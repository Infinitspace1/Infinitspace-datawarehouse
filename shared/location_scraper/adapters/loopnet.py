"""
LoopNet adapter — commercial offices for lease.

Input source:
  - Apify actor: memo23/loopnet-scraper-ppe (pay-per-event)
  - Actor id: 0ZCQONxB3BdyOzrbD
  - Works for US and UK (LoopNet UK / ex-Realla). Currently wired for London (UK).

Notes vs the other sources:
  - LoopNet quotes areas in **square feet** — we convert to m² (`SF_TO_M2`).
  - Listings expose no coordinates; lat/lng are filled later by the geocode
    fallback in `activities/scrape.py` (postcode makes this reliable).
  - Broker name / company / phone / **email** come straight from the payload,
    so unlike idealista/otodom we can populate `email` directly.
  - Hard guardrail: buildings with < 1500 m² of available space are dropped.
"""
from __future__ import annotations

import re
from typing import Any, Optional

from shared.location_scraper.config import LOOPNET_ACTOR_ID, get_actor_max_items
from shared.location_scraper.models import Listing

# 1 square foot = 0.092903 m²
SF_TO_M2 = 0.092903

# Minimum available office space we care about (m²). Anything smaller is trash.
MIN_SURFACE_M2 = 1500.0


# LoopNet quotes square footage as either "SF" (US) or "sq ft" (UK / Realla).
_SF_UNIT = r"(?:SF|sq\s*ft)"


def _first_sf(text: Any) -> Optional[float]:
    """Extract the first square-footage number from a string like
    '33,889 SF of Office Space Available' or '41,511 sq ft' -> 33889.0 / 41511.0."""
    if text is None:
        return None
    match = re.search(rf"(\d[\d,]*)\s*{_SF_UNIT}", str(text), flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _available_sf_from_name(name: Any) -> Optional[float]:
    """Available square footage parsed from a LoopNet listing name/title.

    The memo23 actor's listing-detail payload (``sourceType="listingWeb"``, since
    its 2026-06-27 rebuild) no longer carries ``header``/``spaces`` — the only
    available-area signal is the name, e.g.
    ``"The Concorde | 2222 W Dunlap Ave - 1,605 - 103,916 SF of 4-Star Space Available in Phoenix, AZ"``.
    The figure is a range (smallest unit - largest available); we take the upper
    bound, which mirrors the old ``header.subtext`` total-available semantic. A
    name with no "... SF ... Available" clause (e.g. coworking listings) -> None.
    """
    if name is None:
        return None
    match = re.search(
        rf"([\d,]+)(?:\s*-\s*([\d,]+))?\s*{_SF_UNIT}\b.{{0,60}}?Available",
        str(name),
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    try:
        values = [float(g.replace(",", "")) for g in match.groups() if g]
    except ValueError:
        return None
    return max(values) if values else None


def _parse_abbrev_sf(value: Any) -> Optional[float]:
    """Parse the broad-search ``sizeSf`` field, which uses K/M abbreviations:
    '36.8K' -> 36800, '624K' -> 624000, '1.2M' -> 1200000, '29,186' -> 29186."""
    if value is None:
        return None
    match = re.search(r"([\d,]+(?:\.\d+)?)\s*([KkMm]?)", str(value).strip())
    if not match:
        return None
    try:
        number = float(match.group(1).replace(",", ""))
    except ValueError:
        return None
    suffix = (match.group(2) or "").upper()
    if suffix == "K":
        number *= 1_000
    elif suffix == "M":
        number *= 1_000_000
    return number


def available_surface_sqft_from_payload(payload: dict[str, Any]) -> Optional[float]:
    """Best available-surface estimate in **square feet** for one LoopNet building.

    This is LoopNet's native unit — the exact figure a UK/US broker recognises,
    so we keep it verbatim for `surface_display`.

    memo23 returns three different payload shapes depending on the actor version
    and the input mode, so we try each available-area signal in turn:
      1. header.subtext ("33,889 SF of Office Space Available") — old detail page.
      2. sum of spaces[].size — old detail page fallback.
      3. name / listingName ("... - 103,916 SF of ... Available") — the
         post-2026-06-27 listing-detail ("listingWeb") payload.
      4. sizeSf ("36.8K") — the broad-search / search-results payload.
    """
    header = payload.get("header") if isinstance(payload.get("header"), dict) else {}
    sf = _first_sf(header.get("subtext"))

    if sf is None:
        total = 0.0
        found = False
        for space in payload.get("spaces") or []:
            if not isinstance(space, dict):
                continue
            size = _first_sf(space.get("size"))
            if size:
                total += size
                found = True
        sf = total if found else None

    if sf is None:
        sf = _available_sf_from_name(payload.get("name") or payload.get("listingName"))

    if sf is None:
        sf = _parse_abbrev_sf(payload.get("sizeSf"))

    return sf


def available_surface_m2_from_payload(payload: dict[str, Any]) -> Optional[float]:
    """Canonical available-surface in m² (sq ft -> m²) for one LoopNet building.

    Derived from `available_surface_sqft_from_payload` so the m² value and the
    native sqft display value always agree.
    """
    sf = available_surface_sqft_from_payload(payload)
    if sf is None:
        return None
    return round(sf * SF_TO_M2, 2)


def currency_for_country(country: Any) -> str:
    """LoopNet has no currency field — derive it from the country code."""
    code = str(country or "").strip().upper()
    if code in {"GB", "UK", "GBR", "UNITED KINGDOM", "ENGLAND"}:
        return "GBP"
    if code in {"CA", "CAN", "CANADA"}:
        return "CAD"
    return "USD"


def _num(val: Any) -> Optional[float]:
    """Only accept a genuine number. LoopNet lease prices are quoted as
    '$X/SF/YR' or 'Upon Request' — we deliberately do NOT try to coerce those
    into a monthly figure (the unit is ambiguous), so anything non-numeric -> None."""
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    return None


class LoopnetAdapter:
    actor_id: str = LOOPNET_ACTOR_ID

    def build_input(
        self,
        start_url: str | list[str],
        max_items: int | None | str = "default",
    ) -> dict:
        # A list means individual listing-detail URLs (enumerated from the
        # filtered search pages) — the actor scrapes exactly those listings.
        # A single URL is the legacy broad search (geocoded, capped at 500).
        urls = start_url if isinstance(start_url, list) else [start_url]
        payload = {
            "startUrls": [{"url": u} for u in urls],
            "includeListingDetails": True,
            # Bypass memo23's default 500-item-per-bounding-box cap so the
            # space-available-filtered search returns the FULL qualifying set
            # (the reason the old enumeration 2-step existed). The broad-search
            # payload carries broker contact + sizeSf, which the enumerated
            # listing-URL payload lost in the actor's 2026-06-27 rebuild.
            "moreResults": True,
            "maxConcurrency": 20,
            "minConcurrency": 1,
            "proxy": {"useApifyProxy": True},
        }
        if max_items != "default":
            if max_items is not None:
                payload["maxItems"] = max_items
            return payload
        payload["maxItems"] = get_actor_max_items(default=100)
        return payload

    def normalize(self, raw_item: dict, city: str) -> Optional[Listing]:
        surface_sqft = available_surface_sqft_from_payload(raw_item)
        surface = available_surface_m2_from_payload(raw_item)
        # Hard guardrail: anything under 1500 m² of available space is trash.
        if surface is None or surface < MIN_SURFACE_M2:
            return None

        web_link = raw_item.get("listingUrl") or raw_item.get("inferUrl")
        city_value = str(raw_item.get("city") or city).lower().strip()

        return Listing(
            source="loopnet",
            city=city_value,
            external_id=str(raw_item.get("propertyId") or ""),
            web_link=web_link,
            link_to_gmap=None,  # filled by geocode fallback (no coords in payload)
            latitude=None,
            longitude=None,
            district=None,
            postal_code=raw_item.get("zip") or None,
            address=raw_item.get("address") or None,
            surface_m2=surface,
            # LoopNet is UK/US — show the native square footage, not m².
            surface_display=round(surface_sqft, 2) if surface_sqft is not None else None,
            surface_unit="sqft",
            floor=None,
            status=None,
            is_exterior=None,
            has_lift=None,
            has_air_conditioning=None,
            price_monthly=_num(raw_item.get("priceNumeric")),
            price_per_m2=None,
            currency=currency_for_country(raw_item.get("country")),
            energy_class=None,
            first_listed_date=None,
            last_updated_date=None,
            days_on_market=None,
            contact_name=raw_item.get("brokerName") or None,
            company_name=raw_item.get("brokerCompany") or None,
            phone=raw_item.get("brokerPhone") or None,
            contact_type=None,
            email=raw_item.get("brokerEmail") or "",
            agency_comment=None,
        )
