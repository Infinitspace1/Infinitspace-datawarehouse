from __future__ import annotations

import logging
import os
import re
from typing import Any

from shared.gmaps.geocoding import GeocodingCache

logger = logging.getLogger(__name__)


def get_geocoding_cache():
    """Return the geocoder to use for filling missing coordinates.

    Uses Google Maps when GOOGLE_MAPS_API_KEY is set, otherwise falls back to
    the free Nominatim (OpenStreetMap) geocoder. Both expose the same
    `get_or_geocode(address)` interface.
    """
    if os.getenv("GOOGLE_MAPS_API_KEY"):
        return GeocodingCache()
    from shared.location_scraper.free_geocoding import NominatimGeocodingCache

    logger.info("GOOGLE_MAPS_API_KEY not set — using free Nominatim geocoder")
    return NominatimGeocodingCache()

COUNTRY_NAME_BY_SOURCE = {
    "idealista": {
        "madrid": "Spain",
        "barcelona": "Spain",
        "seville": "Spain",
        "valencia": "Spain",
        "milan": "Italy",
    },
    "otodom": {"warsaw": "Poland"},
    "loopnet": {
        "london": "United Kingdom",
        "new york": "United States",
        "san francisco": "United States",
        "palo alto": "United States",
        "los angeles": "United States",
        "austin": "United States",
        "seattle": "United States",
        "redwood city": "United States",
        "san mateo": "United States",
        "san bruno": "United States",
        "cupertino": "United States",
        "toronto": "Canada",
    },
    "immobilienscout": {
        "berlin": "Germany",
        "munich": "Germany",
        "hamburg": "Germany",
        "cologne": "Germany",
        "frankfurt": "Germany",
        "dusseldorf": "Germany",
        "stuttgart": "Germany",
    },
}


def clean_geocode_part(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value)
    text = re.sub(r"\([^)]*incomplete address[^)]*\)", "", text, flags=re.I)
    text = re.sub(r"\bvollst[aä]ndige adresse[^,]*", "", text, flags=re.I)
    text = " ".join(text.replace("\xa0", " ").split())
    return text.strip(" ,") or None


def build_geocode_query(
    *,
    source: str,
    run_city: str,
    address: str | None,
    postal_code: str | None,
    district: str | None,
    city: str | None,
) -> str | None:
    address = clean_geocode_part(address)
    postal_code = clean_geocode_part(postal_code)
    district = clean_geocode_part(district)
    city = clean_geocode_part(city or run_city)
    country = COUNTRY_NAME_BY_SOURCE.get(source, {}).get((run_city or "").lower())

    # Avoid city-only geocoding: it would stack too many vague records on one marker.
    if not any([address, postal_code, district]):
        return None

    parts = [address, postal_code, district, city, country]
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part:
            continue
        key = part.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(part)
    return ", ".join(deduped)


def geocode_missing_coordinates(
    *,
    cache: GeocodingCache | None,
    source: str,
    run_city: str,
    address: str | None,
    postal_code: str | None,
    district: str | None,
    city: str | None,
    external_id: Any = None,
) -> dict[str, Any] | None:
    if cache is None:
        return None
    query = build_geocode_query(
        source=source,
        run_city=run_city,
        address=address,
        postal_code=postal_code,
        district=district,
        city=city,
    )
    if not query:
        return None

    result = cache.get_or_geocode(query)
    if result:
        logger.info(
            "Geocoded missing scraper coordinates source=%s city=%s external_id=%s query=%s",
            source,
            run_city,
            external_id,
            query,
        )
    return result
