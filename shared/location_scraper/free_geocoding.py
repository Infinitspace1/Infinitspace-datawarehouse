"""
Free forward geocoding via Nominatim (OpenStreetMap) — no API key required.

Drop-in replacement for shared.gmaps.geocoding.GeocodingCache: exposes the same
`get_or_geocode(address) -> {latitude, longitude, formatted_address} | None`
interface, so the location-scraper activities can use it transparently when
GOOGLE_MAPS_API_KEY is not configured.

Nominatim usage policy (https://operations.osmfoundation.org/policies/nominatim/):
  - send a valid identifying User-Agent
  - max 1 request/second (we sleep between uncached calls)
  - results are cached in-memory per run to avoid duplicate lookups
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

NOMINATIM_URL = os.getenv(
    "NOMINATIM_URL", "https://nominatim.openstreetmap.org/search"
)
USER_AGENT = os.getenv(
    "NOMINATIM_USER_AGENT",
    "infinitspace-location-scraper/1.0 (data-engineering@infinitspace.com)",
)
REQUEST_TIMEOUT = 15
MIN_INTERVAL_SECONDS = 1.0  # respect Nominatim's 1 req/sec limit


def geocode_address(address: str) -> dict[str, Any] | None:
    """Geocode a free-text address via Nominatim.

    Returns {"latitude": float, "longitude": float, "formatted_address": str}
    or None when nothing is found / the request fails.
    """
    if not address or not address.strip():
        return None
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={"q": address.strip(), "format": "json", "limit": 1, "addressdetails": 0},
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            logger.info("No Nominatim results for: %s", address)
            return None
        top = data[0]
        return {
            "latitude": float(top["lat"]),
            "longitude": float(top["lon"]),
            "formatted_address": top.get("display_name", ""),
        }
    except (requests.RequestException, KeyError, ValueError, TypeError) as exc:
        logger.error("Nominatim geocoding failed for '%s': %s", address, exc)
        return None


class NominatimGeocodingCache:
    """In-memory cache + rate limiter around the free Nominatim geocoder.

    Mirrors GeocodingCache's public surface so it is interchangeable in the
    location-scraper activities.
    """

    def __init__(self) -> None:
        self._cache: dict[str, dict[str, Any] | None] = {}
        self._last_call_ts: float = 0.0

    @staticmethod
    def _normalise(address: str) -> str:
        return " ".join(address.lower().split())

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_call_ts
        if elapsed < MIN_INTERVAL_SECONDS:
            time.sleep(MIN_INTERVAL_SECONDS - elapsed)
        self._last_call_ts = time.monotonic()

    def get_or_geocode(self, address: str) -> dict[str, Any] | None:
        if not address:
            return None
        key = self._normalise(address)
        if key in self._cache:
            logger.debug("Nominatim cache hit: %s", address)
            return self._cache[key]

        self._throttle()
        result = geocode_address(address)
        self._cache[key] = result
        if result:
            logger.info(
                "Geocoded (free) '%s' -> (%.6f, %.6f)",
                address, result["latitude"], result["longitude"],
            )
        return result
