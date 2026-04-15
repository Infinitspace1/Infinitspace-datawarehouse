"""
shared/gmaps/geocoding.py

Forward geocoding using Google Maps Geocoding API.
Provides a simple address-to-coordinates function with in-memory caching
to avoid duplicate API calls for the same address.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"
REQUEST_TIMEOUT = 15


def geocode_address(address: str, api_key: str | None = None) -> dict[str, Any] | None:
    """Geocode a street address to latitude/longitude using Google Maps API.

    Returns {"latitude": float, "longitude": float, "formatted_address": str}
    or None if geocoding fails.
    """
    key = api_key or os.getenv("GOOGLE_MAPS_API_KEY")
    if not key:
        logger.warning("GOOGLE_MAPS_API_KEY not set — skipping geocoding")
        return None

    if not address or not address.strip():
        return None

    try:
        resp = requests.get(
            GEOCODING_URL,
            params={"address": address.strip(), "key": key},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        status = data.get("status")
        if status == "OK" and data.get("results"):
            result = data["results"][0]
            loc = result["geometry"]["location"]
            return {
                "latitude": loc["lat"],
                "longitude": loc["lng"],
                "formatted_address": result.get("formatted_address", ""),
            }

        if status == "ZERO_RESULTS":
            logger.info("No geocoding results for: %s", address)
        elif status == "OVER_QUERY_LIMIT":
            logger.warning("Google Maps geocoding quota exceeded")
        else:
            logger.warning("Geocoding status %s for: %s", status, address)

        return None

    except requests.RequestException as e:
        logger.error("Geocoding request failed for '%s': %s", address, e)
        return None


class GeocodingCache:
    """In-memory cache that deduplicates geocoding calls by normalised address."""

    def __init__(self, api_key: str | None = None):
        self._cache: dict[str, dict[str, Any] | None] = {}
        self._api_key = api_key or os.getenv("GOOGLE_MAPS_API_KEY")

    @staticmethod
    def _normalise(address: str) -> str:
        return " ".join(address.lower().split())

    def get_or_geocode(self, address: str) -> dict[str, Any] | None:
        """Return cached result or geocode and cache the result."""
        if not address:
            return None

        key = self._normalise(address)
        if key in self._cache:
            logger.debug("Geocoding cache hit: %s", address)
            return self._cache[key]

        result = geocode_address(address, api_key=self._api_key)
        self._cache[key] = result

        if result:
            logger.info(
                "Geocoded '%s' -> (%.6f, %.6f)",
                address, result["latitude"], result["longitude"],
            )
        return result
