"""
Activities: start_apify_run, check_apify_run, fetch_dataset, normalize_listings

These are thin wrappers that the Durable orchestrator calls as activities.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from shared.gmaps.geocoding import GeocodingCache
from shared.location_scraper import clients as _c
from shared.location_scraper.adapters.registry import ADAPTER_REGISTRY
from shared.location_scraper.clients import apify as apify_client
from shared.location_scraper.free_geocoding import NominatimGeocodingCache
from shared.location_scraper.geocoding import geocode_missing_coordinates
from shared.location_scraper.models import Listing, SourceConfig

logger = logging.getLogger(__name__)


def _apply_geocode_fallback(listing: Listing, run_city: str, cache: GeocodingCache | None) -> bool:
    if listing.latitude is not None and listing.longitude is not None:
        return False
    result = geocode_missing_coordinates(
        cache=cache,
        source=listing.source,
        run_city=run_city,
        address=listing.address,
        postal_code=listing.postal_code,
        district=listing.district,
        city=listing.city,
        external_id=listing.external_id,
    )
    if not result:
        return False

    listing.latitude = result["latitude"]
    listing.longitude = result["longitude"]
    listing.link_to_gmap = f"https://www.google.com/maps/search/?api=1&query={listing.latitude},{listing.longitude}"
    if not listing.address:
        listing.address = result.get("formatted_address")
    return True


def start_apify_run(config: dict) -> dict:
    """
    Start the Apify actor for the resolved source.
    Returns {"run_id": str, "dataset_id": str, "actor": str}.
    """
    src = SourceConfig.from_dict(config)
    adapter = ADAPTER_REGISTRY[src.actor]
    actor_input = adapter.build_input(
        src.start_url,
        max_items=None if src.unlimited_items else "default",
    )
    result = apify_client.start_run(src.actor_id, actor_input)
    result["actor"] = src.actor
    return result


def check_apify_run(run_info: dict) -> dict:
    """Poll run status. Returns {"finished": bool, "succeeded": bool, "status": str}."""
    return apify_client.get_run_status(run_info["run_id"])


def fetch_dataset(run_info: dict) -> list[dict[str, Any]]:
    """Download dataset items from the completed run."""
    return apify_client.fetch_dataset(run_info["dataset_id"])


def normalize_listings(payload: dict) -> list[dict]:
    """
    Dispatch raw Apify items through the source adapter.

    payload = {"actor": str, "city": str, "items": [...]}   # in-memory items (tests / back-compat)
           or {"actor": str, "city": str, "run_id": str}    # streamed: read raw back from SQL

    When ``items`` is absent the raw payloads are read page-by-page from
    ``bronze.location_scraper_raw`` (where the streaming fetch persisted them),
    so the full dataset never transits the Durable orchestrator — this is the
    worker-OOM fix for large cities.

    Returns a list of Listing.to_dict() dicts (JSON-serialisable for Durable).
    """
    actor = payload["actor"]
    city: str = payload["city"]
    items = payload.get("items")
    if items is None:
        from shared.location_scraper.activities.raw_payload import read_raw_items

        items = read_raw_items(payload["run_id"])

    adapter = ADAPTER_REGISTRY[actor]
    results = []
    # Google Maps when a key is set, else the free Nominatim geocoder.
    geocode_cache = GeocodingCache() if os.getenv("GOOGLE_MAPS_API_KEY") else NominatimGeocodingCache()
    geocoded_count = 0
    seen = 0
    for raw in items:
        seen += 1
        try:
            listing = adapter.normalize(raw, city)
            if listing is not None:
                if _apply_geocode_fallback(listing, city, geocode_cache):
                    geocoded_count += 1
                results.append(listing.to_dict())
        except Exception:
            logger.exception("normalize error for item %s", raw.get("id") or raw.get("adid"))
    logger.info(
        "Normalized %d/%d items for actor=%s city=%s geocoded_missing_coords=%d",
        len(results),
        seen,
        actor,
        city,
        geocoded_count,
    )
    return results
