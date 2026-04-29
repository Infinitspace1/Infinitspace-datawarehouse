"""
Activities: start_apify_run, check_apify_run, fetch_dataset, normalize_listings

These are thin wrappers that the Durable orchestrator calls as activities.
"""
from __future__ import annotations

import logging
from typing import Any

from shared.location_scraper import clients as _c
from shared.location_scraper.adapters.registry import ADAPTER_REGISTRY
from shared.location_scraper.clients import apify as apify_client
from shared.location_scraper.models import Listing, SourceConfig

logger = logging.getLogger(__name__)


def start_apify_run(config: dict) -> dict:
    """
    Start the Apify actor for the resolved source.
    Returns {"run_id": str, "dataset_id": str, "actor": str}.
    """
    src = SourceConfig.from_dict(config)
    adapter = ADAPTER_REGISTRY[src.actor]
    actor_input = adapter.build_input(src.start_url)
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
    payload = {"actor": str, "items": [...], "city": str}
    Returns a list of Listing.to_dict() dicts (JSON-serialisable for Durable).
    """
    actor = payload["actor"]
    items: list[dict] = payload["items"]
    city: str = payload["city"]

    adapter = ADAPTER_REGISTRY[actor]
    results = []
    for raw in items:
        try:
            listing = adapter.normalize(raw, city)
            if listing is not None:
                results.append(listing.to_dict())
        except Exception:
            logger.exception("normalize error for item %s", raw.get("id") or raw.get("adid"))
    logger.info("Normalized %d/%d items for actor=%s city=%s", len(results), len(items), actor, city)
    return results
