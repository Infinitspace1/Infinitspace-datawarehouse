"""
Activity: resolve_source

Ports the n8n "Config" code node.
Maps {city, shape, run_id} → SourceConfig with actor_id and start_url.
"""
from __future__ import annotations

from shared.location_scraper.config import COUNTRY_CONFIG
from shared.location_scraper.models import SourceConfig


def resolve_source(
    city: str,
    shape: str | None,
    run_id: str,
    unlimited_items: bool = False,
) -> SourceConfig:
    """
    Resolve city to a SourceConfig.
    Raises ValueError for unrecognised cities.
    """
    city_lower = city.lower().strip()

    for country, cfg in COUNTRY_CONFIG.items():
        city_slug = cfg["cities"].get(city_lower)
        if city_slug is None:
            continue

        actor = cfg["actor"]
        domain = cfg["domain"]
        language = cfg["language"]
        property_path = cfg["property_path"]
        filter_suffix = cfg["filter_suffix"]

        if actor == "loopnet":
            # LoopNet: /search/office-space/{city_slug}/{for-rent|for-lease}/
            # `min-space-size` (sq ft) filters on AVAILABLE space server-side —
            # without it the search is capped at ~500 results dominated by
            # small spaces and most qualifying buildings are invisible.
            start_url = f"{domain}/search/{property_path}/{city_slug}/{filter_suffix}/"
            min_space = cfg.get("min_space_size_sqft")
            if min_space:
                start_url += f"?min-space-size={min_space}"
        elif actor == "otodom":
            # Otodom does not support polygon search via URL.
            start_url = f"{domain}/{language}/{property_path}/{city_slug}?{filter_suffix}"
        elif actor == "immobilienscout":
            # Immobilienscout expects /Suche/{lang}/{region}/{city}/{property}?query.
            start_url = f"{domain}/Suche/{language}/{city_slug}/{property_path}?{filter_suffix}"
        elif shape:
            # Idealista shape/polygon search — double-encoded per original n8n logic.
            start_url = (
                f"{domain}/areas/{property_path}/{filter_suffix}"
                f"/?shape=%28%28{shape}%29%29"
            )
        else:
            start_url = f"{domain}/{language}/{property_path}/{city_slug}/{filter_suffix}/"

        return SourceConfig(
            city=city_lower,
            country=country,
            country_code=cfg["country_code"],
            start_url=start_url,
            actor=actor,
            actor_id=cfg["actor_id"],
            run_id=run_id,
            unlimited_items=unlimited_items,
        )

    available = sorted(
        city for cfg in COUNTRY_CONFIG.values() for city in cfg["cities"]
    )
    raise ValueError(
        f"City '{city}' is not recognised. Available cities: {', '.join(available)}"
    )
