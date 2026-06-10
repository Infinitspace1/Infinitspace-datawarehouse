"""
Activity: enumerate_loopnet_listing_urls

Walks the space-available-filtered LoopNet search pages and returns the
individual listing-detail URLs found there.

Why this exists (validated empirically 2026-06-10, see
scripts/python_scripts/test_loopnet_*.py):
  - The memo23 LoopNet actor (the scraper that returns broker emails) never
    loads the start URL — it geocodes the city slug and queries LoopNet's
    internal mobile API with a fixed-size bounding box, hard-capped at 500
    items and dominated by small spaces. All filter inputs (URL query params,
    BuildingSizeRangeMin, moreResults) are ignored. For London that surfaces
    only 42 of the 383 buildings with >=1500 m² available.
  - LoopNet's own `?min-space-size=<sqft>` URL filter is the ONLY full view of
    the qualifying market (it binds SpaceAvailableRangeMin — available space,
    not total building size).
  - LoopNet is behind Akamai: plain HTTP and Apify's generic browser scrapers
    are challenge-blocked. The abotapi/loopnet-scraper actor passes Akamai
    with warmed sessions and its URL mode preserves the query string — but it
    drops the page PATH segment (`/3/` is re-served as page 1). LoopNet also
    accepts the page as a `?page=N` QUERY param (browser-verified identical
    to the path form), which abotapi passes through untouched. So one
    abotapi run with `...?min-space-size=...&page=N` URLs (one per page,
    maxPages=1) enumerates the full filtered search.

The enumerated URLs are then fed to the memo23 actor as startUrls — it
accepts listing-detail URLs and returns its usual rich payload including
brokerEmail, so the rest of the pipeline (adapter, persist, globe) is
unchanged.
"""
from __future__ import annotations

import logging
import os

from shared.location_scraper.clients import apify as apify_client
from shared.location_scraper.config import LOOPNET_ENUM_ACTOR_ID

logger = logging.getLogger(__name__)

# Safety ceiling on filtered search pages fetched per city. LoopNet serves
# ~25 placards per page and caps any search at ~20-25 pages; pages past the
# end re-serve earlier content and are deduped away (small wasted cost, so
# keep the ceiling close to the real maximum).
_DEFAULT_MAX_PAGES = 25


def _max_pages() -> int:
    raw = (os.environ.get("LOOPNET_ENUM_MAX_PAGES") or "").strip()
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_MAX_PAGES
    return value if value > 0 else _DEFAULT_MAX_PAGES


def _page_urls(start_url: str, pages: int) -> list[str]:
    """Build one URL per search page using the `page` QUERY param.

    The path form (`/for-rent/3/?...`) cannot be used: the enumeration actor
    rebuilds URLs and drops the page path segment, while query params are
    passed through untouched.
    """
    sep = "&" if "?" in start_url else "?"
    return [start_url] + [f"{start_url}{sep}page={n}" for n in range(2, pages + 1)]


def enumerate_listing_urls(start_url: str, max_pages: int | None = None) -> list[str]:
    """Return the deduped listing-detail URLs from a filtered LoopNet search.

    Dedupe key is the listing id (pages past the end of the search re-serve
    earlier pages). Returns [] when the scrape yields nothing — the
    orchestrator then falls back to the legacy broad-search run.
    """
    pages_cap = max_pages or _max_pages()
    run_input = {
        "mode": "url",
        "urls": _page_urls(start_url, pages_cap),
        "fetchDetails": False,  # ids + listing URLs only; memo23 does details
        "maxListings": 0,  # unlimited (the page list bounds the volume)
        "maxPages": 1,  # each URL is exactly one page
        "proxy": {"useApifyProxy": True, "apifyProxyGroups": ["RESIDENTIAL"]},
    }
    items = apify_client.run_sync(
        LOOPNET_ENUM_ACTOR_ID, run_input, limit=pages_cap * 30
    )

    by_id: dict[str, str] = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        listing_id = str(item.get("id") or "").strip()
        url = item.get("url")
        if listing_id and url:
            by_id.setdefault(listing_id, str(url))

    urls = list(by_id.values())
    logger.info(
        "LoopNet enumeration: %d distinct listings from %d items / %d page urls (start_url=%s)",
        len(urls),
        len(items or []),
        pages_cap,
        start_url,
    )
    return urls
