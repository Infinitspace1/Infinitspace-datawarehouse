"""
Ported from the n8n "Config" code node.

COUNTRY_CONFIG maps each supported city to its scraping parameters.
Adding a new city: add it under the appropriate country's "cities" dict.
Adding a new country/source: add a new top-level key and implement a matching adapter.
"""
from __future__ import annotations

import os

# Apify actor IDs
IDEALISTA_ACTOR_ID = "OTe82JNUGa93aVcRc"
OTODOM_ACTOR_ID = "ir34sMIv8mrbL0ojO"
IMMOBILIENSCOUT_ACTOR_ID = "ciTdHfgOkkwfEzTE9"
# memo23 pay-per-event LoopNet actor (US + UK). The $31/mo flat-rate twin
# (RuOxoBM1bnc5pQ3TJ) is deliberately NOT used.
LOOPNET_ACTOR_ID = "0ZCQONxB3BdyOzrbD"
# Enumeration actor for the filtered LoopNet search pages. memo23 ignores URL
# filters entirely, plain HTTP and Apify's generic browser scrapers are
# Akamai-challenge-blocked; abotapi passes Akamai and preserves URL query
# params (pagination is driven via `?page=N` — its page-PATH handling is
# broken). Used in URL mode, fetchDetails off — memo23 does the detail work.
LOOPNET_ENUM_ACTOR_ID = "abotapi/loopnet-scraper"
GOOGLE_SEARCH_ACTOR_ID = "nFJndFXA5zjCTuudP"

# LoopNet available-space URL filter, in square feet (= the 1500 m² floor the
# adapter enforces; 1500 / 0.092903 ≈ 16146).
MIN_SPACE_SIZE_SQFT = 16146

# Ceiling sent on "unlimited" LoopNet runs. Leaving `maxItems` ABSENT makes the
# memo23 actor stop a search early at a small internal default (confirmed with
# the actor dev + measured 2026-07-23: Los Angeles returned 67 items with the
# key omitted vs 147 with an explicit cap, same build, same URL).
LOOPNET_UNLIMITED_MAX_ITEMS = 1000

# Per-run budget for the actor's paid unblocker. Only the (legacy) listing-URL
# detail path uses it — the paginated search path never touches the unblocker.
LOOPNET_MAX_UNBLOCKER_REQUESTS = 2000

# How many result pages of the space-available-filtered LoopNet search are
# fetched per city.
#
# The memo23 actor serves ONE search page per start URL from LoopNet's free
# mobile API (`_dataSource: free-mobile-api`) — the stage that still works,
# unlike the per-listing detail fetch that 403s and depends on a throttled paid
# unblocker. Passing `?page=N` URLs walks the whole result set through that free
# stage: London went from 48 items (single page URL) to 320 distinct buildings
# over 12 pages, measured 2026-08-19.
#
# LoopNet serves ~25 placards per page and re-serves earlier pages past the end
# of a search, so overshooting only costs duplicate results (deduped on
# propertyId downstream) — the actor bills per result, so keep this close to the
# real depth of the densest city.
LOOPNET_SEARCH_PAGES = 15

COUNTRY_CONFIG: dict[str, dict] = {
    "spain": {
        "domain": "https://www.idealista.com",
        "language": "en",
        "property_path": "alquiler-oficinas",
        "filter_suffix": "con-metros-cuadrados-mas-de_1500",
        "actor": "idealista",
        "actor_id": IDEALISTA_ACTOR_ID,
        "country_code": "es",
        "cities": {
            "madrid": "madrid-madrid",
            "barcelona": "barcelona-barcelona",
            "seville": "sevilla-sevilla",
            "valencia": "valencia-valencia",
        },
    },
    "italy": {
        "domain": "https://www.idealista.it",
        "language": "en",
        "property_path": "affitto-uffici",
        "filter_suffix": "con-dimensione_1500",
        "actor": "idealista",
        "actor_id": IDEALISTA_ACTOR_ID,
        "country_code": "it",
        "cities": {
            "milan": "milano-milano",
        },
    },
    "poland": {
        "domain": "https://www.otodom.pl",
        "language": "pl",
        "property_path": "wyniki/wynajem/lokal,biuro",
        "filter_suffix": "areaMin=1000",
        "actor": "otodom",
        "actor_id": OTODOM_ACTOR_ID,
        "country_code": "pl",
        # Otodom does not support polygon-based (shape) searches via URL.
        "cities": {
            "warsaw": "mazowieckie/warszawa/warszawa/warszawa",
        },
    },
    "germany": {
        "domain": "https://www.immobilienscout24.de",
        "language": "de",
        "property_path": "buero-mieten",
        "filter_suffix": "netfloorspace=1500",
        "actor": "immobilienscout",
        "actor_id": IMMOBILIENSCOUT_ACTOR_ID,
        "country_code": "de",
        # ImmoScout24 rental office market (>=1500 sqm filter in URL).
        "cities": {
            "berlin": "berlin/berlin",
            "munich": "bayern/muenchen",
            "hamburg": "hamburg/hamburg",
            "cologne": "nordrhein-westfalen/koeln",
            "frankfurt": "hessen/frankfurt-am-main",
            "dusseldorf": "nordrhein-westfalen/duesseldorf",
            "stuttgart": "baden-wuerttemberg/stuttgart",
        },
    },
    "uk": {
        # LoopNet UK moved to its own domain — the old
        # `www.loopnet.com/search/office-properties/...` UK route now 404s.
        "domain": "https://www.loopnet.co.uk",
        "language": "en",
        "property_path": "office-space",
        "filter_suffix": "for-rent",
        "actor": "loopnet",
        "actor_id": LOOPNET_ACTOR_ID,
        "country_code": "gb",
        # `min-space-size` (sq ft) filters on AVAILABLE space server-side.
        # Without it the SRP is capped at ~500 results dominated by small
        # spaces, and most >=1500 m² buildings are invisible (London: 42 of
        # 383). 16146 sq ft = the 1500 m² floor enforced in the adapter.
        "min_space_size_sqft": MIN_SPACE_SIZE_SQFT,
        # City slug MUST include the region + `--united-kingdom`, e.g.
        # `london-england--united-kingdom`.
        "cities": {
            "london": "london-england--united-kingdom",
        },
    },
    "us": {
        "domain": "https://www.loopnet.com",
        "language": "en",
        # US LoopNet uses `for-lease` where UK uses `for-rent`. Same actor.
        "property_path": "office-space",
        "filter_suffix": "for-lease",
        "actor": "loopnet",
        "actor_id": LOOPNET_ACTOR_ID,
        "country_code": "us",
        # Same available-space URL filter as UK (verified on loopnet.com:
        # New York unfiltered is capped while ?min-space-size=16146 -> 455).
        "min_space_size_sqft": MIN_SPACE_SIZE_SQFT,
        # LoopNet US office listings. City slug is `{city}-{state-abbrev}`,
        # e.g. `new-york-ny`.
        "cities": {
            "new york": "new-york-ny",
            "san francisco": "san-francisco-ca",
            "palo alto": "palo-alto-ca",
            "los angeles": "los-angeles-ca",
            "austin": "austin-tx",
            "seattle": "seattle-wa",
            "redwood city": "redwood-city-ca",
            "san mateo": "san-mateo-ca",
            "san bruno": "san-bruno-ca",
            "cupertino": "cupertino-ca",
        },
    },
    "canada": {
        # LoopNet Canada lives on its own domain (like the UK). Same actor.
        "domain": "https://www.loopnet.ca",
        "language": "en",
        # Canada uses the US-style `for-lease` suffix, not the UK `for-rent`.
        "property_path": "office-space",
        "filter_suffix": "for-lease",
        "actor": "loopnet",
        "actor_id": LOOPNET_ACTOR_ID,
        "country_code": "ca",
        # Same available-space URL filter as UK/US (verified on loopnet.ca:
        # Toronto office ?min-space-size=16146 -> 189 results).
        "min_space_size_sqft": MIN_SPACE_SIZE_SQFT,
        # City slug MUST include the province + `--canada` (UK convention),
        # e.g. `toronto-on--canada` — the bare `toronto-on` 404s.
        "cities": {
            "toronto": "toronto-on--canada",
        },
    },
}

# Job titles passed to Lusha's contact search (from n8n "Extract Contact" node)
LUSHA_JOB_TITLES: list[str] = [
    "Head of Office Leasing",
    "Leasing Director",
    "Leasing Manager",
    "Leasing Officer",
    "Asset Manager",
    "Head of Asset Management",
    "Property Manager",
    "Senior Consultant",
    "Director of Real Estate",
    "Commercial Real Estate Director",
    "Office Leasing Director",
    "Head of Offices",
    "Real Estate Agent",
    "Real Estate Advisor",
    "Real Estate Consultant",
    "Property Consultant",
    "Commercial Real Estate Agent",
    "Commercial Real Estate Broker",
    "Commercial Real Estate Advisor",
    "Office Consultant",
    "Office Broker",
    "Broker",
    "Agent nieruchomości",
    "Doradca ds. nieruchomości",
    "Doradca nieruchomości",
    "Pośrednik nieruchomości",
    "Konsultant nieruchomości",
]

# Polish tokens that indicate a name is a company, not a person
POLISH_COMPANY_KEYWORDS: tuple[str, ...] = (
    "biuro",
    "kancelaria",
    "group",
    "property",
    "agency",
    "estate",
    "sp.",
    "sa",
    "s.a.",
    "spółka",
    "nieruchomości",
)

OTODOM_MAX_INDIVIDUAL_CANDIDATES_PER_AGENCY = 5
LUSHA_MAX_REVEALS_PER_AGENCY = 5


def get_actor_max_items(*, default: int) -> int:
    """
    Resolve common max-items cap for all scraper actors.

    Env override:
      LOCATION_SCRAPER_MAX_ITEMS=<int>
    """
    raw = (os.environ.get("LOCATION_SCRAPER_MAX_ITEMS") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def get_loopnet_listing_run_memory_mbytes(listing_count: int) -> int:
    """Apify memory to request for a LoopNet listing-URL run.

    The actor's 512 MB default is OOM-killed (exit 137) once the URL list gets
    large — measured 2026-07-23: 460 New York URLs died at exactly 512 MB while
    lists of ~40 finished fine. The actor bills per result, not per compute
    unit, so over-allocating costs nothing.

    Env override: LOOPNET_LISTING_RUN_MEMORY_MB=<int> (fixed value for all runs)
    """
    raw = (os.environ.get("LOOPNET_LISTING_RUN_MEMORY_MB") or "").strip()
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    # Apify only accepts powers of two.
    if listing_count > 400:
        return 8192
    if listing_count > 200:
        return 4096
    if listing_count > 100:
        return 2048
    return 1024


def get_loopnet_unlimited_max_items() -> int:
    """`maxItems` for uncapped LoopNet runs.

    Env override: LOOPNET_MAX_ITEMS=<int>
    """
    raw = (os.environ.get("LOOPNET_MAX_ITEMS") or "").strip()
    if not raw:
        return LOOPNET_UNLIMITED_MAX_ITEMS
    try:
        value = int(raw)
    except ValueError:
        return LOOPNET_UNLIMITED_MAX_ITEMS
    return value if value > 0 else LOOPNET_UNLIMITED_MAX_ITEMS


def get_loopnet_max_unblocker_requests() -> int:
    """Per-run unblocker budget for LoopNet runs.

    Env override: LOOPNET_MAX_UNBLOCKER_REQUESTS=<int>
    """
    raw = (os.environ.get("LOOPNET_MAX_UNBLOCKER_REQUESTS") or "").strip()
    if not raw:
        return LOOPNET_MAX_UNBLOCKER_REQUESTS
    try:
        value = int(raw)
    except ValueError:
        return LOOPNET_MAX_UNBLOCKER_REQUESTS
    return value if value > 0 else LOOPNET_MAX_UNBLOCKER_REQUESTS


def get_loopnet_search_pages() -> int:
    """How many filtered-search result pages to fetch per LoopNet city.

    Env override: LOOPNET_SEARCH_PAGES=<int>
    """
    raw = (os.environ.get("LOOPNET_SEARCH_PAGES") or "").strip()
    if not raw:
        return LOOPNET_SEARCH_PAGES
    try:
        value = int(raw)
    except ValueError:
        return LOOPNET_SEARCH_PAGES
    return value if value > 0 else LOOPNET_SEARCH_PAGES


def get_country_code_for_city(city: str | None) -> str | None:
    """Return the configured ISO country code for a supported scraper city."""
    city_lower = (city or "").lower().strip()
    if not city_lower:
        return None
    for cfg in COUNTRY_CONFIG.values():
        if city_lower in cfg["cities"]:
            return str(cfg["country_code"]).upper()
    return None
