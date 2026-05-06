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
GOOGLE_SEARCH_ACTOR_ID = "nFJndFXA5zjCTuudP"

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


def get_country_code_for_city(city: str | None) -> str | None:
    """Return the configured ISO country code for a supported scraper city."""
    city_lower = (city or "").lower().strip()
    if not city_lower:
        return None
    for cfg in COUNTRY_CONFIG.values():
        if city_lower in cfg["cities"]:
            return str(cfg["country_code"]).upper()
    return None
