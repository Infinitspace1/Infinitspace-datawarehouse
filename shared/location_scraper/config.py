"""
Ported from the n8n "Config" code node.

COUNTRY_CONFIG maps each supported city to its scraping parameters.
Adding a new city: add it under the appropriate country's "cities" dict.
Adding a new country/source: add a new top-level key and implement a matching adapter.
"""
from __future__ import annotations

# Apify actor IDs
IDEALISTA_ACTOR_ID = "OTe82JNUGa93aVcRc"
OTODOM_ACTOR_ID = "ir34sMIv8mrbL0ojO"
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
]

# Polish tokens that indicate a name is a company, not a person
POLISH_COMPANY_KEYWORDS: tuple[str, ...] = (
    "biuro",
    "kancelaria",
    "group",
    "sp.",
    "sa",
    "s.a.",
    "spółka",
    "nieruchomości",
)
