"""
Central registry of source adapters.
Adding a new source: implement SourceAdapter, import it here, add one line.
"""
from shared.location_scraper.adapters.idealista import IdealistaAdapter
from shared.location_scraper.adapters.immobilienscout import ImmobilienscoutAdapter
from shared.location_scraper.adapters.otodom import OtodomAdapter

ADAPTER_REGISTRY: dict = {
    "idealista": IdealistaAdapter(),
    "otodom": OtodomAdapter(),
    "immobilienscout": ImmobilienscoutAdapter(),
}
