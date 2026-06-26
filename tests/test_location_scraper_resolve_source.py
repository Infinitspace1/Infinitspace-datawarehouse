"""Tests for resolve_source activity — no external deps required."""
from __future__ import annotations

import pytest

from shared.location_scraper.activities.resolve import resolve_source
from shared.location_scraper.config import (
    IDEALISTA_ACTOR_ID,
    IMMOBILIENSCOUT_ACTOR_ID,
    LOOPNET_ACTOR_ID,
    OTODOM_ACTOR_ID,
)


class TestResolveSource:
    def test_madrid_no_shape(self):
        cfg = resolve_source("madrid", None, "run-001")
        assert cfg.city == "madrid"
        assert cfg.country == "spain"
        assert cfg.country_code == "es"
        assert cfg.actor == "idealista"
        assert cfg.actor_id == IDEALISTA_ACTOR_ID
        assert "madrid-madrid" in cfg.start_url
        assert "alquiler-oficinas" in cfg.start_url
        assert "con-metros-cuadrados-mas-de_1500" in cfg.start_url
        assert cfg.run_id == "run-001"

    def test_madrid_with_shape(self):
        shape = "ABCDE12345"
        cfg = resolve_source("madrid", shape, "run-002")
        assert "areas" in cfg.start_url
        assert shape in cfg.start_url

    def test_barcelona(self):
        cfg = resolve_source("barcelona", None, "x")
        assert "barcelona-barcelona" in cfg.start_url

    def test_milan(self):
        cfg = resolve_source("Milan", None, "x")  # case-insensitive
        assert cfg.country == "italy"
        assert cfg.country_code == "it"
        assert "idealista.it" in cfg.start_url
        assert "affitto-uffici" in cfg.start_url

    def test_warsaw(self):
        cfg = resolve_source("warsaw", None, "x")
        assert cfg.country == "poland"
        assert cfg.country_code == "pl"
        assert cfg.actor == "otodom"
        assert cfg.actor_id == OTODOM_ACTOR_ID
        assert "otodom.pl" in cfg.start_url
        assert "areaMin=1000" in cfg.start_url

    def test_warsaw_ignores_shape(self):
        """Otodom does not support polygon search — shape must be ignored."""
        cfg_with = resolve_source("warsaw", "POLYGON", "x")
        cfg_without = resolve_source("warsaw", None, "x")
        assert cfg_with.start_url == cfg_without.start_url

    def test_berlin_immobilienscout(self):
        cfg = resolve_source("berlin", None, "run-de-1")
        assert cfg.country == "germany"
        assert cfg.country_code == "de"
        assert cfg.actor == "immobilienscout"
        assert cfg.actor_id == IMMOBILIENSCOUT_ACTOR_ID
        assert "immobilienscout24.de/Suche/de/berlin/berlin/buero-mieten" in cfg.start_url
        assert "netfloorspace=1500" in cfg.start_url

    def test_frankfurt_immobilienscout(self):
        cfg = resolve_source("frankfurt", None, "run-de-2")
        assert "immobilienscout24.de/Suche/de/hessen/frankfurt-am-main/buero-mieten" in cfg.start_url
        assert cfg.run_id == "run-de-2"

    def test_london_loopnet(self):
        cfg = resolve_source("London", None, "run-uk-1")  # case-insensitive
        assert cfg.city == "london"
        assert cfg.country == "uk"
        assert cfg.country_code == "gb"
        assert cfg.actor == "loopnet"
        assert cfg.actor_id == LOOPNET_ACTOR_ID
        # UK lives on loopnet.co.uk; min-space-size filters on AVAILABLE space
        # so the search only contains qualifying buildings (~383 for London
        # instead of a 500-capped window with 42).
        assert cfg.start_url == (
            "https://www.loopnet.co.uk/search/office-space/"
            "london-england--united-kingdom/for-rent/"
            "?min-space-size=16146"
        )

    def test_london_ignores_shape(self):
        """LoopNet UK uses URL geocoding — shape (Idealista polygon) must be ignored."""
        with_shape = resolve_source("london", "POLYGON", "x")
        without = resolve_source("london", None, "x")
        assert with_shape.start_url == without.start_url

    def test_new_york_loopnet_us(self):
        cfg = resolve_source("New York", None, "run-us-1")  # case-insensitive
        assert cfg.city == "new york"
        assert cfg.country == "us"
        assert cfg.country_code == "us"
        assert cfg.actor == "loopnet"
        assert cfg.actor_id == LOOPNET_ACTOR_ID
        assert cfg.start_url == (
            "https://www.loopnet.com/search/office-space/new-york-ny/for-lease/"
            "?min-space-size=16146"
        )

    def test_san_francisco_loopnet_us(self):
        cfg = resolve_source("san francisco", None, "x")
        assert cfg.country == "us"
        assert cfg.start_url == (
            "https://www.loopnet.com/search/office-space/san-francisco-ca/for-lease/"
            "?min-space-size=16146"
        )

    def test_us_cities_all_resolve(self):
        expected = {
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
            "phoenix": "phoenix-az",
            "atlanta": "atlanta-ga",
        }
        for city, slug in expected.items():
            cfg = resolve_source(city, None, "x")
            assert cfg.actor == "loopnet"
            assert cfg.country == "us"
            assert cfg.start_url == (
                f"https://www.loopnet.com/search/office-space/{slug}/for-lease/"
                "?min-space-size=16146"
            )

    def test_us_loopnet_ignores_shape(self):
        """LoopNet US uses URL geocoding — Idealista polygon shape must be ignored."""
        with_shape = resolve_source("austin", "POLYGON", "x")
        without = resolve_source("austin", None, "x")
        assert with_shape.start_url == without.start_url

    def test_unknown_city_raises(self):
        with pytest.raises(ValueError, match="not recognised"):
            resolve_source("atlantis", None, "x")

    def test_case_insensitive(self):
        cfg1 = resolve_source("MADRID", None, "x")
        cfg2 = resolve_source("madrid", None, "x")
        assert cfg1.start_url == cfg2.start_url

    def test_trailing_whitespace(self):
        cfg = resolve_source("  madrid  ", None, "x")
        assert cfg.city == "madrid"

    def test_to_dict_roundtrip(self):
        from shared.location_scraper.models import SourceConfig
        cfg = resolve_source("warsaw", None, "run-99", unlimited_items=True)
        d = cfg.to_dict()
        restored = SourceConfig.from_dict(d)
        assert restored.city == cfg.city
        assert restored.actor_id == cfg.actor_id
        assert restored.start_url == cfg.start_url
        assert restored.unlimited_items is True
        assert restored.listing_urls is None

    def test_listing_urls_roundtrip(self):
        from shared.location_scraper.models import SourceConfig
        cfg = resolve_source("london", None, "run-uk-2")
        cfg.listing_urls = ["https://www.loopnet.co.uk/listing/x-london/123/"]
        restored = SourceConfig.from_dict(cfg.to_dict())
        assert restored.listing_urls == cfg.listing_urls

    def test_legacy_config_dict_without_listing_urls(self):
        """Queue messages serialized before the listing_urls field must load."""
        from shared.location_scraper.models import SourceConfig
        d = resolve_source("london", None, "x").to_dict()
        d.pop("listing_urls")
        restored = SourceConfig.from_dict(d)
        assert restored.listing_urls is None
