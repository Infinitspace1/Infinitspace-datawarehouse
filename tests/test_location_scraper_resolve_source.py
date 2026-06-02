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
        assert cfg.start_url == (
            "https://www.loopnet.com/search/office-properties/"
            "london-england--united-kingdom/for-rent/"
        )

    def test_london_ignores_shape(self):
        """LoopNet UK uses URL geocoding — shape (Idealista polygon) must be ignored."""
        with_shape = resolve_source("london", "POLYGON", "x")
        without = resolve_source("london", None, "x")
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
