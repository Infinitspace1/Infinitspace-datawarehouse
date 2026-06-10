"""Tests for the LoopNet listing-URL enumeration activity (no network)."""
from __future__ import annotations

from unittest.mock import patch

from shared.location_scraper.activities import enumerate_loopnet


def _items(*pairs: tuple[str, str]) -> list[dict]:
    return [{"id": lid, "url": url} for lid, url in pairs]


class TestPageUrls:
    def test_appends_page_query_param(self):
        urls = enumerate_loopnet._page_urls("https://x/for-rent/?min-space-size=16146", 3)
        assert urls == [
            "https://x/for-rent/?min-space-size=16146",
            "https://x/for-rent/?min-space-size=16146&page=2",
            "https://x/for-rent/?min-space-size=16146&page=3",
        ]

    def test_no_existing_query(self):
        urls = enumerate_loopnet._page_urls("https://x/for-lease/", 2)
        assert urls == ["https://x/for-lease/", "https://x/for-lease/?page=2"]


class TestEnumerateListingUrls:
    def test_dedupes_by_listing_id(self):
        items = _items(
            ("34548634", "https://www.loopnet.co.uk/Listing/34548634/"),
            ("34501482", "https://www.loopnet.co.uk/Listing/34501482/"),
            # over-range pages re-serve earlier content -> duplicate ids
            ("34548634", "https://www.loopnet.co.uk/Listing/34548634/"),
        )
        with patch.object(enumerate_loopnet.apify_client, "run_sync", return_value=items):
            urls = enumerate_loopnet.enumerate_listing_urls("https://x/for-rent/?min-space-size=16146")
        assert sorted(urls) == [
            "https://www.loopnet.co.uk/Listing/34501482/",
            "https://www.loopnet.co.uk/Listing/34548634/",
        ]

    def test_empty_scrape_returns_empty_list(self):
        with patch.object(enumerate_loopnet.apify_client, "run_sync", return_value=[]):
            assert enumerate_loopnet.enumerate_listing_urls("https://x/for-rent/") == []

    def test_items_without_id_or_url_ignored(self):
        items = [
            {"id": "", "url": "https://x/Listing/1/"},
            {"id": "2", "url": None},
            {"id": "3", "url": "https://www.loopnet.com/Listing/3/"},
            "not-a-dict",
        ]
        with patch.object(enumerate_loopnet.apify_client, "run_sync", return_value=items):
            urls = enumerate_loopnet.enumerate_listing_urls("https://x/for-lease/")
        assert urls == ["https://www.loopnet.com/Listing/3/"]

    def test_run_input_shape(self):
        """The enumeration must run in URL mode, one page per URL, no details."""
        captured = {}

        def fake_run_sync(actor_id, run_input, limit):
            captured["actor_id"] = actor_id
            captured["run_input"] = run_input
            return []

        with patch.object(enumerate_loopnet.apify_client, "run_sync", side_effect=fake_run_sync):
            enumerate_loopnet.enumerate_listing_urls(
                "https://x/for-rent/?min-space-size=16146", max_pages=4
            )
        ri = captured["run_input"]
        assert captured["actor_id"] == "abotapi/loopnet-scraper"
        assert ri["mode"] == "url"
        assert ri["maxPages"] == 1
        assert ri["fetchDetails"] is False
        assert len(ri["urls"]) == 4
        assert ri["urls"][3].endswith("&page=4")
