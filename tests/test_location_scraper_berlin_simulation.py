"""
Simulated Germany scraping test (Immobilienscout path).

Goal:
  - emulate a "real" run without calling external services
  - validate resolve -> start/poll/fetch -> normalize -> dedupe -> enrich -> consolidate
  - keep city selection modular through pytest parametrization
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from shared.location_scraper.activities.enrich import (
    consolidate_contacts,
    dedupe_agencies,
    enrich_agency,
)
from shared.location_scraper.activities.resolve import resolve_source
from shared.location_scraper.activities.scrape import (
    check_apify_run,
    fetch_dataset,
    normalize_listings,
    start_apify_run,
)


def _fake_item_for_city(city_name: str) -> dict:
    return {
        "normalized": {
            "listingId": "167325674",
            "url": "https://www.immobilienscout24.de/expose/167325674",
            "address": {
                "city": city_name,
                "latitude": "52.50524",
                "longitude": "13.44052",
                "street": "Muehlenstrasse",
                "houseNumber": "25",
                "zip": "10243",
                "region": "Friedrichshain",
            },
            "area": {"livingSpace": "2068"},
            "price": {"amount": "62040", "currency": "EUR"},
            "contact": {
                "name": "Frau Malina Trockel",
                "company": "Savills Immobilien Beratungs-GmbH - Office Agency",
                "phone": "+49 173 192 42 11",
            },
        },
        "adTargetingParameters": {"obj_rentPerSqM": "30"},
        "header": {"id": "167325674", "publicationState": "active"},
    }

FAKE_GOOGLE_RESULT = [
    {
        "searchQuery": {"term": "Savills Immobilien Beratungs-GmbH real estate official website"},
        "organicResults": [{"url": "https://www.savills.de/"}],
    }
]

FAKE_LUSHA_CONTACTS = [
    {
        "contactId": "lusha-001",
        "fullName": "Anna Schmidt",
        "jobTitle": "Leasing Director",
        "emailAddresses": [{"email": "anna.schmidt@savills.de", "emailConfidence": 0.95}],
        "linkedinUrl": "https://linkedin.com/in/anna-schmidt",
    }
]

FAKE_LUSHA_ENRICHED = {
    "fullName": "Anna Schmidt",
    "jobTitle": "Leasing Director",
    "emailAddresses": [{"email": "anna.schmidt@savills.de", "emailConfidence": 0.95}],
    "linkedinUrl": "https://linkedin.com/in/anna-schmidt",
}


@pytest.mark.parametrize(
    ("city", "city_label", "url_fragment"),
    [
        ("berlin", "Berlin", "immobilienscout24.de/Suche/de/berlin/berlin/buero-mieten"),
        ("munich", "Muenchen", "immobilienscout24.de/Suche/de/bayern/muenchen/buero-mieten"),
        ("hamburg", "Hamburg", "immobilienscout24.de/Suche/de/hamburg/hamburg/buero-mieten"),
        ("cologne", "Koeln", "immobilienscout24.de/Suche/de/nordrhein-westfalen/koeln/buero-mieten"),
        ("frankfurt", "Frankfurt", "immobilienscout24.de/Suche/de/hessen/frankfurt-am-main/buero-mieten"),
        ("dusseldorf", "Duesseldorf", "immobilienscout24.de/Suche/de/nordrhein-westfalen/duesseldorf/buero-mieten"),
        ("stuttgart", "Stuttgart", "immobilienscout24.de/Suche/de/baden-wuerttemberg/stuttgart/buero-mieten"),
    ],
)
def test_simulated_germany_scrape_pipeline(city: str, city_label: str, url_fragment: str):
    run_id = f"{city}-sim-001"

    # 1) Resolve source configuration for requested city.
    cfg = resolve_source(city, None, run_id)
    assert cfg.actor == "immobilienscout"
    assert url_fragment in cfg.start_url
    assert "netfloorspace=1500" in cfg.start_url

    with (
        patch("shared.location_scraper.clients.apify.start_run", return_value={"run_id": "apify-1", "dataset_id": "ds-1"}),
        patch("shared.location_scraper.clients.apify.get_run_status", return_value={"finished": True, "succeeded": True, "status": "SUCCEEDED"}),
        patch("shared.location_scraper.clients.apify.fetch_dataset", return_value=[_fake_item_for_city(city_label)]),
        patch("shared.location_scraper.activities.enrich.apify_client.run_sync", return_value=FAKE_GOOGLE_RESULT),
        patch("shared.location_scraper.clients.lusha.search_contacts_by_domain", return_value=FAKE_LUSHA_CONTACTS),
        patch("shared.location_scraper.clients.lusha.enrich_contact", return_value=FAKE_LUSHA_ENRICHED),
    ):
        # 2) Simulate Apify run lifecycle.
        run_info = start_apify_run(cfg.to_dict())
        assert run_info["run_id"] == "apify-1"
        assert run_info["dataset_id"] == "ds-1"
        assert check_apify_run(run_info)["succeeded"] is True
        raw_items = fetch_dataset(run_info)
        assert len(raw_items) == 1

        # 3) Normalize city listings.
        listings = normalize_listings({"actor": "immobilienscout", "items": raw_items, "city": city})
        assert len(listings) == 1
        listing = listings[0]
        assert listing["source"] == "immobilienscout"
        assert listing["city"] == city_label.lower()
        assert listing["surface_m2"] == 2068.0
        assert listing["surface_unit"] == "m2"
        assert listing["price_per_m2"] == 30.0

        # 4) Simulate agency/contact enrichment flow.
        agencies = dedupe_agencies(listings)
        assert len(agencies) == 1
        enriched = [
            enrich_agency({"agency": agency, "country": "germany", "country_code": "de"})
            for agency in agencies
        ]
        bundles = consolidate_contacts(enriched)

        assert len(bundles) == 1
        # Current dedupe rule uses contact_name for non-otodom sources.
        assert bundles[0]["agency_name"] == "Frau Malina Trockel"
        assert bundles[0]["email_1"] == "anna.schmidt@savills.de"
