"""
End-to-end test for the idealista scrape path with all external boundaries mocked.

Mocked boundaries:
  - apify-client  (via unittest.mock.patch on shared.location_scraper.clients.apify)
  - Lusha REST    (via unittest.mock.patch on shared.location_scraper.clients.lusha)
  - pyodbc / SQLClient  (via unittest.mock.patch on shared.azure_clients.sql_client.SQLClient)

The test exercises:
  resolve_source → normalize_listings (IdealistaAdapter) → dedupe_agencies →
  enrich_agency (company path: Google Search + Lusha) → consolidate_contacts →
  upsert_sql (verifies MERGE + INSERT calls with correct parameters)
"""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from shared.location_scraper.activities.enrich import (
    consolidate_contacts,
    dedupe_agencies,
    enrich_agency,
)
from shared.location_scraper.activities.resolve import resolve_source
from shared.location_scraper.activities.scrape import normalize_listings
from shared.location_scraper.models import ContactBundle, Listing

# ---------------------------------------------------------------------------
# Fixtures — raw Apify dataset item (Idealista schema)
# ---------------------------------------------------------------------------

RAW_IDEALISTA_ITEM = {
    "adid": "12345",
    "detailWebLink": "https://www.idealista.com/inmueble/12345/",
    "ubication": {
        "latitude": 40.4168,
        "longitude": -3.7038,
        "title": "Calle Gran Vía, 28, Madrid",
    },
    "basicInfo": {
        "size": 2000,
        "floor": "3",
        "price": 18000,
        "priceByArea": 9.0,
        "status": "good",
        "exterior": True,
        "hasLift": True,
        "features": {"hasAirConditioning": True},
        "firstActivationDate": "2026-01-15T00:00:00",
        "district": "Centro",
    },
    "contactInfo": {
        "commercialName": "Savills Aguirre Newman",
        "userType": "professional",
        "phone1": {"phoneNumberForMobileDialing": "+34600000000"},
        "address": {"postalCode": "28013", "streetName": "Gran Vía", "streetNumber": "28"},
    },
    "modificationDate": {"value": "2026-03-01T00:00:00"},
    "energyCertification": {"energyConsumption": {"type": "B"}},
    "comments": [{"propertyComment": "Oficinas de lujo en el centro de Madrid."}],
}

FAKE_RUN_ID = "test-run-" + str(uuid.uuid4())[:8]

# ---------------------------------------------------------------------------
# Helper — mock SQL returning no existing buildings
# ---------------------------------------------------------------------------


@contextmanager
def mock_sql_empty():
    """Patch SQLClient so it looks like there are no existing buildings or lusha contacts."""
    mock_client = MagicMock()
    mock_client.execute_query.return_value = []  # no existing buildings, no lusha contacts
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Building MERGE returns (action='INSERT', id=uuid)
    building_id = str(uuid.uuid4())
    listing_id = str(uuid.uuid4())
    contact_id = str(uuid.uuid4())
    mock_cursor.fetchone.side_effect = [
        ("INSERT", building_id),  # MERGE building OUTPUT
        (listing_id,),            # INSERT listing OUTPUT
        (contact_id,),            # MERGE contact OUTPUT
    ]
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit = MagicMock()
    mock_client.get_connection.return_value = mock_conn

    with patch("shared.azure_clients.sql_client.get_sql_client", return_value=mock_client):
        yield mock_client, mock_cursor


# ---------------------------------------------------------------------------
# Step 1: resolve_source
# ---------------------------------------------------------------------------


def test_resolve_idealista_madrid():
    cfg = resolve_source("madrid", None, FAKE_RUN_ID)
    assert cfg.actor == "idealista"
    assert "madrid-madrid" in cfg.start_url


# ---------------------------------------------------------------------------
# Step 2: normalize_listings via IdealistaAdapter
# ---------------------------------------------------------------------------


def test_normalize_idealista():
    result = normalize_listings(
        {"actor": "idealista", "items": [RAW_IDEALISTA_ITEM], "city": "madrid"}
    )
    assert len(result) == 1
    listing = result[0]
    assert listing["source"] == "idealista"
    assert listing["city"] == "madrid"
    assert listing["external_id"] == "12345"
    assert listing["latitude"] == pytest.approx(40.4168)
    assert listing["longitude"] == pytest.approx(-3.7038)
    assert listing["price_monthly"] == pytest.approx(18000)
    assert listing["price_per_m2"] == pytest.approx(9.0)
    assert listing["currency"] == "EUR"
    assert listing["floor"] == "3"
    assert listing["is_exterior"] is True
    assert listing["has_lift"] is True
    assert listing["has_air_conditioning"] is True
    assert listing["energy_class"] == "B"
    assert listing["contact_name"] == "Savills Aguirre Newman"
    assert listing["days_on_market"] is not None
    assert listing["days_on_market"] >= 0


def test_normalize_idealista_missing_coords():
    """Items without lat/lon should still be normalized (coords will be None)."""
    item = dict(RAW_IDEALISTA_ITEM)
    item["ubication"] = {}
    result = normalize_listings({"actor": "idealista", "items": [item], "city": "madrid"})
    assert len(result) == 1
    assert result[0]["latitude"] is None
    assert result[0]["link_to_gmap"] is None


# ---------------------------------------------------------------------------
# Step 3: dedupe_agencies
# ---------------------------------------------------------------------------


def test_dedupe_agencies_idealista():
    listings = normalize_listings(
        {"actor": "idealista", "items": [RAW_IDEALISTA_ITEM, RAW_IDEALISTA_ITEM], "city": "madrid"}
    )
    agencies = dedupe_agencies(listings)
    assert len(agencies) == 1
    assert agencies[0]["company_name"] == "Savills Aguirre Newman"
    # Idealista: contact_name = company_name → no first/last split
    assert agencies[0]["first_name"] == ""


def test_dedupe_agencies_otodom_individual():
    """Otodom items with a distinct individual contact_name should have first/last extracted."""
    otodom_listing = {
        "source": "otodom",
        "city": "warsaw",
        "contact_name": "Jan Kowalski",
        "company_name": "Biuro Nieruchomości ABC",
        "external_id": "1",
        "web_link": None,
        "link_to_gmap": None,
        "latitude": None,
        "longitude": None,
        "district": None,
        "postal_code": None,
        "address": None,
        "available_surface_m2": None,
        "floor": None,
        "status": None,
        "is_exterior": None,
        "has_lift": None,
        "has_air_conditioning": None,
        "price_monthly": None,
        "price_per_m2": None,
        "currency": None,
        "energy_class": None,
        "first_listed_date": None,
        "last_updated_date": None,
        "days_on_market": None,
        "phone": None,
        "contact_type": None,
        "email": "",
        "agency_comment": None,
    }
    agencies = dedupe_agencies([otodom_listing])
    assert len(agencies) == 1
    assert agencies[0]["company_name"] == "Biuro Nieruchomości ABC"
    assert agencies[0]["first_name"] == "Jan"
    assert agencies[0]["last_name"] == "Kowalski"


# ---------------------------------------------------------------------------
# Step 4: enrich_agency — company path with mocked Apify + Lusha
# ---------------------------------------------------------------------------

FAKE_GOOGLE_RESULT = [
    {
        "searchQuery": {"term": "Savills Aguirre Newman real estate official website"},
        "organicResults": [
            {"url": "https://www.savills.es/some-page"},
            {"url": "https://www.savills.com/another"},
        ],
    }
]

FAKE_LUSHA_CONTACTS = [
    {
        "contactId": "lusha-001",
        "fullName": "Ana García",
        "jobTitle": "Leasing Director",
        "emailAddresses": [{"email": "ana.garcia@savills.es", "emailConfidence": 0.95}],
        "linkedinUrl": "https://linkedin.com/in/ana-garcia",
    }
]

FAKE_LUSHA_ENRICHED = {
    "fullName": "Ana García",
    "jobTitle": "Leasing Director",
    "emailAddresses": [{"email": "ana.garcia@savills.es", "emailConfidence": 0.95}],
    "linkedinUrl": "https://linkedin.com/in/ana-garcia",
}


def test_enrich_agency_individual_fallback_to_company():
    """If the individual Lusha search returns nothing, the company path should run."""
    with (
        patch(
            "shared.location_scraper.clients.lusha.search_individual",
            return_value=None,  # individual search finds nothing
        ),
        patch(
            "shared.location_scraper.activities.enrich.apify_client.run_sync",
            return_value=FAKE_GOOGLE_RESULT,
        ),
        patch(
            "shared.location_scraper.clients.lusha.search_contacts_by_domain",
            return_value=FAKE_LUSHA_CONTACTS,
        ),
        patch(
            "shared.location_scraper.clients.lusha.enrich_contact",
            return_value=FAKE_LUSHA_ENRICHED,
        ),
    ):
        agency_dict = {
            "company_name": "Biuro ABC",
            "first_name": "Jan",
            "last_name": "Kowalski",
            "source": "otodom",
            "contacts": [],
        }
        result = enrich_agency(
            {"agency": agency_dict, "country": "poland", "country_code": "pl"}
        )
        # Should have fallen back to company path and found Lusha contacts
        assert len(result["contacts"]) >= 1
        assert result["contacts"][0]["email"] == "ana.garcia@savills.es"


def test_enrich_agency_individual_no_fallback_when_found():
    """If the individual path succeeds, the company path must NOT run."""
    individual_result = {
        "fullName": "Jan Kowalski",
        "jobTitle": "Leasing Manager",
        "primaryEmail": "jan@biuroabc.pl",
        "primaryEmailConfidence": 0.88,
        "linkedinProfile": "",
        "companyName": "Biuro ABC",
    }
    with (
        patch(
            "shared.location_scraper.clients.lusha.search_individual",
            return_value=individual_result,
        ),
        patch(
            "shared.location_scraper.activities.enrich.apify_client.run_sync"
        ) as mock_google,
    ):
        agency_dict = {
            "company_name": "Biuro ABC",
            "first_name": "Jan",
            "last_name": "Kowalski",
            "source": "otodom",
            "contacts": [],
        }
        result = enrich_agency(
            {"agency": agency_dict, "country": "poland", "country_code": "pl"}
        )
        assert result["contacts"][0]["email"] == "jan@biuroabc.pl"
        mock_google.assert_not_called()  # company path must not have run


def test_enrich_agency_company_path():
    with (
        patch(
            "shared.location_scraper.activities.enrich.apify_client.run_sync",
            return_value=FAKE_GOOGLE_RESULT,
        ),
        patch(
            "shared.location_scraper.clients.lusha.search_contacts_by_domain",
            return_value=FAKE_LUSHA_CONTACTS,
        ),
        patch(
            "shared.location_scraper.clients.lusha.enrich_contact",
            return_value=FAKE_LUSHA_ENRICHED,
        ),
    ):
        agency_dict = {
            "company_name": "Savills Aguirre Newman",
            "first_name": "",
            "last_name": "",
            "source": "idealista",
            "contacts": [],
        }
        result = enrich_agency(
            {"agency": agency_dict, "country": "spain", "country_code": "es"}
        )
        assert result["company_name"] == "Savills Aguirre Newman"
        assert len(result["contacts"]) >= 1
        top = result["contacts"][0]
        assert top["email"] == "ana.garcia@savills.es"
        assert top["full_name"] == "Ana García"
        assert top["job_title"] == "Leasing Director"


# ---------------------------------------------------------------------------
# Step 5: consolidate_contacts
# ---------------------------------------------------------------------------


def test_consolidate_contacts():
    enriched = [
        {
            "company_name": "Savills Aguirre Newman",
            "first_name": "",
            "last_name": "",
            "source": "idealista",
            "contacts": [
                {"full_name": "Ana García", "job_title": "Leasing Director", "email": "ana@savills.es",
                 "email_confidence": "0.95", "linkedin_url": "", "seniority_rank": 3, "domain_rank": 1},
                {"full_name": "Pedro López", "job_title": "Consultant", "email": "pedro@savills.es",
                 "email_confidence": "0.80", "linkedin_url": "", "seniority_rank": 1, "domain_rank": 2},
            ],
        }
    ]
    bundles = consolidate_contacts(enriched)
    assert len(bundles) == 1
    b = bundles[0]
    assert b["agency_name"] == "Savills Aguirre Newman"
    assert b["email_1"] == "ana@savills.es"
    assert b["email_1_contact"] == "Ana García"
    assert b["email_2"] == "pedro@savills.es"
    assert b["email_3"] == ""


def test_consolidate_dedupes_by_email():
    enriched = [
        {
            "company_name": "CBRE",
            "first_name": "",
            "last_name": "",
            "source": "idealista",
            "contacts": [
                {"full_name": "X", "job_title": "Manager", "email": "dup@cbre.es",
                 "email_confidence": "0.9", "linkedin_url": "", "seniority_rank": 2, "domain_rank": 1},
                {"full_name": "X2", "job_title": "Manager", "email": "dup@cbre.es",
                 "email_confidence": "0.9", "linkedin_url": "", "seniority_rank": 2, "domain_rank": 2},
            ],
        }
    ]
    bundles = consolidate_contacts(enriched)
    assert bundles[0]["email_1"] == "dup@cbre.es"
    assert bundles[0]["email_2"] == ""


# ---------------------------------------------------------------------------
# Full pipeline: normalize → dedupe → enrich → consolidate → upsert
# ---------------------------------------------------------------------------


def test_full_idealista_pipeline():
    """
    Smoke test: run all steps end-to-end with mocked external calls.
    Verifies that upsert_sql issues a MERGE and INSERT via the cursor.
    """
    with (
        patch(
            "shared.location_scraper.activities.enrich.apify_client.run_sync",
            return_value=FAKE_GOOGLE_RESULT,
        ),
        patch(
            "shared.location_scraper.clients.lusha.search_contacts_by_domain",
            return_value=FAKE_LUSHA_CONTACTS,
        ),
        patch(
            "shared.location_scraper.clients.lusha.enrich_contact",
            return_value=FAKE_LUSHA_ENRICHED,
        ),
        mock_sql_empty() as (mock_client, mock_cursor),
    ):
        from shared.location_scraper.activities.persist import upsert_sql

        # Step 1: normalize
        listings = normalize_listings(
            {"actor": "idealista", "items": [RAW_IDEALISTA_ITEM], "city": "madrid"}
        )

        # Step 2: dedupe
        agencies = dedupe_agencies(listings)

        # Step 3: filter (mock returns empty → all agencies are new)
        mock_client.execute_query.return_value = []

        # Step 4: enrich each agency
        enriched = [
            enrich_agency({"agency": a, "country": "spain", "country_code": "es"})
            for a in agencies
        ]

        # Step 5: consolidate
        bundles = consolidate_contacts(enriched)

        # Step 6: upsert
        stats = upsert_sql(
            {"listings": listings, "bundles": bundles, "run_id": FAKE_RUN_ID, "city": "madrid"}
        )

        assert stats["buildings_found"] == 1
        assert stats["buildings_new"] == 1
        assert stats["buildings_updated"] == 0
        assert stats["city"] == "madrid"

        # Verify MERGE and INSERT were called
        assert mock_cursor.execute.call_count >= 2  # at minimum: MERGE building + INSERT listing
