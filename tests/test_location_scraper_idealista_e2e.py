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
    _clean_company_name_for_lusha,
    _company_name_variants_for_lusha,
    consolidate_contacts,
    dedupe_agencies,
    enrich_agency,
    filter_new_agencies,
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

    with (
        patch("shared.azure_clients.sql_client.get_sql_client", return_value=mock_client),
        # persist.py binds get_sql_client at import time, so patching only the
        # source module silently misses it once persist was imported elsewhere.
        patch("shared.location_scraper.activities.persist.get_sql_client", return_value=mock_client),
    ):
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


def test_normalize_idealista_missing_coords(monkeypatch):
    """Items without lat/lon should still be normalized.

    With no geocoder available, coordinates stay None. We stub the free geocoder
    so the test stays offline (no live Nominatim call) and asserts that path.
    """
    class _NoGeocode:
        def get_or_geocode(self, _address):
            return None

    monkeypatch.setattr(
        "shared.location_scraper.activities.scrape.NominatimGeocodingCache",
        _NoGeocode,
    )

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
    """Otodom uses company-first enrichment even when a broker name is present."""
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
        "surface_m2": None,
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
    assert agencies[0]["first_name"] == ""
    assert agencies[0]["last_name"] == ""


def test_dedupe_agencies_otodom_skips_private_sellers():
    """Private Otodom sellers are low-value for B2B Lusha enrichment."""
    otodom_listing = {
        "source": "otodom",
        "city": "warsaw",
        "contact_name": "Marek",
        "company_name": "Marek Property",
        "external_id": "1",
        "web_link": None,
        "link_to_gmap": None,
        "latitude": None,
        "longitude": None,
        "district": None,
        "postal_code": None,
        "address": None,
        "surface_m2": None,
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
        "contact_type": "private",
        "email": "",
        "agency_comment": None,
    }
    assert dedupe_agencies([otodom_listing]) == []


def test_dedupe_agencies_otodom_prefers_later_individual_contact():
    """Otodom dedupes to one company candidate regardless of listing broker names."""
    base = {
        "source": "otodom",
        "city": "warsaw",
        "company_name": "MAXON Nieruchomości",
        "external_id": "1",
        "web_link": None,
        "link_to_gmap": None,
        "latitude": None,
        "longitude": None,
        "district": None,
        "postal_code": None,
        "address": None,
        "surface_m2": None,
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
    company_only = {**base, "contact_name": "MAXON Nieruchomości"}
    with_person = {**base, "external_id": "2", "contact_name": "Ewa Hołopiak"}

    agencies = dedupe_agencies([company_only, with_person])

    assert len(agencies) == 1
    assert agencies[0]["company_name"] == "MAXON Nieruchomości"
    assert agencies[0]["first_name"] == ""
    assert agencies[0]["last_name"] == ""


def test_dedupe_agencies_otodom_keeps_multiple_individual_candidates_per_company():
    """Multiple broker names for one Otodom agency still produce one company candidate."""
    base = {
        "source": "otodom",
        "city": "warsaw",
        "company_name": "MAXON Nieruchomości",
        "external_id": "1",
        "web_link": None,
        "link_to_gmap": None,
        "latitude": None,
        "longitude": None,
        "district": None,
        "postal_code": None,
        "address": None,
        "surface_m2": None,
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
    agencies = dedupe_agencies(
        [
            {**base, "external_id": "1", "contact_name": "Ewa Hołopiak"},
            {**base, "external_id": "2", "contact_name": "Marcin Nowak"},
            {**base, "external_id": "3", "contact_name": "MAXON Nieruchomości"},
        ]
    )

    assert len(agencies) == 1
    assert agencies[0]["first_name"] == ""
    assert agencies[0]["last_name"] == ""
    assert {a["company_name"] for a in agencies} == {"MAXON Nieruchomości"}


def test_clean_company_name_for_lusha_removes_polish_legal_suffixes():
    assert _clean_company_name_for_lusha("CBRE Sp.z o.o.") == "CBRE"
    assert _clean_company_name_for_lusha("Vertigo Property Group sp. j.") == "Vertigo"
    assert _clean_company_name_for_lusha("Polski Holding Nieruchomości S.A.") == "Polski Holding"


def test_idealista_company_name_variants_remove_listing_descriptors():
    assert _company_name_variants_for_lusha(
        "Knight Frank Oficinas y Locales",
        "idealista",
    ) == ["Knight Frank Oficinas y Locales", "Knight Frank"]
    assert _company_name_variants_for_lusha(
        "Nirvana - Especialistas en oficinas",
        "idealista",
    ) == ["Nirvana - Especialistas en oficinas", "Nirvana Especialistas en oficinas", "Nirvana"]
    # City qualifier + "Commercial" suffix — Engel & Völkers Barcelona pattern
    assert _company_name_variants_for_lusha(
        "Engel & Völkers Commercial Barcelona",
        "idealista",
    ) == ["Engel & Völkers Commercial Barcelona", "Engel & Völkers"]
    # Italian city variant
    assert _company_name_variants_for_lusha(
        "Cushman & Wakefield Commercial Milano",
        "idealista",
    ) == ["Cushman & Wakefield Commercial Milano", "Cushman & Wakefield"]


def test_filter_new_agencies_skips_previously_successful_agency(monkeypatch):
    mock_client = MagicMock()
    mock_client.execute_query.side_effect = [
        [],
        [{"agency_name": "CBRE Sp.z o.o."}],
    ]
    monkeypatch.setattr("shared.azure_clients.sql_client.get_sql_client", lambda: mock_client)

    agencies = [
        {"company_name": "CBRE Sp.z o.o.", "first_name": "", "last_name": "", "source": "otodom", "contacts": []},
        {"company_name": "MAXON Nieruchomo>ci", "first_name": "", "last_name": "", "source": "otodom", "contacts": []},
    ]

    assert [a["company_name"] for a in filter_new_agencies(agencies)] == ["MAXON Nieruchomo>ci"]


def test_filter_new_agencies_skips_existing_exact_contact(monkeypatch):
    mock_client = MagicMock()
    mock_client.execute_query.side_effect = [
        [{"name": "Jan Kowalski"}],
        [],
    ]
    monkeypatch.setattr("shared.azure_clients.sql_client.get_sql_client", lambda: mock_client)

    agencies = [
        {"company_name": "Biuro ABC", "first_name": "Jan", "last_name": "Kowalski", "source": "otodom", "contacts": []},
        {"company_name": "Biuro XYZ", "first_name": "Anna", "last_name": "Nowak", "source": "otodom", "contacts": []},
    ]

    assert [a["company_name"] for a in filter_new_agencies(agencies)] == ["Biuro XYZ"]


def test_filter_new_agencies_skips_building_that_already_has_lusha_contact(monkeypatch):
    mock_client = MagicMock()
    mock_client.execute_query.side_effect = [
        [],
        [],
        [{"latitude": 52.12345, "longitude": 21.98765, "floor": 3}],
    ]
    monkeypatch.setattr("shared.azure_clients.sql_client.get_sql_client", lambda: mock_client)

    agency = {"company_name": "CBRE Sp.z o.o.", "first_name": "", "last_name": "", "source": "otodom", "contacts": []}
    listing = {
        "source": "otodom",
        "city": "warsaw",
        "external_id": "1",
        "web_link": None,
        "link_to_gmap": None,
        "latitude": 52.12345,
        "longitude": 21.98765,
        "district": None,
        "postal_code": None,
        "address": None,
        "surface_m2": None,
        "floor": "3",
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
        "contact_name": "CBRE Sp.z o.o.",
        "company_name": "CBRE Sp.z o.o.",
        "phone": None,
        "contact_type": "business",
        "email": "",
        "agency_comment": None,
    }

    assert filter_new_agencies({"agencies": [agency], "listings": [listing]}) == []


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


def test_enrich_agency_company_path_falls_back_to_global_lusha_search():
    calls = []

    def fake_search(domain, country=None, job_titles=None, company_name=None, limit=5):
        calls.append((domain, country, bool(job_titles), company_name, limit))
        if country is None and job_titles:
            return FAKE_LUSHA_CONTACTS
        return []

    with (
        patch(
            "shared.location_scraper.activities.enrich.apify_client.run_sync",
            return_value=FAKE_GOOGLE_RESULT,
        ),
        patch(
            "shared.location_scraper.clients.lusha.search_contacts_by_domain",
            side_effect=fake_search,
        ),
        patch(
            "shared.location_scraper.clients.lusha.enrich_contact",
            return_value=FAKE_LUSHA_ENRICHED,
        ),
    ):
        agency_dict = {
            "company_name": "CBRE Sp.z o.o.",
            "first_name": "",
            "last_name": "",
            "source": "otodom",
            "contacts": [],
        }
        result = enrich_agency(
            {"agency": agency_dict, "country": "poland", "country_code": "pl"}
        )

    assert result["contacts"][0]["email"] == "ana.garcia@savills.es"
    assert result["_diagnostics"]["lusha_search_mode"] == "company_job_titles_global"
    assert calls[:2] == [
        ("savills.es", "poland", True, "CBRE", 5),
        ("savills.es", None, True, "CBRE", 5),
    ]


def test_enrich_agency_company_path_skips_blocked_domains():
    google_result = [
        {
            "organicResults": [
                {"url": "https://www.facebook.com/example"},
                {"url": "https://www.linkedin.com/company/example"},
                {"url": "https://example-realestate.pl"},
            ],
        }
    ]
    calls = []

    def fake_search(domain, country=None, job_titles=None, company_name=None, limit=5):
        calls.append(domain)
        return FAKE_LUSHA_CONTACTS

    with (
        patch(
            "shared.location_scraper.activities.enrich.apify_client.run_sync",
            return_value=google_result,
        ),
        patch(
            "shared.location_scraper.clients.lusha.search_contacts_by_domain",
            side_effect=fake_search,
        ),
        patch(
            "shared.location_scraper.clients.lusha.enrich_contact",
            return_value=FAKE_LUSHA_ENRICHED,
        ),
    ):
        agency_dict = {
            "company_name": "Example Real Estate",
            "first_name": "",
            "last_name": "",
            "source": "otodom",
            "contacts": [],
        }
        result = enrich_agency(
            {"agency": agency_dict, "country": "poland", "country_code": "pl"}
        )

    assert calls[0] == "example-realestate.pl"
    assert result["_diagnostics"]["domain_used"] == "example-realestate.pl"


def test_enrich_agency_idealista_retries_with_cleaned_company_variant():
    calls = []

    def fake_search(domain, country=None, job_titles=None, company_name=None, limit=5):
        calls.append((domain, country, bool(job_titles), company_name, limit))
        if company_name == "Knight Frank" and country == "spain" and job_titles:
            return FAKE_LUSHA_CONTACTS
        return []

    google_result = [
        {
            "organicResults": [
                {"url": "https://www.knightfrank.com"},
                {"url": "https://www.knightfrank.es"},
                {"url": "https://www.knightfrank.es/oficinas"},
            ],
        }
    ]

    with (
        patch(
            "shared.location_scraper.activities.enrich.apify_client.run_sync",
            return_value=google_result,
        ),
        patch(
            "shared.location_scraper.clients.lusha.search_contacts_by_domain",
            side_effect=fake_search,
        ),
        patch(
            "shared.location_scraper.clients.lusha.enrich_contact",
            return_value=FAKE_LUSHA_ENRICHED,
        ),
    ):
        agency_dict = {
            "company_name": "Knight Frank Oficinas y Locales",
            "first_name": "",
            "last_name": "",
            "source": "idealista",
            "contacts": [],
        }
        result = enrich_agency(
            {"agency": agency_dict, "country": "spain", "country_code": "es"}
        )

    assert result["contacts"][0]["email"] == "ana.garcia@savills.es"
    assert result["_diagnostics"]["company_name_cleaned"] == "Knight Frank"
    assert result["_diagnostics"]["lusha_search_mode"] == "company_job_titles_country"
    assert [call[0] for call in calls].count("knightfrank.com") == 5
    assert "knightfrank.es" not in [call[0] for call in calls]


# ---------------------------------------------------------------------------
# Website scrape fallback
# ---------------------------------------------------------------------------


def test_website_scraper_extracts_emails_from_homepage():
    from shared.location_scraper.clients.website_scraper import scrape_emails_from_domain
    from unittest.mock import patch, MagicMock

    html = """
    <html><body>
      <p>Contact us at info@chrysol.es or hello@chrysol.es</p>
      <a href="mailto:noreply@chrysol.es">unsubscribe</a>
    </body></html>
    """
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.content = b"x"
    mock_resp.text = html

    with patch("shared.location_scraper.clients.website_scraper.requests.get", return_value=mock_resp):
        emails = scrape_emails_from_domain("chrysol.es")

    assert "info@chrysol.es" in emails
    assert "hello@chrysol.es" in emails
    assert "noreply@chrysol.es" not in emails


def test_website_scraper_stops_after_first_page_with_emails():
    from shared.location_scraper.clients.website_scraper import scrape_emails_from_domain
    from unittest.mock import patch, MagicMock

    call_count = 0

    def fake_get(url, **kwargs):
        nonlocal call_count
        call_count += 1
        mock = MagicMock()
        if url == "https://gralen.es":
            mock.ok = True
            mock.content = b"x"
            mock.text = "<p>info@gralen.es</p>"
        else:
            mock.ok = False
            mock.content = b""
        return mock

    with patch("shared.location_scraper.clients.website_scraper.requests.get", side_effect=fake_get):
        emails = scrape_emails_from_domain("gralen.es")

    assert emails == ["info@gralen.es"]
    assert call_count == 1  # stopped after homepage


def test_website_scraper_filters_system_emails():
    from shared.location_scraper.clients.website_scraper import scrape_emails_from_domain
    from unittest.mock import patch, MagicMock

    html = "<p>noreply@x.es bounce@x.es postmaster@x.es contact@x.es</p>"
    mock = MagicMock()
    mock.ok = True
    mock.content = b"x"
    mock.text = html

    with patch("shared.location_scraper.clients.website_scraper.requests.get", return_value=mock):
        emails = scrape_emails_from_domain("x.es")

    assert emails == ["contact@x.es"]


def test_website_scraper_decodes_html_entities():
    from shared.location_scraper.clients.website_scraper import scrape_emails_from_domain
    from unittest.mock import patch, MagicMock

    # &#64; is the HTML entity for @
    html = "<p>info&#64;gralen.es</p>"
    mock = MagicMock()
    mock.ok = True
    mock.content = b"x"
    mock.text = html

    with patch("shared.location_scraper.clients.website_scraper.requests.get", return_value=mock):
        emails = scrape_emails_from_domain("gralen.es")

    assert "info@gralen.es" in emails


def test_website_scraper_handles_at_obfuscation():
    from shared.location_scraper.clients.website_scraper import scrape_emails_from_domain
    from unittest.mock import patch, MagicMock

    html = "<p>info [at] gralen.es</p>"
    mock = MagicMock()
    mock.ok = True
    mock.content = b"x"
    mock.text = html

    with patch("shared.location_scraper.clients.website_scraper.requests.get", return_value=mock):
        emails = scrape_emails_from_domain("gralen.es")

    assert "info@gralen.es" in emails


def test_website_scraper_extracts_email_from_jsonld():
    from shared.location_scraper.clients.website_scraper import scrape_emails_from_domain
    from unittest.mock import patch, MagicMock

    # Email only in JSON-LD, not in visible HTML — typical SPA pattern
    html = """
    <html>
    <head>
    <script type="application/ld+json">
    {"@type": "LocalBusiness", "name": "Gralen", "email": "info@gralen.es"}
    </script>
    </head>
    <body><p>Contáctenos</p></body>
    </html>
    """
    mock = MagicMock()
    mock.ok = True
    mock.content = b"x"
    mock.text = html

    with patch("shared.location_scraper.clients.website_scraper.requests.get", return_value=mock):
        emails = scrape_emails_from_domain("gralen.es")

    assert "info@gralen.es" in emails


def test_website_scraper_tries_www_prefix_if_bare_domain_empty():
    from shared.location_scraper.clients.website_scraper import scrape_emails_from_domain
    from unittest.mock import patch, MagicMock

    def fake_get(url, **kwargs):
        mock = MagicMock()
        if "www.gralen.es" in url:
            mock.ok = True
            mock.content = b"x"
            mock.text = "<p>info@gralen.es</p>"
        else:
            mock.ok = False
            mock.content = b""
        return mock

    with patch("shared.location_scraper.clients.website_scraper.requests.get", side_effect=fake_get):
        emails = scrape_emails_from_domain("gralen.es")

    assert "info@gralen.es" in emails


def test_enrich_agency_falls_back_to_website_when_lusha_empty():
    google_result = [
        {"organicResults": [{"url": "https://www.chrysol.es"}]}
    ]

    html_with_email = "<p>Llámanos o escríbenos a info@chrysol.es</p>"
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.content = b"x"
    mock_resp.text = html_with_email

    with (
        patch(
            "shared.location_scraper.activities.enrich.apify_client.run_sync",
            return_value=google_result,
        ),
        patch(
            "shared.location_scraper.clients.lusha.search_contacts_by_domain",
            return_value=[],
        ),
        patch(
            "shared.location_scraper.clients.website_scraper.requests.get",
            return_value=mock_resp,
        ),
    ):
        result = enrich_agency(
            {
                "agency": {
                    "company_name": "Chrysol Value Real Estate",
                    "first_name": "",
                    "last_name": "",
                    "source": "idealista",
                    "contacts": [],
                },
                "country": "spain",
                "country_code": "es",
            }
        )

    assert result["contacts"][0]["email"] == "info@chrysol.es"
    assert result["contacts"][0]["email_confidence"] == "website"
    assert result["_diagnostics"]["reason"] == "website_success"
    assert result["_diagnostics"]["lusha_search_mode"] == "website_scrape"
    assert result["_diagnostics"]["has_contact"] is True


def test_enrich_agency_no_website_fallback_when_no_domain():
    """If Google search finds no domain, website fallback must not be attempted."""
    with (
        patch(
            "shared.location_scraper.activities.enrich.apify_client.run_sync",
            return_value=[{"organicResults": []}],
        ),
        patch(
            "shared.location_scraper.clients.website_scraper.requests.get",
        ) as mock_get,
    ):
        result = enrich_agency(
            {
                "agency": {
                    "company_name": "Unknown Agency",
                    "first_name": "",
                    "last_name": "",
                    "source": "idealista",
                    "contacts": [],
                },
                "country": "spain",
                "country_code": "es",
            }
        )

    mock_get.assert_not_called()
    assert result["_diagnostics"]["reason"] == "company_no_domain_found"


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
