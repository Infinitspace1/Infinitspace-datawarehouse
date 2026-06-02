"""Tests for the LoopNet adapter — field shapes taken from real actor output."""
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.location_scraper.adapters.loopnet import (
    LoopnetAdapter,
    available_surface_m2_from_payload,
    currency_for_country,
)
from shared.location_scraper.config import LOOPNET_ACTOR_ID


def _uk_listing(subtext="29,270 SF of Office Space Available", country="GB"):
    return {
        "propertyId": "12345678",
        "listingUrl": "https://www.loopnet.com/Listing/181A-High-Holborn/12345678/",
        "address": "181A High Holborn",
        "city": "London",
        "zip": "WC1V 7AP",
        "country": country,
        "price": "Upon Request",
        "priceNumeric": None,
        "header": {"subtext": subtext},
        "spaces": [{"size": "12,000 SF", "spaceUse": "Office"}],
        "brokerName": "Jane Broker",
        "brokerCompany": "Savills",
        "brokerPhone": "020 7000 0000",
        "brokerEmail": "jane@savills.com",
    }


def test_build_input_shape_and_cap():
    adapter = LoopnetAdapter()
    payload = adapter.build_input("https://www.loopnet.com/search/office-properties/london-england--united-kingdom/for-rent/")
    assert payload["startUrls"] == [
        {"url": "https://www.loopnet.com/search/office-properties/london-england--united-kingdom/for-rent/"}
    ]
    assert payload["includeListingDetails"] is True
    assert payload["maxItems"] == 100
    assert adapter.actor_id == LOOPNET_ACTOR_ID


def test_build_input_unlimited_omits_max_items():
    assert "maxItems" not in LoopnetAdapter().build_input("https://x", max_items=None)


def test_sf_to_m2_conversion():
    # 29,270 SF -> ~2719 m²
    assert round(available_surface_m2_from_payload({"header": {"subtext": "29,270 SF of Office Space Available"}})) == 2719


def test_surface_falls_back_to_summed_spaces():
    payload = {"spaces": [{"size": "10,000 SF"}, {"size": "12,000 SF"}]}
    # 22,000 SF -> ~2044 m²
    assert round(available_surface_m2_from_payload(payload)) == 2044


def test_currency_by_country():
    assert currency_for_country("GB") == "GBP"
    assert currency_for_country("United Kingdom") == "GBP"
    assert currency_for_country("US") == "USD"
    assert currency_for_country(None) == "USD"


def test_normalize_maps_core_fields_uk():
    listing = LoopnetAdapter().normalize(_uk_listing(), "london")
    assert listing is not None
    assert listing.source == "loopnet"
    assert listing.city == "london"
    assert listing.external_id == "12345678"
    assert listing.postal_code == "WC1V 7AP"
    assert round(listing.available_surface_m2) == 2719
    assert listing.currency == "GBP"
    assert listing.contact_name == "Jane Broker"
    assert listing.company_name == "Savills"
    assert listing.phone == "020 7000 0000"
    assert listing.email == "jane@savills.com"
    # No coords in payload -> filled later by geocode fallback
    assert listing.latitude is None and listing.longitude is None
    # Price is "Upon Request" -> not coerced
    assert listing.price_monthly is None
    # Agency join key for loopnet is the company name
    assert listing.matching_name() == "Savills"


def test_normalize_drops_building_below_1500_m2():
    # 10,988 SF -> ~1021 m² -> below the 1500 m² floor -> trash
    small = _uk_listing(subtext="10,988 SF of Office Space Available")
    assert LoopnetAdapter().normalize(small, "london") is None


def test_normalize_drops_when_no_surface():
    payload = {"propertyId": "1", "city": "London", "header": {}}
    assert LoopnetAdapter().normalize(payload, "london") is None


# --- globe materialization: broker contacts + currency (Lusha skipped) ---

def test_globe_currency_loopnet_by_country():
    from shared.location_scraper.activities.materialize_globe import _pick_currency
    assert _pick_currency({"country": "GB"}, "loopnet") == "GBP"
    assert _pick_currency({"country": "US"}, "loopnet") == "USD"


def test_globe_surfaces_broker_email_for_loopnet():
    from shared.location_scraper.activities.materialize_globe import (
        _contact_slots,
        _loopnet_broker_contacts,
    )
    payload = _uk_listing()
    payload["brokerEmails"] = ["jane@savills.com", "team@savills.com"]
    contacts = _loopnet_broker_contacts(payload)
    assert [c["email"] for c in contacts] == ["jane@savills.com", "team@savills.com"]
    assert contacts[0]["name"] == "Jane Broker"
    # _contact_slots packs 3 slots x 4 fields = 12 values; slot 1 email first
    slots = _contact_slots(contacts)
    assert slots[0] == "jane@savills.com"
    assert slots[1] == "Jane Broker"
    assert slots[4] == "team@savills.com"


def test_globe_broker_contacts_empty_when_no_email():
    from shared.location_scraper.activities.materialize_globe import _loopnet_broker_contacts
    assert _loopnet_broker_contacts({"brokerName": "X"}) == []
