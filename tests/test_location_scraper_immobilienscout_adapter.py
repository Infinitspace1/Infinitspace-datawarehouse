from __future__ import annotations

from shared.location_scraper.adapters.immobilienscout import ImmobilienscoutAdapter


def test_normalize_skips_small_surface():
    adapter = ImmobilienscoutAdapter()
    raw = {
        "id": "123",
        "obj_netFloorSpace": "1499 m²",
        "geo_city": "Berlin",
    }
    assert adapter.normalize(raw, "berlin") is None


def test_normalize_maps_core_fields():
    adapter = ImmobilienscoutAdapter()
    raw = {
        "id": "160531543",
        "obj_netFloorSpace": "1,850 m²",
        "obj_totalRent": "€25,500",
        "obj_baseRent": "€13.78 /m²",
        "geo_wgs84Lat": "52.5200",
        "geo_wgs84Lon": "13.4050",
        "geo_city": "Berlin",
        "geo_quarter": "Mitte",
        "geo_plz": "10115",
        "geo_street": "Friedrichstr.",
        "geo_houseNumber": "1",
        "obj_contactName": "Max Mustermann",
        "obj_realtorCompanyName": "Example Office GmbH",
        "obj_phoneNumber": "+49 30 123456",
        "url": "/expose/160531543",
    }
    listing = adapter.normalize(raw, "berlin")
    assert listing is not None
    assert listing.source == "immobilienscout"
    assert listing.available_surface_m2 == 1850.0
    assert listing.price_monthly == 25500.0
    assert listing.city == "berlin"
    assert listing.web_link == "https://www.immobilienscout24.de/expose/160531543"
    assert listing.company_name == "Example Office GmbH"


def test_normalize_maps_flattened_csv_shape():
    adapter = ImmobilienscoutAdapter()
    raw = {
        "normalized/listingId": "167325674",
        "normalized/url": "https://www.immobilienscout24.de/expose/167325674",
        "normalized/address/city": "Berlin",
        "normalized/address/latitude": "52.50524",
        "normalized/address/longitude": "13.44052",
        "normalized/address/street": "Mühlenstraße",
        "normalized/address/houseNumber": "25",
        "normalized/address/zip": "10243",
        "adTargetingParameters/obj_mainFloorSpace": "2068",
        "adTargetingParameters/obj_rentPerSqM": "30",
        "normalized/price/currency": "EUR",
        "contact/contactData/agent/name": "Frau Malina Trockel",
        "contact/contactData/agent/company": "Savills Immobilien Beratungs-GmbH - Office Agency",
        "contact/phoneNumbers/0/text": "+49 173 192 42 11",
        "header/publicationState": "active",
    }
    listing = adapter.normalize(raw, "berlin")
    assert listing is not None
    assert listing.external_id == "167325674"
    assert listing.city == "berlin"
    assert listing.available_surface_m2 == 2068.0
    assert listing.price_per_m2 == 30.0
    assert listing.contact_name == "Frau Malina Trockel"
    assert listing.company_name == "Savills Immobilien Beratungs-GmbH - Office Agency"
    assert listing.phone == "+49 173 192 42 11"
