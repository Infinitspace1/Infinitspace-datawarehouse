"""Tests for the LoopNet adapter — field shapes taken from real actor output."""
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.location_scraper.adapters.loopnet import (
    LoopnetAdapter,
    _available_sf_from_name,
    _parse_abbrev_sf,
    available_surface_m2_from_payload,
    available_surface_sqft_from_payload,
    currency_for_country,
)
from shared.location_scraper.config import (
    LOOPNET_ACTOR_ID,
    LOOPNET_MAX_UNBLOCKER_REQUESTS,
    LOOPNET_UNLIMITED_MAX_ITEMS,
)


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


def test_build_input_paginates_the_search_and_skips_the_detail_fetch(monkeypatch):
    """A search URL is walked page by page, with the detail fetch OFF.

    memo23 serves one result page per start URL from LoopNet's free mobile API.
    The per-listing detail fetch is the broken stage (403 -> throttled paid
    unblocker, 20-77% of listings dropped), and the placard already carries the
    building, the broker email and the available surface, so it is not needed.
    """
    monkeypatch.setenv("LOOPNET_SEARCH_PAGES", "4")
    base = "https://www.loopnet.co.uk/search/office-space/london-england--united-kingdom/for-rent/?min-space-size=16146"
    adapter = LoopnetAdapter()
    payload = adapter.build_input(base)

    assert payload["startUrls"] == [
        {"url": base},
        {"url": f"{base}&page=2"},
        {"url": f"{base}&page=3"},
        {"url": f"{base}&page=4"},
    ]
    assert payload["includeListingDetails"] is False
    assert payload["moreResults"] is True
    assert payload["maxItems"] == 100
    assert payload["maxUnblockerRequests"] == LOOPNET_MAX_UNBLOCKER_REQUESTS
    assert adapter.actor_id == LOOPNET_ACTOR_ID


def test_search_page_urls_uses_query_param_not_path():
    """The page MUST be a query param: the actor drops the `/N/` path segment
    and re-serves page 1, so the path form silently collapses the pagination."""
    from shared.location_scraper.adapters.loopnet import search_page_urls

    assert search_page_urls("https://x/search/?a=1", 3) == [
        "https://x/search/?a=1",
        "https://x/search/?a=1&page=2",
        "https://x/search/?a=1&page=3",
    ]
    # no query string yet -> starts one
    assert search_page_urls("https://x/search/", 2)[1] == "https://x/search/?page=2"
    assert search_page_urls("https://x/search/", 1) == ["https://x/search/"]


def test_build_input_unlimited_sends_explicit_ceiling():
    """`maxItems` must be SENT even when uncapped.

    Omitting the key makes the actor stop the search early at a small internal
    default — measured 2026-07-23 on Los Angeles: 67 items with the key absent
    vs 147 with an explicit ceiling (same build, same URL).
    """
    payload = LoopnetAdapter().build_input("https://x", max_items=None)
    assert payload["maxItems"] == LOOPNET_UNLIMITED_MAX_ITEMS


def test_build_input_explicit_max_items_wins():
    payload = LoopnetAdapter().build_input("https://x", max_items=250)
    assert payload["maxItems"] == 250


def test_listing_run_memory_scales_with_url_count():
    """The actor's 512 MB default is OOM-killed on large listing-URL lists
    (460 New York URLs died at exactly 512 MB on 2026-07-23)."""
    from shared.location_scraper.config import get_loopnet_listing_run_memory_mbytes

    assert get_loopnet_listing_run_memory_mbytes(40) == 1024
    assert get_loopnet_listing_run_memory_mbytes(150) == 2048
    assert get_loopnet_listing_run_memory_mbytes(300) == 4096
    assert get_loopnet_listing_run_memory_mbytes(564) == 8192


def test_listing_run_memory_env_override(monkeypatch):
    from shared.location_scraper.config import get_loopnet_listing_run_memory_mbytes

    monkeypatch.setenv("LOOPNET_LISTING_RUN_MEMORY_MB", "4096")
    assert get_loopnet_listing_run_memory_mbytes(40) == 4096


def test_start_apify_run_requests_memory_for_listing_urls(monkeypatch):
    from shared.location_scraper.activities.scrape import start_apify_run
    from shared.location_scraper.activities.resolve import resolve_source

    captured = {}

    def fake_start_run(actor_id, actor_input, memory_mbytes=None):
        captured["memory_mbytes"] = memory_mbytes
        captured["actor_input"] = actor_input
        return {"run_id": "r", "dataset_id": "d"}

    monkeypatch.setattr(
        "shared.location_scraper.activities.scrape.apify_client.start_run", fake_start_run
    )
    cfg = resolve_source("london", None, "run-1", unlimited_items=True)
    cfg.listing_urls = [f"https://www.loopnet.co.uk/Listing/{i}/" for i in range(300)]
    start_apify_run(cfg.to_dict())

    assert captured["memory_mbytes"] == 4096
    assert len(captured["actor_input"]["startUrls"]) == 300


def test_build_input_listing_url_list():
    """Enumerated listing-detail URLs are passed through one-to-one."""
    urls = [
        "https://www.loopnet.co.uk/listing/10-queen-street-pl-london/34548634/",
        "https://www.loopnet.co.uk/listing/21-southampton-row-london/34501482/",
    ]
    payload = LoopnetAdapter().build_input(urls, max_items=None)
    assert payload["startUrls"] == [{"url": u} for u in urls]
    assert payload["maxItems"] == LOOPNET_UNLIMITED_MAX_ITEMS
    # Bare listing URLs carry no placard, so this mode alone still needs the
    # (fragile) detail fetch — which is why it is no longer the default path.
    assert payload["includeListingDetails"] is True


def test_sf_to_m2_conversion():
    # 29,270 SF -> ~2719 m²
    assert round(available_surface_m2_from_payload({"header": {"subtext": "29,270 SF of Office Space Available"}})) == 2719


def test_surface_falls_back_to_summed_spaces():
    payload = {"spaces": [{"size": "10,000 SF"}, {"size": "12,000 SF"}]}
    # 22,000 SF -> ~2044 m²
    assert round(available_surface_m2_from_payload(payload)) == 2044


def test_native_sqft_preserved():
    # Display value keeps LoopNet's native square footage verbatim.
    assert available_surface_sqft_from_payload(
        {"header": {"subtext": "29,270 SF of Office Space Available"}}
    ) == 29270.0
    payload = {"spaces": [{"size": "10,000 SF"}, {"size": "12,000 SF"}]}
    assert available_surface_sqft_from_payload(payload) == 22000.0


def test_currency_by_country():
    assert currency_for_country("GB") == "GBP"
    assert currency_for_country("United Kingdom") == "GBP"
    assert currency_for_country("US") == "USD"
    assert currency_for_country("CA") == "CAD"
    assert currency_for_country("Canada") == "CAD"
    assert currency_for_country(None) == "USD"


def test_normalize_maps_core_fields_uk():
    listing = LoopnetAdapter().normalize(_uk_listing(), "london")
    assert listing is not None
    assert listing.source == "loopnet"
    assert listing.city == "london"
    assert listing.external_id == "12345678"
    assert listing.postal_code == "WC1V 7AP"
    assert round(listing.surface_m2) == 2719
    # UK/US listings display square feet (the native LoopNet figure), not m².
    assert listing.surface_unit == "sqft"
    assert listing.surface_display == 29270.0
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


# --- srp-ldjson fallback (mobile API 403 -> search-results LD+JSON) -----------
# Since LoopNet locked its mobile API behind App Check, the actor often serves a
# `srp-ldjson` payload: broker name + company, surface only in `description`,
# address only in `listingName`, no `sizeSf`/`spaces`/coordinates/email.
# Observed live 2026-08-31 (London W36: 609 such items -> 0 buildings before this).

def _srp_ldjson_listing(
    description="27,568 sq ft Office Property Offered in West Drayton UB7 0EB",
    listing_name="450 Bath Rd, West Drayton UB7 0EB",
):
    return {
        "propertyId": "40510241",
        "listingUrl": "https://www.loopnet.co.uk/listing/450-bath-rd-west-drayton/40510241/",
        "listingName": listing_name,
        "description": description,
        "listingType": "For Lease",
        "brokerName": "John Hicks",
        "brokerCompany": "IW Group Services (UK) Ltd",
        "position": 18,
        "_dataSource": "srp-ldjson",
    }


def test_surface_from_srp_ldjson_description():
    # 27,568 sq ft -> ~2561 m², above the 1500 m² floor.
    payload = _srp_ldjson_listing()
    assert available_surface_sqft_from_payload(payload) == 27568.0
    assert available_surface_m2_from_payload(payload) > 1500


def test_normalize_srp_ldjson_keeps_building_with_address_and_broker():
    listing = LoopnetAdapter().normalize(_srp_ldjson_listing(), "london")
    assert listing is not None
    # address falls back to listingName so the geocode step has something to work with
    assert listing.address == "450 Bath Rd, West Drayton UB7 0EB"
    assert listing.contact_name == "John Hicks"
    assert listing.company_name == "IW Group Services (UK) Ltd"
    assert listing.currency == "GBP"  # derived from the city, not the absent country
    assert listing.email == ""  # no payload email — filled by directory/Lusha downstream


def test_normalize_srp_ldjson_drops_below_floor():
    small = _srp_ldjson_listing(description="9,228 sq ft Office Property Offered in New Malden")
    assert LoopnetAdapter().normalize(small, "london") is None


def test_description_surface_never_overrides_a_healthy_sizesf_payload():
    # A free-mobile-API payload carries sizeSf AND a description; the description
    # must never win over the real available-surface field.
    payload = {"propertyId": "9", "city": "London", "sizeSf": "36.8K",
               "description": "1,000 sq ft tiny mention"}
    assert available_surface_sqft_from_payload(payload) == 36800.0


# --- globe materialization: broker contacts + currency (Lusha skipped) ---

def test_globe_currency_loopnet_by_country():
    from shared.location_scraper.activities.materialize_globe import _pick_currency
    assert _pick_currency({"country": "GB"}, "loopnet") == "GBP"
    assert _pick_currency({"country": "US"}, "loopnet") == "USD"
    assert _pick_currency({"country": "CA"}, "loopnet") == "CAD"


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


# --- memo23 schema variants after the 2026-06-27 actor rebuild ---


def test_parse_abbrev_sf_handles_k_and_m():
    # The broad-search `sizeSf` field is abbreviated.
    assert _parse_abbrev_sf("36.8K") == 36800
    assert _parse_abbrev_sf("624K") == 624000
    assert _parse_abbrev_sf("1.2M") == 1200000
    assert _parse_abbrev_sf("29,186") == 29186
    assert _parse_abbrev_sf(None) is None
    assert _parse_abbrev_sf("Upon Request") is None


def test_available_sf_from_name_range_and_single():
    # "X - Y SF ... Available" -> upper bound; single "Y SF ... Available" -> Y.
    assert _available_sf_from_name(
        "The Concorde | 2222 W Dunlap Ave - 1,605 - 103,916 SF of 4-Star Space Available in Phoenix, AZ"
    ) == 103916
    assert _available_sf_from_name(
        "345 Convention Way - 7,334 SF of Office  Space Available in Redwood City, CA"
    ) == 7334
    # UK listings quote "sq ft", not "SF".
    assert _available_sf_from_name(
        "1 Edcity - 10,173 - 41,511 sq ft of 4-Star Office  Space Available in London"
    ) == 41511
    # Coworking listings have no "... SF ... Available" clause.
    assert _available_sf_from_name("88 Kingsway - Coworking Space Available in London WC2B 6AA") is None


def test_surface_from_listing_web_name_payload():
    """memo23 'listingWeb' detail payload — surface lives only in `name`."""
    payload = {
        "sourceType": "listingWeb",
        "name": "2222 W Dunlap Ave - 1,605 - 103,916 SF of 4-Star Space Available in Phoenix, AZ",
        "buildingSize": "140,161 SF",  # total building size — must NOT be used as available
    }
    assert available_surface_sqft_from_payload(payload) == 103916.0
    assert round(available_surface_m2_from_payload(payload)) == 9654


def test_normalize_broad_search_sizesf_payload_with_broker():
    """The broad-search payload uses `sizeSf` + carries broker contact."""
    payload = {
        "propertyId": "40870460",
        "listingUrl": "https://www.loopnet.com/Listing/2222-W-Dunlap-Ave-Phoenix-AZ/40870460/",
        "address": "2222 W Dunlap Ave",
        "city": "Phoenix",
        "state": "AZ",
        "zip": "85021",
        "sizeSf": "36.8K",
        "brokerName": "Charles Strouss",
        "brokerCompany": "CBRE",
        "brokerPhone": "602-000-0000",
        "brokerEmail": "Charles.Strouss@cbre.com",
    }
    listing = LoopnetAdapter().normalize(payload, "phoenix")
    assert listing is not None
    assert round(listing.surface_m2) == 3419  # 36,800 SF
    assert listing.surface_display == 36800.0
    assert listing.email == "Charles.Strouss@cbre.com"
    assert listing.company_name == "CBRE"
    assert listing.external_id == "40870460"


def test_normalize_listing_web_name_payload_no_broker():
    """The listingWeb payload yields a building from `name` even without broker."""
    payload = {
        "sourceType": "listingWeb",
        "propertyId": "37654403",
        "listingUrl": "https://www.loopnet.com/Listing/2727-W-Glendale-Ave-Phoenix-AZ/37654403/",
        "name": "2727 W Glendale Ave - 8,299 - 20,255 SF of Office  Space Available in Phoenix, AZ 85051",
        "city": "Phoenix",
        "zip": "85051",
    }
    listing = LoopnetAdapter().normalize(payload, "phoenix")
    assert listing is not None
    assert round(listing.surface_m2) == 1882  # 20,255 SF
    assert listing.email == ""  # no broker in this schema


# ---------------------------------------------------------------------------
# Search-placard payload (2026-08-19): the shape the paginated search returns.
# ---------------------------------------------------------------------------

def _search_placard(**overrides):
    """One item as memo23's free-mobile-API search stage returns it."""
    item = {
        "propertyId": "39413508",
        "listingName": "Esavian House",
        "address": "181A High Holborn",
        "city": "London",
        "zip": "WC1V 7AP",
        "sizeSf": "29.3K",
        "totalSize": 29300,
        "latitude": 51.517,
        "longitude": -0.119,
        "currency": "USD",  # deliberately wrong for London -- must be ignored
        "price": "$27.07 - $175.98 /SF/YR",
        "brokerName": "Billie Collins",
        "brokerCompany": "Esavian House Offices Ltd",
        "brokerPhone": "07919 112537",
        "brokerEmail": "billie@esavianhouse.co.uk",
        "listingUrl": "https://www.loopnet.com/Listing/39413508/",
        "_dataSource": "free-mobile-api",
    }
    item.update(overrides)
    return item


def test_normalize_search_placard_keeps_building_and_broker_email():
    """The two things the pipeline exists for must survive the search path."""
    listing = LoopnetAdapter().normalize(_search_placard(), "london")
    assert listing is not None
    assert listing.address == "181A High Holborn"
    assert listing.postal_code == "WC1V 7AP"
    assert listing.external_id == "39413508"
    assert listing.email == "billie@esavianhouse.co.uk"
    assert listing.contact_name == "Billie Collins"
    assert listing.company_name == "Esavian House Offices Ltd"
    # 29,300 SF -> ~2722 m², kept natively in sqft for display
    assert listing.surface_display == 29300.0
    assert round(listing.surface_m2) == 2722


def test_normalize_search_placard_uses_payload_coordinates():
    """The placard carries coordinates; the detail payload never did, so every
    LoopNet listing used to be geocoded from its postcode."""
    listing = LoopnetAdapter().normalize(_search_placard(), "london")
    assert listing.latitude == 51.517
    assert listing.longitude == -0.119
    assert listing.link_to_gmap == "https://www.google.com/maps/search/?api=1&query=51.517,-0.119"


def test_normalize_zero_coordinates_fall_back_to_geocoding():
    listing = LoopnetAdapter().normalize(
        _search_placard(latitude=0, longitude=0), "london"
    )
    assert listing.latitude is None and listing.longitude is None
    assert listing.link_to_gmap is None


def test_normalize_currency_from_city_when_payload_has_no_country():
    """The placard drops `country` and its `currency` says USD for London, so
    the currency has to come from the city being scraped."""
    adapter = LoopnetAdapter()
    assert adapter.normalize(_search_placard(), "london").currency == "GBP"
    assert adapter.normalize(_search_placard(), "toronto").currency == "CAD"
    assert adapter.normalize(_search_placard(), "new york").currency == "USD"
    # an explicit country on the payload (detail path) still wins
    assert adapter.normalize(_search_placard(country="GB"), "new york").currency == "GBP"


def test_normalize_listings_drops_repeated_listings_within_a_run(monkeypatch):
    """Result pages overlap -- London returned 550 items for 320 buildings."""
    from shared.location_scraper.activities import scrape as scrape_act

    monkeypatch.setattr(scrape_act, "GeocodingCache", lambda *a, **k: None)
    monkeypatch.setattr(scrape_act, "NominatimGeocodingCache", lambda *a, **k: None)
    monkeypatch.setattr(scrape_act, "_apply_geocode_fallback", lambda *a, **k: False)

    items = [
        _search_placard(),
        _search_placard(),  # same propertyId, served again by a later page
        _search_placard(propertyId="99999999"),
    ]
    out = scrape_act.normalize_listings({"actor": "loopnet", "city": "london", "items": items})
    assert [l["external_id"] for l in out] == ["39413508", "99999999"]
