"""
Tests for shared/location_scraper/activities/persist.py — the existing-building
update path.

Covers the two bugs confirmed against prod on 2026-06-11:
  1. contacts were only linked on the new-building path, so a re-scrape could
     never attach a broker/Lusha email to a building already in SQL;
  2. SQL DECIMAL values (pyodbc -> Decimal) were compared to scraped floats
     with !=, flagging unchanged prices as changed ("phantom" snapshots).
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from unittest.mock import MagicMock, patch

from shared.location_scraper.activities.persist import _dec2, upsert_sql
from shared.location_scraper.models import Listing

RUN_ID = "weekly-london-2026-W25"

BUILDING_ID = str(uuid.uuid4())
LATEST_LISTING_ID = str(uuid.uuid4())
NEW_LISTING_ID = str(uuid.uuid4())
CONTACT_ID = str(uuid.uuid4())


def _listing_dict(**overrides) -> dict:
    base = Listing(
        source="loopnet",
        city="london",
        external_id="123",
        web_link=None,
        link_to_gmap=None,
        latitude=51.5072,
        longitude=-0.1276,
        district=None,
        postal_code="EC1",
        address="1 Test St",
        surface_m2=2000.0,
        surface_display=21527.8,
        surface_unit="sqft",
        floor=None,
        status="active",
        is_exterior=None,
        has_lift=None,
        has_air_conditioning=None,
        price_monthly=18234.56,
        price_per_m2=9.12,
        currency="GBP",
        energy_class=None,
        first_listed_date=None,
        last_updated_date=None,
        days_on_market=None,
        contact_name="Bob Broker",
        company_name="CBRE",
        phone=None,
        contact_type=None,
        email="bob@cbre.com",
    ).to_dict()
    base.update(overrides)
    return base


def _existing_row(**overrides) -> dict:
    row = {
        "id": BUILDING_ID,
        "latitude": Decimal("51.507200"),
        "longitude": Decimal("-0.127600"),
        "floor": None,
        "latest_listing_id": LATEST_LISTING_ID,
        "price_monthly": Decimal("18234.56"),
        "price_per_m2": Decimal("9.12"),
        "status": "active",
        "has_lusha_contacts": 0,
    }
    row.update(overrides)
    return row


class _FakeCursor:
    """Records executed statements and answers fetchone() per statement kind."""

    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []
        self._last_sql = ""

    def execute(self, sql: str, params=()):
        self._last_sql = sql
        self.calls.append((sql, params))

    def fetchone(self):
        if "MERGE bronze.n8n_location_scraper_buildings" in self._last_sql:
            return ("INSERT", BUILDING_ID)
        if "INSERT INTO bronze.n8n_location_scraper_listings" in self._last_sql:
            return (NEW_LISTING_ID,)
        if "MERGE bronze.n8n_location_scraper_contacts" in self._last_sql:
            return (CONTACT_ID,)
        return None

    def executed(self, fragment: str) -> list[tuple[str, tuple]]:
        return [(s, p) for s, p in self.calls if fragment in s]


def _run_upsert(existing_rows: list[dict], listings: list[dict], bundles: list[dict] | None = None):
    cursor = _FakeCursor()
    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor

    client = MagicMock()
    client.execute_query.return_value = existing_rows
    client.get_connection.return_value = conn

    with patch("shared.location_scraper.activities.persist.get_sql_client", return_value=client):
        stats = upsert_sql(
            {"listings": listings, "bundles": bundles or [], "run_id": RUN_ID, "city": "london"}
        )
    return stats, cursor


def test_dec2_normalizes_decimal_and_float():
    assert _dec2(18234.56) == Decimal("18234.56")
    assert _dec2(Decimal("18234.56")) == _dec2(18234.56)
    assert _dec2(None) is None


def test_unchanged_price_inserts_no_snapshot_but_links_contact():
    """Decimal-from-SQL vs float-from-scraper must not count as a change,
    and the broker contact must still be linked to the latest listing."""
    stats, cursor = _run_upsert([_existing_row()], [_listing_dict()])

    assert stats["buildings_updated"] == 0
    assert cursor.executed("INSERT INTO bronze.n8n_location_scraper_listings") == []

    contact_merges = cursor.executed("MERGE bronze.n8n_location_scraper_contacts")
    assert len(contact_merges) == 1
    assert contact_merges[0][1][0] == "bob@cbre.com"

    links = cursor.executed("INSERT INTO bronze.n8n_location_scraper_listing_contacts")
    assert len(links) == 1
    assert links[0][1][0] == LATEST_LISTING_ID
    assert links[0][1][1] == CONTACT_ID


def test_changed_price_inserts_snapshot_and_links_contact_to_it():
    stats, cursor = _run_upsert(
        [_existing_row(price_monthly=Decimal("9999.00"))], [_listing_dict()]
    )

    assert stats["buildings_updated"] == 1
    assert len(cursor.executed("INSERT INTO bronze.n8n_location_scraper_listings")) == 1

    links = cursor.executed("INSERT INTO bronze.n8n_location_scraper_listing_contacts")
    assert len(links) == 1
    assert links[0][1][0] == NEW_LISTING_ID


def test_lusha_bundle_linked_on_existing_building():
    listing = _listing_dict(source="idealista", contact_name="Savills", email="")
    bundle = {
        "agency_name": "Savills",
        "email_1": "ana@savills.es",
        "email_1_contact": "Ana García",
        "email_1_title": "Leasing Director",
        "email_1_confidence": "0.95",
        "email_1_linkedin": "",
        "email_2": "",
        "email_2_contact": "",
        "email_2_title": "",
        "email_2_confidence": "",
        "email_2_linkedin": "",
        "email_3": "",
        "email_3_contact": "",
        "email_3_title": "",
        "email_3_confidence": "",
        "email_3_linkedin": "",
    }
    stats, cursor = _run_upsert([_existing_row()], [listing], bundles=[bundle])

    assert stats["buildings_updated"] == 0
    contact_merges = cursor.executed("MERGE bronze.n8n_location_scraper_contacts")
    assert len(contact_merges) == 1
    assert contact_merges[0][1][0] == "ana@savills.es"

    links = cursor.executed("INSERT INTO bronze.n8n_location_scraper_listing_contacts")
    assert len(links) == 1
    assert links[0][1][0] == LATEST_LISTING_ID


def test_second_identical_listing_same_run_inserts_single_snapshot():
    """After an update snapshot is written, a same-key listing later in the
    same run compares against the freshly written values."""
    stats, cursor = _run_upsert(
        [_existing_row(price_monthly=Decimal("9999.00"))],
        [_listing_dict(), _listing_dict()],
    )

    assert stats["buildings_updated"] == 1
    assert len(cursor.executed("INSERT INTO bronze.n8n_location_scraper_listings")) == 1
