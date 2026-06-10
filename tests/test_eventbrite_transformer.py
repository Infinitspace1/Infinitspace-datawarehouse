"""
tests/test_eventbrite_transformer.py

Unit tests for the Eventbrite event transformer.
"""
import unittest
from datetime import datetime, timezone

from shared.eventbrite.transformers.events import transform_event


EVENT = {
    "id": "987654321098",
    "name": {"text": "Rooftop Networking Night", "html": "<b>Rooftop Networking Night</b>"},
    "summary": "Meet fellow members over drinks.",
    "description": {"text": "Long description here.", "html": "<p>Long description here.</p>"},
    "url": "https://www.eventbrite.com/e/rooftop-networking-night-tickets-987654321098",
    "start": {"timezone": "Europe/Amsterdam", "local": "2026-06-20T18:00:00", "utc": "2026-06-20T16:00:00Z"},
    "end": {"timezone": "Europe/Amsterdam", "local": "2026-06-20T21:00:00", "utc": "2026-06-20T19:00:00Z"},
    "organization_id": "111222333",
    "organizer_id": "444555666",
    "organizer": {"id": "444555666", "name": "InfinitSpace Amsterdam"},
    "created": "2026-05-01T09:00:00Z",
    "changed": "2026-06-01T12:00:00Z",
    "published": "2026-05-02T10:00:00Z",
    "status": "live",
    "currency": "EUR",
    "online_event": False,
    "listed": True,
    "shareable": True,
    "is_free": False,
    "is_series": False,
    "is_series_parent": False,
    "hide_start_date": False,
    "hide_end_date": False,
    "capacity": 120,
    "capacity_is_custom": True,
    "series_id": None,
    "format_id": "100",
    "format": {"id": "100", "name": "Networking"},
    "category_id": "101",
    "category": {"id": "101", "name": "Business & Professional"},
    "subcategory_id": None,
    "venue_id": "777888999",
    "venue": {
        "id": "777888999",
        "resource_uri": "https://www.eventbriteapi.com/v3/venues/777888999/",
        "name": "InfinitSpace Amerika Building",
        "latitude": "52.3563",
        "longitude": "4.8896",
        "capacity": 120,
        "age_restriction": "18+",
        "address": {
            "address_1": "Amerikastraat 1",
            "city": "Amsterdam",
            "region": "NH",
            "postal_code": "1043 AA",
            "country": "NL",
            "latitude": "52.3562",
            "longitude": "4.8895",
            "localized_address_display": "Amerikastraat 1, 1043 AA Amsterdam",
            "localized_area_display": "Amsterdam",
            "localized_multi_line_address_display": ["Amerikastraat 1", "Amsterdam", "1043 AA"],
        },
    },
    "ticket_availability": {
        "has_available_tickets": True,
        "is_sold_out": False,
        "waitlist_available": False,
        "minimum_ticket_price": {"currency": "EUR", "value": 1500, "major_value": "15.00", "display": "15.00 EUR"},
        "maximum_ticket_price": {"currency": "EUR", "value": 3500, "major_value": "35.00", "display": "35.00 EUR"},
        "start_sales_date": {"timezone": "Europe/Amsterdam", "local": "2026-05-02T10:00:00", "utc": "2026-05-02T08:00:00Z"},
    },
    "logo": {"url": "https://img.evbuc.com/logo.png", "original": {"url": "https://img.evbuc.com/logo_orig.png"}},
}


class TestEventbriteEventTransformer(unittest.TestCase):
    def test_identity_fields(self):
        ev = transform_event(EVENT, bronze_id=21, sync_run_id="run-1")
        self.assertEqual(ev["source_id"], "987654321098")
        self.assertEqual(ev["name"], "Rooftop Networking Night")
        self.assertEqual(ev["summary"], "Meet fellow members over drinks.")
        self.assertEqual(ev["description_text"], "Long description here.")
        self.assertEqual(ev["description_html"], "<p>Long description here.</p>")
        self.assertEqual(ev["status"], "live")
        self.assertEqual(ev["currency"], "EUR")
        self.assertEqual(ev["organization_id"], "111222333")
        self.assertEqual(ev["organizer_name"], "InfinitSpace Amsterdam")

    def test_schedule(self):
        ev = transform_event(EVENT, 1, "r")
        self.assertEqual(
            ev["start_utc"], datetime(2026, 6, 20, 16, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(ev["timezone"], "Europe/Amsterdam")
        self.assertEqual(ev["start_local"], datetime(2026, 6, 20, 18, 0))

    def test_venue_flattened(self):
        ev = transform_event(EVENT, 1, "r")
        self.assertEqual(ev["venue_id"], "777888999")
        self.assertEqual(ev["venue_resource_uri"], "https://www.eventbriteapi.com/v3/venues/777888999/")
        self.assertEqual(ev["venue_name"], "InfinitSpace Amerika Building")
        self.assertEqual(ev["venue_address"], "Amerikastraat 1, 1043 AA Amsterdam")
        self.assertEqual(ev["venue_address_1"], "Amerikastraat 1")
        self.assertIsNone(ev["venue_address_2"])
        self.assertEqual(ev["venue_city"], "Amsterdam")
        self.assertEqual(ev["venue_country"], "NL")
        self.assertAlmostEqual(ev["venue_address_latitude"], 52.3562)
        self.assertAlmostEqual(ev["venue_address_longitude"], 4.8895)
        self.assertEqual(ev["venue_localized_area"], "Amsterdam")
        self.assertEqual(ev["venue_multi_line_address"], "Amerikastraat 1\nAmsterdam\n1043 AA")
        self.assertAlmostEqual(ev["venue_latitude"], 52.3563)
        self.assertEqual(ev["venue_capacity"], 120)
        self.assertEqual(ev["venue_age_restriction"], "18+")
        self.assertNotIn("venue_json", ev)

    def test_tickets(self):
        ev = transform_event(EVENT, 1, "r")
        self.assertEqual(ev["has_available_tickets"], 1)
        self.assertEqual(ev["is_sold_out"], 0)
        self.assertEqual(ev["minimum_ticket_price"], 15.0)
        self.assertEqual(ev["maximum_ticket_price"], 35.0)
        self.assertEqual(ev["minimum_ticket_price_display"], "15.00 EUR")
        self.assertEqual(ev["minimum_ticket_price_currency"], "EUR")
        self.assertEqual(ev["minimum_ticket_price_minor"], 1500)
        self.assertEqual(ev["maximum_ticket_price_currency"], "EUR")
        self.assertEqual(ev["maximum_ticket_price_minor"], 3500)
        self.assertEqual(ev["ticket_currency"], "EUR")
        self.assertEqual(
            ev["sales_start_utc"], datetime(2026, 5, 2, 8, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(ev["sales_start_local"], datetime(2026, 5, 2, 10, 0))
        self.assertEqual(ev["sales_start_timezone"], "Europe/Amsterdam")
        self.assertNotIn("ticket_availability_json", ev)

    def test_price_falls_back_to_minor_units(self):
        raw = dict(EVENT)
        raw["ticket_availability"] = {
            "minimum_ticket_price": {"currency": "EUR", "value": 2050},
        }
        ev = transform_event(raw, 1, "r")
        self.assertEqual(ev["minimum_ticket_price"], 20.5)

    def test_flags_and_capacity(self):
        ev = transform_event(EVENT, 1, "r")
        self.assertEqual(ev["online_event"], 0)
        self.assertEqual(ev["listed"], 1)
        self.assertEqual(ev["is_free"], 0)
        self.assertEqual(ev["capacity"], 120)
        self.assertEqual(ev["capacity_is_custom"], 1)

    def test_logo_url(self):
        ev = transform_event(EVENT, 1, "r")
        self.assertEqual(ev["logo_url"], "https://img.evbuc.com/logo.png")

    def test_minimal_online_event(self):
        raw = {
            "id": "1",
            "name": {"text": "Webinar"},
            "status": "draft",
            "online_event": True,
        }
        ev = transform_event(raw, 1, "r")
        self.assertEqual(ev["source_id"], "1")
        self.assertEqual(ev["name"], "Webinar")
        self.assertEqual(ev["online_event"], 1)
        self.assertIsNone(ev["venue_id"])
        self.assertIsNone(ev["venue_capacity"])
        self.assertIsNone(ev["minimum_ticket_price"])
        self.assertIsNone(ev["ticket_currency"])


if __name__ == "__main__":
    unittest.main()
