"""
tests/test_nexudus_event_transformers.py

Unit tests for the Nexudus events transformers (calendar_events,
event_attendees, event_products). Sample payloads mirror real API
responses captured 2026-06-10 via scripts/python_scripts/inspect_nexudus_events.py.
"""
import unittest
from datetime import datetime, timezone

from shared.nexudus.transformers.calendar_events import transform_calendar_event
from shared.nexudus.transformers.event_attendees import transform_event_attendee
from shared.nexudus.transformers.event_products import transform_event_product


CALENDAR_EVENT = {
    "Id": 1376542172,
    "UniqueId": "31bf3673-285c-4da2-92eb-284affe072de",
    "BusinessId": 1376491117,
    "Name": "UX/UI Event",
    "Slug": "ux-ui-event",
    "ShortDescription": "Proin sodales lobortis purus.",
    "LongDescription": "Proin sodales lobortis purus, id elementum sapien.",
    "Location": "Amerika building",
    "VenueAddress": None,
    "WebAddress": None,
    "TicketsPage": None,
    "FacebookPage": None,
    "HostFullName": None,
    "ResourceId": None,
    "StartDate": "2021-01-29T21:00:00Z",
    "EndDate": "2021-01-29T22:00:00Z",
    "PublishDate": "2020-08-28T17:15:00Z",
    "OnlyForContacts": False,
    "OnlyForMembers": False,
    "AllowComments": False,
    "EnableWaitList": False,
    "ShowEventAttendees": False,
    "ShowInHomePage": False,
    "ShowInHomeBanner": False,
    "RepeatEvent": False,
    "Repeats": 2,
    "RepeatEvery": None,
    "RepeatUntil": None,
    "RepeatSeriesUniqueId": None,
    "HasEventForm": False,
    "FormPageId": None,
    "FormPageName": None,
    "TicketNotes": None,
    "LargeLogoFileName": "2021-05-19_12-11-14.png",
    "SmallLogoFileName": "2021-05-19_12-11-14.png",
    "UpdatedBy": "wilco.wijnbergen@infinitspace.com",
    "CreatedOn": "2020-08-28T17:28:37Z",
    "UpdatedOn": "2022-03-07T20:20:55Z",
    "ToStringText": "UX/UI Event (1/29/2021 - 1/29/2021)",
}

EVENT_ATTENDEE = {
    "Id": 1376883535,
    "UniqueId": "3983dbf0-8af8-48bd-964e-006dbf435fdf",
    "BusinessId": 1376491117,
    "CalendarEventId": 1376542172,
    "CalendarEventName": "UX/UI Event",
    "CoworkerId": 1376858554,
    "CoworkerFullName": "Wilco Wijnbergen externalmember",
    "FullName": "Wilco Wijnbergen",
    "Email": "wacwijnbergen+nexudusexternaleventjoiner@gmail.com",
    "AttendeeCode": "99769",
    "CheckedIn": False,
    "CheckedInDate": None,
    "EventProductId": 1376543173,
    "EventProductName": "UX/UI Event",
    "EventProductPrice": 25.0,
    "EventProductCurrencyCode": "EUR",
    "Invoiced": True,
    "CoworkerInvoiceId": None,
    "CoworkerInvoiceNumber": None,
    "CoworkerInvoicePaid": False,
    "DueDate": None,
    "PurchaseOrder": None,
    "UpdatedBy": "wacwijnbergen+nexudusexternaleventjoiner@gmail.com",
    "CreatedOn": "2020-08-29T13:22:56Z",
    "UpdatedOn": "2024-02-26T14:53:22Z",
    "ToStringText": "Wilco Wijnbergen",
}

EVENT_PRODUCT = {
    "Id": 1376543173,
    "UniqueId": "ddae39b9-fa88-4953-b658-6c5c561389c4",
    "CalendarEventId": 1376542172,
    "Name": "UX/UI Event",
    "Description": "UX/UI Event",
    "Price": 25.0,
    "CurrencyCode": "EUR",
    "CurrencyId": 3003,
    "Allocation": 100,
    "Sales": 1,
    "MaxTicketsPerAttendee": None,
    "StartDate": "2020-05-20T17:28:49Z",
    "EndDate": "2020-08-29T20:00:00Z",
    "OnlyForContacts": False,
    "OnlyForMembers": False,
    "Visible": True,
    "DisplayOrder": 0,
    "TicketNotes": "Pimp your Salad ticket",
    "TaxRateId": 1415016365,
    "FinancialAccountId": None,
    "UpdatedBy": "wacwijnbergen@gmail.com",
    "CreatedOn": "2020-08-28T17:28:38Z",
    "UpdatedOn": "2021-03-17T11:46:22Z",
    "ToStringText": "UX/UI Event (25.00)",
}


class TestCalendarEventTransformer(unittest.TestCase):
    def test_basic_fields(self):
        ev = transform_calendar_event(CALENDAR_EVENT, bronze_id=7, sync_run_id="run-1")
        self.assertEqual(ev["source_id"], 1376542172)
        self.assertEqual(ev["location_source_id"], 1376491117)
        self.assertEqual(ev["name"], "UX/UI Event")
        self.assertEqual(ev["slug"], "ux-ui-event")
        self.assertEqual(ev["venue_name"], "Amerika building")
        self.assertEqual(ev["bronze_id"], 7)
        self.assertEqual(ev["sync_run_id"], "run-1")

    def test_dates_parsed(self):
        ev = transform_calendar_event(CALENDAR_EVENT, 1, "r")
        self.assertEqual(
            ev["start_date"], datetime(2021, 1, 29, 21, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(
            ev["end_date"], datetime(2021, 1, 29, 22, 0, tzinfo=timezone.utc)
        )
        self.assertIsNone(ev["repeat_until"])

    def test_bits_and_ints(self):
        ev = transform_calendar_event(CALENDAR_EVENT, 1, "r")
        self.assertEqual(ev["repeat_event"], 0)
        self.assertEqual(ev["repeats"], 2)
        self.assertIsNone(ev["resource_source_id"])

    def test_name_falls_back_to_tostring(self):
        raw = dict(CALENDAR_EVENT, Name=None)
        ev = transform_calendar_event(raw, 1, "r")
        self.assertEqual(ev["name"], "UX/UI Event (1/29/2021 - 1/29/2021)")


class TestEventAttendeeTransformer(unittest.TestCase):
    def test_linking_ids(self):
        at = transform_event_attendee(EVENT_ATTENDEE, bronze_id=3, sync_run_id="run-2")
        self.assertEqual(at["source_id"], 1376883535)
        self.assertEqual(at["calendar_event_source_id"], 1376542172)
        self.assertEqual(at["location_source_id"], 1376491117)
        self.assertEqual(at["coworker_source_id"], 1376858554)
        self.assertEqual(at["event_product_source_id"], 1376543173)
        self.assertIsNone(at["coworker_invoice_source_id"])

    def test_attendee_fields(self):
        at = transform_event_attendee(EVENT_ATTENDEE, 1, "r")
        self.assertEqual(at["full_name"], "Wilco Wijnbergen")
        self.assertEqual(at["email"], "wacwijnbergen+nexudusexternaleventjoiner@gmail.com")
        self.assertEqual(at["attendee_code"], "99769")
        self.assertEqual(at["checked_in"], 0)
        self.assertEqual(at["invoiced"], 1)
        self.assertEqual(at["event_product_price"], 25.0)
        self.assertEqual(at["event_product_currency_code"], "EUR")

    def test_external_guest_without_coworker(self):
        raw = dict(EVENT_ATTENDEE, CoworkerId=None, CoworkerFullName=None)
        at = transform_event_attendee(raw, 1, "r")
        self.assertIsNone(at["coworker_source_id"])
        self.assertIsNone(at["coworker_full_name"])
        self.assertEqual(at["email"], EVENT_ATTENDEE["Email"])


class TestEventProductTransformer(unittest.TestCase):
    def test_basic_fields(self):
        ep = transform_event_product(EVENT_PRODUCT, bronze_id=5, sync_run_id="run-3")
        self.assertEqual(ep["source_id"], 1376543173)
        self.assertEqual(ep["calendar_event_source_id"], 1376542172)
        self.assertEqual(ep["name"], "UX/UI Event")
        self.assertEqual(ep["price"], 25.0)
        self.assertEqual(ep["currency_code"], "EUR")
        self.assertEqual(ep["allocation"], 100)
        self.assertEqual(ep["sales"], 1)
        self.assertEqual(ep["visible"], 1)
        self.assertEqual(ep["ticket_notes"], "Pimp your Salad ticket")

    def test_location_inherited_from_parent_event(self):
        ep = transform_event_product(
            EVENT_PRODUCT, 1, "r", location_source_id=1376491117
        )
        self.assertEqual(ep["location_source_id"], 1376491117)

    def test_location_none_when_parent_unknown(self):
        ep = transform_event_product(EVENT_PRODUCT, 1, "r")
        self.assertIsNone(ep["location_source_id"])

    def test_free_ticket_defaults_price_zero(self):
        raw = dict(EVENT_PRODUCT, Price=None)
        ep = transform_event_product(raw, 1, "r")
        self.assertEqual(ep["price"], 0.0)


if __name__ == "__main__":
    unittest.main()
