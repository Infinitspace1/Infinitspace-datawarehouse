import unittest
from datetime import datetime

from shared.firebase.transformers.competence import (
    _parse_dt,
    transform_competence_list,
    transform_competitor,
)


def _competitor(**overrides) -> dict:
    base = {
        "title": "Spaces Zuidas",
        "address": "Gustav Mahlerplein 2, Amsterdam",
        "street": "Gustav Mahlerplein 2",
        "city": "Amsterdam",
        "postalCode": "1082 MA",
        "phone": "+31 20 123 4567",
        "website": "https://www.spacesworks.com",
        "googleMapsUrl": "https://maps.google.com/?cid=123",
        "placeId": "ChIJabc123",
        "categoryName": "coworking space",
        "latitude": "52.3376",
        "longitude": "4.8726",
        "last_seen_at": "2026-01-15 10:30:45.123456789+00:00",
        "last_seen_in_city": "Amsterdam",
        "last_seen_country_code": "NL",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-15T10:30:45Z",
    }
    base.update(overrides)
    return base


def _list(**overrides) -> dict:
    base = {
        "uid": "NL_AUTO",
        "competitor_list_name": "Netherlands (auto)",
        "country": "Netherlands",
        "country_code": "NL",
        "auto_managed": True,
        "status": "completed",
        "competitor_count": 312,
        "schema_version": 2,
        "last_error": None,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-15T10:30:45Z",
        "last_run_at": "2026-01-15T10:30:45Z",
    }
    base.update(overrides)
    return base


class TestTransformCompetitor(unittest.TestCase):

    def test_maps_canonical_fields(self):
        r = transform_competitor(
            _competitor(), "NL_AUTO::ChIJabc123", "NL_AUTO", bronze_id=7, sync_run_id="run-1"
        )
        self.assertEqual(r["source_id"], "NL_AUTO::ChIJabc123")
        self.assertEqual(r["list_source_id"], "NL_AUTO")
        self.assertEqual(r["place_id"], "ChIJabc123")
        self.assertEqual(r["title"], "Spaces Zuidas")
        self.assertEqual(r["category_name"], "coworking space")
        self.assertEqual(r["postal_code"], "1082 MA")
        self.assertEqual(r["country_code"], "NL")  # from last_seen_country_code
        self.assertEqual(r["google_maps_url"], "https://maps.google.com/?cid=123")
        self.assertEqual(r["bronze_id"], 7)

    def test_lat_lng_string_to_float(self):
        r = transform_competitor(_competitor(), "x", "NL_AUTO", 1, "x")
        self.assertAlmostEqual(r["latitude"], 52.3376)
        self.assertAlmostEqual(r["longitude"], 4.8726)

    def test_blank_lat_lng_is_none(self):
        r = transform_competitor(
            _competitor(latitude="", longitude=None), "x", "NL_AUTO", 1, "x"
        )
        self.assertIsNone(r["latitude"])
        self.assertIsNone(r["longitude"])

    def test_last_seen_at_parsed_with_nanoseconds(self):
        # Firestore Timestamps land in raw_json as a stringified 9-fractional-
        # digit value with a space separator; it must still parse.
        r = transform_competitor(_competitor(), "x", "NL_AUTO", 1, "x")
        self.assertIsInstance(r["last_seen_at"], datetime)
        self.assertEqual(r["last_seen_at"].year, 2026)


class TestTransformCompetenceList(unittest.TestCase):

    def test_maps_fields(self):
        r = transform_competence_list(_list(), "NL_AUTO", bronze_id=3, sync_run_id="run-2")
        self.assertEqual(r["source_id"], "NL_AUTO")
        self.assertEqual(r["competitor_list_name"], "Netherlands (auto)")
        self.assertEqual(r["country_code"], "NL")
        self.assertEqual(r["auto_managed"], 1)
        self.assertEqual(r["schema_version"], 2)
        self.assertEqual(r["competitor_count"], 312)
        self.assertEqual(r["status"], "completed")
        self.assertIsInstance(r["updated_at"], datetime)

    def test_auto_managed_missing_is_none(self):
        data = _list()
        del data["auto_managed"]
        r = transform_competence_list(data, "NL_AUTO", 1, "x")
        self.assertIsNone(r["auto_managed"])


class TestParseDt(unittest.TestCase):

    def test_space_separator_nanoseconds(self):
        dt = _parse_dt("2026-01-15 10:30:45.123456789+00:00")
        self.assertIsInstance(dt, datetime)
        self.assertEqual((dt.year, dt.month, dt.day), (2026, 1, 15))

    def test_zulu_suffix(self):
        dt = _parse_dt("2026-02-03T04:05:06Z")
        self.assertIsInstance(dt, datetime)

    def test_blank_and_none(self):
        self.assertIsNone(_parse_dt(""))
        self.assertIsNone(_parse_dt(None))

    def test_passthrough_datetime(self):
        now = datetime(2026, 1, 1, 12, 0, 0)
        self.assertEqual(_parse_dt(now), now)


if __name__ == "__main__":
    unittest.main()
