import unittest
from datetime import datetime

from shared.firebase.transformers.competence import (
    _parse_dt,
    resolve_competitor_country,
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
        self.assertEqual(r["country"], "Netherlands")  # named from the code
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


class TestCountryEnrichment(unittest.TestCase):

    def test_inherits_country_from_parent_list_when_own_empty(self):
        # The real-world case being fixed: competitor has no own country code.
        comp = _competitor(last_seen_country_code=None)
        r = transform_competitor(
            comp, "ES_AUTO::x", "ES_AUTO", 1, "run",
            list_country_name="Spain", list_country_code="ES",
        )
        self.assertEqual(r["country"], "Spain")
        self.assertEqual(r["country_code"], "ES")

    def test_own_code_wins_over_list_code(self):
        name, code = resolve_competitor_country("NL", "ES_AUTO", "Spain", "ES")
        self.assertEqual(code, "NL")           # own observed code wins
        self.assertEqual(name, "Netherlands")  # name realigned to the resolved code

    def test_list_name_used_when_codes_agree(self):
        # List name is authoritative when its code matches the resolved code,
        # even if the ISO map would phrase it differently.
        name, code = resolve_competitor_country(None, "GB_AUTO", "UK / Britain", "GB")
        self.assertEqual(code, "GB")
        self.assertEqual(name, "UK / Britain")

    def test_code_from_list_id_prefix_fallback(self):
        # No own code, no list passed -> derive ISO2 from the list id prefix.
        name, code = resolve_competitor_country(None, "PL_AUTO", None, None)
        self.assertEqual(code, "PL")
        self.assertEqual(name, "Poland")

    def test_code_derived_from_list_name_when_no_code(self):
        # The real competence_new shape: random list id, country NAME only, no code.
        name, code = resolve_competitor_country(None, "x9JY0OQhB6GalxIomfYK", "United Kingdom", None)
        self.assertEqual(code, "GB")
        self.assertEqual(name, "United Kingdom")

    def test_usa_alias_canonicalised(self):
        # Two lists spell it "USA" and "United States"; both must converge.
        n1, c1 = resolve_competitor_country(None, "3kK83ODpBn6uZmUJCKvg", "USA", None)
        n2, c2 = resolve_competitor_country(None, "61QWwMDzrtU4YwzsWfF6", "United States", None)
        self.assertEqual((n1, c1), ("United States", "US"))
        self.assertEqual((n2, c2), ("United States", "US"))

    def test_uk_normalised_to_gb(self):
        _name, code = resolve_competitor_country("uk", "GB_AUTO", None, None)
        self.assertEqual(code, "GB")

    def test_unresolvable_returns_none(self):
        name, code = resolve_competitor_country(None, "weird-list-id", None, None)
        self.assertIsNone(name)
        self.assertIsNone(code)


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
