import unittest

from shared.nexudus.colleague_sync import (
    compute_mapping_delta,
    extract_accessible_businesses,
    parse_coworker_payload,
)


class TestNexudusColleagueSync(unittest.TestCase):
    def test_extract_accessible_businesses_from_known_key(self):
        payload = {
            "Id": 101,
            "AccessibleBusinesses": [
                {"Id": 10, "Name": "London"},
                {"Id": 20, "Name": "Berlin"},
            ],
        }
        businesses = extract_accessible_businesses(payload)
        by_id = {b.business_id: b.name for b in businesses}
        self.assertEqual(by_id[10], "London")
        self.assertEqual(by_id[20], "Berlin")

    def test_extract_accessible_businesses_uses_fallback_primary_business(self):
        payload = {
            "Id": 101,
            "BusinessId": 1376491118,
            "BusinessName": "infinitSpace Team",
        }
        businesses = extract_accessible_businesses(payload)
        self.assertEqual(len(businesses), 1)
        self.assertEqual(businesses[0].business_id, 1376491118)
        self.assertEqual(businesses[0].name, "infinitSpace Team")

    def test_extract_accessible_businesses_from_scalar_business_ids(self):
        payload = {
            "Id": 101,
            "Businesses": [1376491116, 1415499547, 1420976575],
            "InvoicingBusinessId": 1420976575,
            "InvoicingBusinessName": "London - Holborn - 14 Grays Inn Rd",
        }
        businesses = extract_accessible_businesses(payload)
        by_id = {b.business_id: b.name for b in businesses}
        self.assertEqual(set(by_id.keys()), {1376491116, 1415499547, 1420976575})
        self.assertEqual(by_id[1420976575], "London - Holborn - 14 Grays Inn Rd")

    def test_compute_mapping_delta_adds_and_removes(self):
        existing = {1, 2, 3}
        desired = {2, 3, 4}
        to_add, to_remove = compute_mapping_delta(existing, desired)
        self.assertEqual(to_add, {4})
        self.assertEqual(to_remove, {1})

    def test_parse_coworker_payload_handles_partial_payload(self):
        payload = {
            "Id": 555,
            "FirstName": "Ada",
            "Email": "ada@example.com",
            "BusinessId": 42,
        }
        parsed = parse_coworker_payload(payload, default_team_business_id=1376491118)
        self.assertEqual(parsed.nexudus_coworker_id, 555)
        self.assertEqual(parsed.first_name, "Ada")
        self.assertEqual(parsed.last_name, None)
        self.assertEqual(parsed.email, "ada@example.com")
        self.assertEqual(parsed.team_business_id, 42)
        self.assertEqual(parsed.raw_payload["Id"], 555)


if __name__ == "__main__":
    unittest.main()
