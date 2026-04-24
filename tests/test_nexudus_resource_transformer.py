import unittest

from shared.nexudus.transformers.resources import transform_resource


def _raw(**overrides) -> dict:
    base = {
        "Id": 123,
        "BusinessId": 456,
        "UniqueId": "abc-123",
        "Name": "Atlas Room",
        "Description": "Board room",
        "ResourceTypeId": 9,
        "ResourceTypeName": "10 person meeting room",
        "GroupId": 77,
        "GroupName": "Meeting Rooms",
        "Visible": True,
        "Online": False,
        "VisibleToOthers": True,
        "Available": True,
        "Capacity": "10",
        "Size": 24.5,
        "FloorNumber": 3,
        "Accessible": True,
        "CreatedOn": "2026-01-02T03:04:05Z",
        "UpdatedOn": "2026-02-03T04:05:06Z",
    }
    base.update(overrides)
    return base


class TestTransformResource(unittest.TestCase):

    def test_maps_canonical_visibility_and_allocation_fields(self):
        result = transform_resource(_raw(), bronze_id=99, sync_run_id="run-abc")

        self.assertEqual(result["source_id"], 123)
        self.assertEqual(result["group_id"], 77)
        self.assertEqual(result["group_name"], "Meeting Rooms")
        self.assertTrue(result["is_visible"])
        self.assertEqual(result["allocation"], 10)
        self.assertTrue(result["available"])

    def test_missing_id_returns_none(self):
        self.assertIsNone(transform_resource(_raw(Id=None), bronze_id=1, sync_run_id="x"))

    def test_non_numeric_capacity_returns_none(self):
        result = transform_resource(_raw(Capacity="unknown"), bronze_id=1, sync_run_id="x")
        self.assertIsNone(result["allocation"])


if __name__ == "__main__":
    unittest.main()
