import unittest
from datetime import date

from shared.bamboohr.transformers.employees import transform_employee


def _raw(**overrides) -> dict:
    base = {
        "id": "42",
        "employeeNumber": "E001",
        "firstName": "Alice",
        "lastName": "Smith",
        "workEmail": "alice@infinitspace.com",
        "jobTitle": "Community Manager",
        "department": "Operations",
        "division": "EMEA",
        "location": "Dublin",
        "supervisor": "Bob Jones",
        "supervisorId": "7",
        "hireDate": "2021-03-15",
        "workPhone": "+353 1 234 5678",
        "workPhoneExtension": "101",
        "address1": "1 Main St",
        "city": "Dublin",
    }
    base.update(overrides)
    return base


class TestTransformEmployee(unittest.TestCase):

    def test_maps_core_fields(self):
        result = transform_employee(_raw(), bronze_id=99, sync_run_id="run-abc")
        self.assertEqual(result["source_id"], 42)
        self.assertEqual(result["first_name"], "Alice")
        self.assertEqual(result["last_name"], "Smith")
        self.assertEqual(result["work_email"], "alice@infinitspace.com")
        self.assertEqual(result["job_title"], "Community Manager")
        self.assertEqual(result["department"], "Operations")
        self.assertEqual(result["manager_id"], 7)
        self.assertEqual(result["manager_name"], "Bob Jones")
        self.assertEqual(result["bronze_id"], 99)
        self.assertEqual(result["sync_run_id"], "run-abc")

    def test_parses_hire_date(self):
        result = transform_employee(_raw(), bronze_id=1, sync_run_id="x")
        self.assertEqual(result["hire_date"], date(2021, 3, 15))

    def test_empty_string_date_returns_none(self):
        result = transform_employee(_raw(hireDate=""), bronze_id=1, sync_run_id="x")
        self.assertIsNone(result["hire_date"])

    def test_bamboohr_zero_date_returns_none(self):
        result = transform_employee(_raw(hireDate="0000-00-00"), bronze_id=1, sync_run_id="x")
        self.assertIsNone(result["hire_date"])

    def test_none_date_returns_none(self):
        result = transform_employee(_raw(hireDate=None), bronze_id=1, sync_run_id="x")
        self.assertIsNone(result["hire_date"])

    def test_empty_string_field_returns_none(self):
        result = transform_employee(_raw(department=""), bronze_id=1, sync_run_id="x")
        self.assertIsNone(result["department"])

    def test_none_field_returns_none(self):
        result = transform_employee(_raw(jobTitle=None), bronze_id=1, sync_run_id="x")
        self.assertIsNone(result["job_title"])

    def test_none_manager_id_returns_none(self):
        result = transform_employee(_raw(supervisorId=None), bronze_id=1, sync_run_id="x")
        self.assertIsNone(result["manager_id"])


if __name__ == "__main__":
    unittest.main()
