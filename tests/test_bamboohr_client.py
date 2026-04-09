# tests/test_bamboohr_client.py
import unittest
from unittest.mock import MagicMock, patch

import requests as req

from shared.bamboohr.client import BambooHRClient


class TestBambooHRClient(unittest.TestCase):

    def setUp(self):
        self.client = BambooHRClient(subdomain="testco", api_key="testkey")

    @patch("shared.bamboohr.client.requests.get")
    def test_get_employee_ids_returns_list_of_ints(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "employees": [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        }
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = self.client.get_employee_ids()

        self.assertEqual(result, [1, 2, 3])

    @patch("shared.bamboohr.client.requests.get")
    def test_get_employee_returns_dict(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"id": "42", "firstName": "Alice", "workEmail": "alice@test.com"}
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = self.client.get_employee(42)

        self.assertEqual(result["id"], "42")
        self.assertEqual(result["firstName"], "Alice")

    @patch("shared.bamboohr.client.requests.get")
    def test_get_employees_skips_http_errors(self, mock_get):
        directory_resp = MagicMock()
        directory_resp.json.return_value = {"employees": [{"id": "1"}, {"id": "2"}]}
        directory_resp.raise_for_status.return_value = None

        good_resp = MagicMock()
        good_resp.json.return_value = {"id": "1", "firstName": "Alice"}
        good_resp.raise_for_status.return_value = None

        bad_resp = MagicMock()
        bad_resp.raise_for_status.side_effect = req.HTTPError("404 Not Found")

        mock_get.side_effect = [directory_resp, good_resp, bad_resp]

        result = self.client.get_employees()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "1")

    @patch("shared.bamboohr.client.requests.get")
    def test_get_employee_ids_empty_directory(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"employees": []}
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = self.client.get_employee_ids()

        self.assertEqual(result, [])
