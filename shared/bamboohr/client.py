# shared/bamboohr/client.py
"""
shared/bamboohr/client.py

Synchronous BambooHR API client.
BambooHR Basic Auth: API key as username, literal "x" as password.
All methods return raw dicts — no transformation here.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

BAMBOOHR_FIELDS = ",".join([
    "id", "employeeNumber", "firstName", "lastName", "displayName",
    "workEmail", "homeEmail", "jobTitle", "department", "division",
    "location", "supervisor", "supervisorId", "employmentHistoryStatus",
    "hireDate", "terminationDate", "workPhone", "workPhoneExtension",
    "mobilePhone", "costCenter", "payGroup", "flsaCode",
    "gender", "nationality", "maritalStatus", "dateOfBirth",
    "address1", "city", "state", "country", "zipCode", "photoUrl",
])


class BambooHRClient:
    """
    Thin wrapper around the BambooHR REST API.

    Usage:
        client = BambooHRClient(subdomain="infinitspace", api_key="...")
        employees = client.get_employees()
    """

    def __init__(self, subdomain: str, api_key: str):
        self._base_url = f"https://api.bamboohr.com/api/gateway.php/{subdomain}/v1"
        self._auth = HTTPBasicAuth(api_key, "x")
        self._headers = {"Accept": "application/json"}

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self._base_url}/{path}"
        response = requests.get(
            url, auth=self._auth, headers=self._headers,
            params=params, timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_employee_ids(self) -> list[int]:
        """Fetch all employee IDs from the directory endpoint."""
        data = self._get("employees/directory")
        return [int(e["id"]) for e in data.get("employees", []) if e.get("id")]

    def get_employee(self, employee_id: int) -> dict:
        """Fetch a single employee's full standard field set."""
        return self._get(f"employees/{employee_id}", params={"fields": BAMBOOHR_FIELDS})

    def get_employees(self) -> list[dict]:
        """Fetch all employees with full standard fields. Skips individual failures."""
        ids = self.get_employee_ids()
        logger.info("BambooHR: %s employees in directory", len(ids))
        employees = []
        for emp_id in ids:
            try:
                employees.append(self.get_employee(emp_id))
            except requests.HTTPError as exc:
                logger.warning("BambooHR: failed to fetch employee %s: %s", emp_id, exc)
        return employees


def get_bamboohr_client() -> BambooHRClient:
    """Build a client from environment variables."""
    return BambooHRClient(
        subdomain=os.environ["BAMBOOHR_SUBDOMAIN"],
        api_key=os.environ["BAMBOOHR_API_KEY"],
    )
