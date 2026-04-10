"""
scripts/python_scripts/bamboohr_explore.py

Standalone script: explores the BambooHR API and prints the raw JSON
response for one employee. No database writes.

Usage:
    python scripts/python_scripts/bamboohr_explore.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

FIELDS = ",".join([
    "id", "employeeNumber", "firstName", "lastName", "displayName",
    "workEmail", "homeEmail", "jobTitle", "department", "division",
    "location", "supervisor", "supervisorId", "employmentHistoryStatus",
    "hireDate", "terminationDate", "workPhone", "workPhoneExtension",
    "mobilePhone", "costCenter", "payGroup", "flsaCode",
    "gender", "nationality", "maritalStatus", "dateOfBirth",
    "address1", "city", "state", "country", "zipCode", "photoUrl",
])


def main() -> None:
    subdomain = os.environ["BAMBOOHR_SUBDOMAIN"]
    api_key = os.environ["BAMBOOHR_API_KEY"]
    base_url = f"https://api.bamboohr.com/api/gateway.php/{subdomain}/v1"
    auth = HTTPBasicAuth(api_key, "x")
    headers = {"Accept": "application/json"}

    print(f"Connecting to: {base_url}\n")

    # Step 1: fetch directory
    resp = requests.get(
        f"{base_url}/employees/directory",
        auth=auth, headers=headers, timeout=30,
    )
    resp.raise_for_status()
    directory = resp.json()
    employees = directory.get("employees", [])
    print(f"Directory: {len(employees)} employees")
    if employees:
        print(f"Directory fields: {list(employees[0].keys())}\n")

    if not employees:
        print("No employees found — check subdomain and API key.")
        return

    # Step 2: fetch first employee full record
    first_id = employees[0]["id"]
    print(f"Fetching full record for employee id={first_id} ...")
    resp = requests.get(
        f"{base_url}/employees/{first_id}",
        auth=auth, headers=headers,
        params={"fields": FIELDS},
        timeout=30,
    )
    resp.raise_for_status()
    employee = resp.json()

    print("\nFull employee record (first employee):")
    print(json.dumps(employee, indent=2, default=str))
    print(f"\nFields returned ({len(employee)}): {list(employee.keys())}")


if __name__ == "__main__":
    main()
