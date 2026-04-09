#!/usr/bin/env python3
"""
BambooHR API exploration script.

Demonstrates directory and employee API calls without database writes.
Loads BAMBOOHR_SUBDOMAIN and BAMBOOHR_API_KEY from .env via dotenv.
"""

import os
import json
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth


def main():
    """Main entry point."""
    # Load environment variables from .env
    load_dotenv()

    subdomain = os.getenv("BAMBOOHR_SUBDOMAIN")
    api_key = os.getenv("BAMBOOHR_API_KEY")

    if not subdomain or not api_key:
        raise ValueError(
            "Missing required environment variables: "
            "BAMBOOHR_SUBDOMAIN and/or BAMBOOHR_API_KEY"
        )

    base_url = f"https://api.bamboohr.com/api/gateway.php/{subdomain}/v1"
    headers = {"Accept": "application/json"}
    auth = HTTPBasicAuth(api_key, "x")

    # Fetch employee directory
    print("Fetching employee directory...")
    directory_url = f"{base_url}/employees/directory"
    resp = requests.get(directory_url, headers=headers, auth=auth)
    resp.raise_for_status()

    directory = resp.json()
    employees = directory.get("employees", [])

    print(f"\nEmployee count: {len(employees)}")

    if employees:
        # Print directory field names
        first_employee = employees[0]
        field_names = list(first_employee.keys())
        print(f"Directory field names: {field_names}")

        # Fetch first employee's full record with all 32 standard fields
        employee_id = first_employee.get("id")
        if employee_id:
            print(f"\nFetching full record for employee {employee_id}...")

            fields = [
                "id", "employeeNumber", "firstName", "lastName", "displayName",
                "workEmail", "homeEmail", "jobTitle", "department", "division",
                "location", "supervisor", "supervisorId", "employmentHistoryStatus",
                "hireDate", "terminationDate", "workPhone", "workPhoneExtension",
                "mobilePhone", "costCenter", "payGroup", "flsaCode", "gender",
                "nationality", "maritalStatus", "dateOfBirth", "address1", "city",
                "state", "country", "zipCode", "photoUrl"
            ]

            employee_url = f"{base_url}/employees/{employee_id}"
            params = {"fields": ",".join(fields)}
            resp = requests.get(
                employee_url,
                headers=headers,
                auth=auth,
                params=params
            )
            resp.raise_for_status()

            employee_record = resp.json()
            print("\nFull employee record:")
            print(json.dumps(employee_record, indent=2))


if __name__ == "__main__":
    main()
