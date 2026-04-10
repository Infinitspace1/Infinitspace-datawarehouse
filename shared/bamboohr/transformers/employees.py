"""
Transforms a raw BambooHR employee API response dict into a typed dict
for silver.bamboohr_employees.
Pure function — no I/O.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Optional


def _str(value: Any) -> Optional[str]:
    if value in (None, "", "0000-00-00"):
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _date(value: Any) -> Optional[date]:
    if value in (None, "", "0000-00-00"):
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        return None


def transform_employee(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    return {
        "source_id":        _int(raw.get("id")),
        "employee_number":  _str(raw.get("employeeNumber")),
        "first_name":       _str(raw.get("firstName")),
        "last_name":        _str(raw.get("lastName")),
        "work_email":       _str(raw.get("workEmail")),
        "job_title":        _str(raw.get("jobTitle")),
        "department":       _str(raw.get("department")),
        "division":         _str(raw.get("division")),
        "location":         _str(raw.get("location")),
        "manager_id":       _int(raw.get("supervisorId")),
        "manager_name":     _str(raw.get("supervisor")),
        "hire_date":        _date(raw.get("hireDate")),
        "work_phone":       _str(raw.get("workPhone")),
        "work_phone_ext":   _str(raw.get("workPhoneExtension")),
        "address1":         _str(raw.get("address1")),
        "city":             _str(raw.get("city")),
        "bronze_id":        bronze_id,
        "sync_run_id":      sync_run_id,
    }
