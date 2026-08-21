"""
shared/nexudus/transformers/helpdesk_departments.py

Transforms raw bronze.nexudus_helpdesk_departments JSON into a typed dict
for silver.nexudus_helpdesk_departments.

Source endpoint: GET /api/support/helpdeskdepartments

A HelpDeskDepartment is the routing category a ticket is filed under
("Air con queries", "Cleaning queries", ...). Departments are PER-LOCATION,
so the same name legitimately exists once per site — never group by name
alone downstream.

Linking:
  - BusinessId -> silver.nexudus_locations.source_id

Fields deliberately excluded (verified against live payloads 2026-08-20):
  - Managers, AddedManagers, RemovedManagers -> always null on the list
    endpoint (only the per-ID detail call populates them)
  - CustomFields, LocalizationDetails        -> always null
  - IsNew, SystemId                          -> internal
  - ToStringText                             -> duplicate of Name
"""
from datetime import datetime
from typing import Optional


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _bit(value) -> int:
    return 1 if value else 0


def _int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _str(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def transform_helpdesk_department(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Transform one raw Nexudus HelpDeskDepartment record into a silver row dict."""
    return {
        # Source
        "source_id":            raw["Id"],
        "unique_id":            _str(raw.get("UniqueId")),
        "bronze_id":            bronze_id,
        "sync_run_id":          sync_run_id,

        # Location (soft FK -> silver.nexudus_locations.source_id)
        "location_source_id":   raw["BusinessId"],

        # Identity
        "name":                 _str(raw.get("Name")) or _str(raw.get("ToStringText")) or "",
        "description":          _str(raw.get("Description")),

        # Flags
        "is_active":            _bit(raw.get("Active")),
        "task_list_id":         _int(raw.get("TaskListId")),

        # Audit
        "updated_by":           _str(raw.get("UpdatedBy")),
        "created_on":           _parse_dt(raw.get("CreatedOn")),
        "updated_on":           _parse_dt(raw.get("UpdatedOn")),
    }
