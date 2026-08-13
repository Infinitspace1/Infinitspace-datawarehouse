"""
shared/nexudus/transformers/resources.py

Transforms bronze.nexudus_resources -> silver.nexudus_resources
"""
from typing import Optional


def _int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def transform_resource(raw: dict, bronze_id: int, sync_run_id: str) -> Optional[dict]:
    """
    Transform a Nexudus resource record from bronze -> silver.

    Args:
        raw: Raw JSON from bronze.nexudus_resources
        bronze_id: PK from bronze table
        sync_run_id: Current sync run UUID

    Returns:
        Dict with silver columns, or None if record should be skipped
    """
    source_id = raw.get("Id")
    if not source_id:
        return None

    return {
        "source_id": source_id,
        "bronze_id": bronze_id,
        "sync_run_id": sync_run_id,
        "location_source_id": raw.get("BusinessId"),
        "nexudus_uuid": raw.get("UniqueId"),
        "name": raw.get("Name"),
        "description": raw.get("Description"),
        "resource_type_id": raw.get("ResourceTypeId"),
        "resource_type_name": raw.get("ResourceTypeName"),
        # 1 = meeting room, 2 = desk, 3 = office (Nexudus SystemResourceType).
        # The AVA meeting-room refresh filters on this, so it must stay fresh.
        "system_resource_type": _int(raw.get("SystemResourceType")),
        "is_archived": raw.get("Archived", False),
        "group_id": raw.get("GroupId"),
        "group_name": raw.get("GroupName"),
        "is_visible": raw.get("Visible", False),
        "online": raw.get("Online", False),
        "visible_to_others": raw.get("VisibleToOthers", False),
        "available": raw.get("Available", False),
        # Room seat count lives in "Allocation" on the API payload; "Capacity"
        # (the old mapping) is absent on most records, which left allocation
        # NULL for nearly every meeting room.
        "allocation": _int(raw.get("Allocation") if raw.get("Allocation") is not None else raw.get("Capacity")),
        "size": raw.get("Size"),
        "floor_number": raw.get("FloorNumber"),
        "accessible": raw.get("Accessible", False),
        "created_on": raw.get("CreatedOn"),
        "updated_on": raw.get("UpdatedOn"),
    }
