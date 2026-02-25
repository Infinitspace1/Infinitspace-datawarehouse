"""
shared/nexudus/transformers/resources.py

Transforms bronze.nexudus_resources → silver.nexudus_resources
"""
from typing import Optional


def transform_resource(raw: dict, bronze_id: int, sync_run_id: str) -> Optional[dict]:
    """
    Transform a Nexudus resource record from bronze → silver.
    
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
        "group_id": raw.get("GroupId"),
        "group_name": raw.get("GroupName"),
        "visible": raw.get("Visible", False),
        "online": raw.get("Online", False),
        "visible_to_others": raw.get("VisibleToOthers", False),
        "available": raw.get("Available", False),
        "capacity": raw.get("Capacity"),
        "size": raw.get("Size"),
        "floor_number": raw.get("FloorNumber"),
        "accessible": raw.get("Accessible", False),
        "created_on": raw.get("CreatedOn"),
        "updated_on": raw.get("UpdatedOn"),
    }
