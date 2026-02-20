"""
shared/nexudus/transformers/products.py

Transforms raw bronze.nexudus_products JSON into a single typed dict
for silver.nexudus_products.

All columns in one table:
  - Shared columns: all types
  - custom_size_sqm: type 1 only (from CustomFields)
  - resource_* + amenity_*: types 4+5 (null for others)
"""
from datetime import datetime
from typing import Optional

ITEM_TYPE_LABELS = {
    1: "Private Office",
    2: "Dedicated Desk",
    3: "Hot Desk",
    4: "Other",
    5: "Meeting Room",
}


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _bit(value) -> Optional[int]:
    if value is None:
        return None
    return 1 if value else 0


def _int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_custom_size(raw: dict) -> Optional[float]:
    custom = raw.get("CustomFields")
    if not isinstance(custom, dict):
        return None
    for item in (custom.get("Data") or []):
        if item.get("Name") == "Nexudus.FloorPlan.Size":
            try:
                return float(item["Value"])
            except (TypeError, ValueError, KeyError):
                return None
    return None


def transform_product(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Single transform for all ItemTypes → silver.nexudus_products."""
    item_type = raw.get("ItemType")
    is_room = item_type in (4, 5)

    return {
        # Source
        "source_id":                raw["Id"],
        "bronze_id":                bronze_id,
        "sync_run_id":              sync_run_id,

        # Classification
        "item_type":                item_type,
        "product_type_label":       ITEM_TYPE_LABELS.get(item_type, "Unknown"),

        # Location
        "location_source_id":       raw.get("FloorPlanBusinessId"),
        "location_name":            raw.get("FloorPlanBusinessName"),
        "floor_plan_id":            raw.get("FloorPlanId"),
        "floor_plan_name":          raw.get("FloorPlanName"),

        # Identity
        "name":                     raw.get("Name") or raw.get("ToStringText") or "Unknown",
        "area_code":                raw.get("Area") or None,

        # Pricing
        "price":                    raw.get("Price"),
        "currency_code":            raw.get("FloorPlanBusinessCurrencyCode"),

        # Availability
        "is_available":             1 if raw.get("Available") else 0,
        "available_from":           _parse_dt(raw.get("AvailableFromTime")),
        "available_to":             _parse_dt(raw.get("AvailableToTime")),

        # Current occupant
        "coworker_id":              raw.get("CoworkerId"),
        "coworker_name":            raw.get("CoworkerFullName"),
        "coworker_company":         raw.get("CoworkerTeamNames") or raw.get("CoworkerCompanyName"),
        "coworker_email":           raw.get("CoworkerEmail"),
        "contract_ids_raw":         raw.get("CoworkerContractIds"),

        # Physical
        "size_sqm":                 raw.get("Size"),
        "custom_size_sqm":          _extract_custom_size(raw),   # type 1 only, None otherwise
        "capacity":                 _int(raw.get("Capacity")),
        "size_is_linked_to_area":   _bit(raw.get("SizeIsLinkedToArea")),

        # Resource fields (types 4+5, None for others)
        "resource_id":              raw.get("ResourceId") if is_room else None,
        "resource_name":            raw.get("ResourceName") if is_room else None,
        "resource_type_name":       raw.get("ResourceResourceTypeName") if is_room else None,
        "resource_allocation":      raw.get("ResourceAllocation") if is_room else None,
        "resource_shifts":          raw.get("ResourceShifts") or None if is_room else None,

        # Amenities (types 4+5, None for others)
        "amenity_air_conditioning": _bit(raw.get("ResourceAirConditioning")) if is_room else None,
        "amenity_heating":          _bit(raw.get("ResourceHeating")) if is_room else None,
        "amenity_internet":         _bit(raw.get("ResourceInternet")) if is_room else None,
        "amenity_large_display":    _bit(raw.get("ResourceLargeDisplay")) if is_room else None,
        "amenity_natural_light":    _bit(raw.get("ResourceNaturalLight")) if is_room else None,
        "amenity_whiteboard":       _bit(raw.get("ResourceWhiteBoard")) if is_room else None,
        "amenity_soundproof":       _bit(raw.get("ResourceSoundproof")) if is_room else None,
        "amenity_quiet_zone":       _bit(raw.get("ResourceQuietZone")) if is_room else None,
        "amenity_tea_coffee":       _bit(raw.get("ResourceTeaAndCoffee")) if is_room else None,
        "amenity_security_lock":    _bit(raw.get("ResourceSecurityLock")) if is_room else None,
        "amenity_cctv":             _bit(raw.get("ResourceCCTV")) if is_room else None,
        "amenity_catering":         _bit(raw.get("ResourceCatering")) if is_room else None,
        "amenity_conference_phone": _bit(raw.get("ResourceConferencePhone")) if is_room else None,
        "amenity_projector":        _bit(raw.get("ResourceProjector")) if is_room else None,
        "amenity_standing_desk":    _bit(raw.get("ResourceStandingDesk")) if is_room else None,
        "amenity_drinks":           _bit(raw.get("ResourceDrinks")) if is_room else None,
        "amenity_privacy_screen":   _bit(raw.get("ResourcePrivacyScreen")) if is_room else None,
        "amenity_voice_recorder":   _bit(raw.get("ResourceVoiceRecorder")) if is_room else None,
        "amenity_standard_phone":   _bit(raw.get("ResourceStandardPhone")) if is_room else None,
        "amenity_wireless_charger": _bit(raw.get("ResourceWirelessCharger")) if is_room else None,

        # Timestamps
        "created_on":               _parse_dt(raw.get("CreatedOn")),
        "updated_on":               _parse_dt(raw.get("UpdatedOn")),
    }