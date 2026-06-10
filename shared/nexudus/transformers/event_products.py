"""
shared/nexudus/transformers/event_products.py

Transforms raw bronze.nexudus_event_products JSON into a typed dict
for silver.nexudus_event_products.

Source endpoint: GET /api/content/eventproducts

An EventProduct is a ticket type for a CalendarEvent (name, price,
allocation, sales counter).

Linking:
  - CalendarEventId -> silver.nexudus_calendar_events.source_id
  - The raw payload carries NO BusinessId; location_source_id is derived
    by the silver writer from the parent calendar event's BusinessId and
    passed in here, so event products can be filtered by location without
    a join.

Fields deliberately excluded (verified against live payloads 2026-06-10):
  - AddedTariffs, RemovedTariffs, Tariffs -> always null
  - CurrencyId                            -> CurrencyCode is more useful
  - CustomFields, LocalizationDetails    -> always null
  - IsNew, SystemId                      -> internal
  - ToStringText                         -> derivative of Name + Price
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


def _decimal(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _str(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def transform_event_product(
    raw: dict,
    bronze_id: int,
    sync_run_id: str,
    location_source_id: Optional[int] = None,
) -> dict:
    """Transform one raw Nexudus EventProduct record into a silver row dict.

    location_source_id is the parent calendar event's BusinessId, resolved by
    the silver writer (the EventProduct payload itself has no location field).
    """
    return {
        # Source
        "source_id":                raw["Id"],
        "unique_id":                _str(raw.get("UniqueId")),
        "bronze_id":                bronze_id,
        "sync_run_id":              sync_run_id,

        # Event (soft FK -> silver.nexudus_calendar_events.source_id)
        "calendar_event_source_id": raw["CalendarEventId"],

        # Location, inherited from the parent event
        # (soft FK -> silver.nexudus_locations.source_id)
        "location_source_id":       _int(location_source_id),

        # Identity
        "name":                     _str(raw.get("Name")) or _str(raw.get("ToStringText")) or "",
        "description":              _str(raw.get("Description")),

        # Pricing
        "price":                    _decimal(raw.get("Price")) or 0.0,
        "currency_code":            _str(raw.get("CurrencyCode")),

        # Capacity / sales
        "allocation":               _int(raw.get("Allocation")),
        "sales":                    _int(raw.get("Sales")),
        "max_tickets_per_attendee": _int(raw.get("MaxTicketsPerAttendee")),

        # Sale window
        "start_date":               _parse_dt(raw.get("StartDate")),
        "end_date":                 _parse_dt(raw.get("EndDate")),

        # Flags
        "only_for_contacts":        _bit(raw.get("OnlyForContacts")),
        "only_for_members":         _bit(raw.get("OnlyForMembers")),
        "visible":                  _bit(raw.get("Visible")),
        "display_order":            _int(raw.get("DisplayOrder")),

        # Tickets
        "ticket_notes":             _str(raw.get("TicketNotes")),

        # Financial
        "tax_rate_id":              _int(raw.get("TaxRateId")),
        "financial_account_id":     _int(raw.get("FinancialAccountId")),

        # Audit
        "updated_by":               _str(raw.get("UpdatedBy")),
        "created_on":               _parse_dt(raw.get("CreatedOn")),
        "updated_on":               _parse_dt(raw.get("UpdatedOn")),
    }
