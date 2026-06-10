"""
shared/nexudus/transformers/event_attendees.py

Transforms raw bronze.nexudus_event_attendees JSON into a typed dict
for silver.nexudus_event_attendees.

Source endpoint: GET /api/content/eventattendees

Linking:
  - CalendarEventId   -> silver.nexudus_calendar_events.source_id
  - BusinessId        -> silver.nexudus_locations.source_id
  - CoworkerId        -> silver.nexudus_coworkers.source_id        (null for external guests)
  - EventProductId    -> silver.nexudus_event_products.source_id   (the ticket purchased)
  - CoworkerInvoiceId -> silver.nexudus_coworker_invoices.source_id (null until invoiced)

Fields deliberately excluded (verified against live payloads 2026-06-10):
  - CustomFields, LocalizationDetails -> always null
  - IsNew, SystemId                   -> internal
  - ToStringText                      -> duplicate of FullName
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


def transform_event_attendee(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Transform one raw Nexudus EventAttendee record into a silver row dict."""
    return {
        # Source
        "source_id":                    raw["Id"],
        "unique_id":                    _str(raw.get("UniqueId")),
        "bronze_id":                    bronze_id,
        "sync_run_id":                  sync_run_id,

        # Event (soft FK -> silver.nexudus_calendar_events.source_id)
        "calendar_event_source_id":     raw["CalendarEventId"],
        "calendar_event_name":          _str(raw.get("CalendarEventName")),

        # Location (soft FK -> silver.nexudus_locations.source_id)
        "location_source_id":           _int(raw.get("BusinessId")),

        # Attendee identity — CoworkerId is null for external (non-member) guests
        "coworker_source_id":           _int(raw.get("CoworkerId")),
        "coworker_full_name":           _str(raw.get("CoworkerFullName")),
        "full_name":                    _str(raw.get("FullName")) or _str(raw.get("ToStringText")),
        "email":                        _str(raw.get("Email")),
        "attendee_code":                _str(raw.get("AttendeeCode")),

        # Check-in
        "checked_in":                   _bit(raw.get("CheckedIn")),
        "checked_in_date":              _parse_dt(raw.get("CheckedInDate")),

        # Ticket (soft FK -> silver.nexudus_event_products.source_id)
        "event_product_source_id":      _int(raw.get("EventProductId")),
        "event_product_name":           _str(raw.get("EventProductName")),
        "event_product_price":          _decimal(raw.get("EventProductPrice")),
        "event_product_currency_code":  _str(raw.get("EventProductCurrencyCode")),

        # Billing (soft FK -> silver.nexudus_coworker_invoices.source_id)
        "invoiced":                     _bit(raw.get("Invoiced")),
        "coworker_invoice_source_id":   _int(raw.get("CoworkerInvoiceId")),
        "coworker_invoice_number":      _str(raw.get("CoworkerInvoiceNumber")),
        "coworker_invoice_paid":        _bit(raw.get("CoworkerInvoicePaid")),
        "due_date":                     _parse_dt(raw.get("DueDate")),
        "purchase_order":               _str(raw.get("PurchaseOrder")),

        # Audit
        "updated_by":                   _str(raw.get("UpdatedBy")),
        "created_on":                   _parse_dt(raw.get("CreatedOn")),
        "updated_on":                   _parse_dt(raw.get("UpdatedOn")),
    }
