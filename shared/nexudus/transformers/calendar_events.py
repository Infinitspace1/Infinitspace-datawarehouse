"""
shared/nexudus/transformers/calendar_events.py

Transforms raw bronze.nexudus_calendar_events JSON into a typed dict
for silver.nexudus_calendar_events.

Source endpoint: GET /api/content/calendarevents

Linking:
  - BusinessId -> silver.nexudus_locations.source_id   (location_source_id)
  - ResourceId -> silver.nexudus_resources.source_id   (resource_source_id, optional)

Fields deliberately excluded (always null / internal / low value, verified
against live payloads 2026-06-10):
  - AddedEventCategories, RemovedEventCategories, EventCategories -> always null
  - Allocation                                   -> always null (lives on EventProduct)
  - AfterEventNotificationText, SendBefore/AfterEventNotification -> notification config
  - AskBuyerAddress, AllowComments kept; Zoom* fields              -> unused (no Zoom)
  - ClearLargeLogoFile, ClearSmallLogoFile, NewLarge/SmallLogoUrl  -> write-only API fields
  - CustomFields, LocalizationDetails            -> always null
  - IsNew, SystemId, WhichEventsToUpdate         -> internal
  - RepeatOnMondays..RepeatOnSundays             -> repeat detail, rarely used
  - ToStringText                                 -> derivative of Name + dates
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


def transform_calendar_event(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Transform one raw Nexudus CalendarEvent record into a silver row dict."""
    return {
        # Source
        "source_id":                raw["Id"],
        "unique_id":                _str(raw.get("UniqueId")),
        "bronze_id":                bronze_id,
        "sync_run_id":              sync_run_id,

        # Location (soft FK -> silver.nexudus_locations.source_id)
        "location_source_id":       raw["BusinessId"],

        # Identity
        "name":                     _str(raw.get("Name")) or _str(raw.get("ToStringText")) or "",
        "slug":                     _str(raw.get("Slug")),
        "short_description":        _str(raw.get("ShortDescription")),
        "long_description":         _str(raw.get("LongDescription")),

        # Venue / links
        "venue_name":               _str(raw.get("Location")),
        "venue_address":            _str(raw.get("VenueAddress")),
        "web_address":              _str(raw.get("WebAddress")),
        "tickets_page":             _str(raw.get("TicketsPage")),
        "facebook_page":            _str(raw.get("FacebookPage")),
        "host_full_name":           _str(raw.get("HostFullName")),

        # Optional booked resource (soft FK -> silver.nexudus_resources.source_id)
        "resource_source_id":       _int(raw.get("ResourceId")),

        # Schedule
        "start_date":               _parse_dt(raw.get("StartDate")),
        "end_date":                 _parse_dt(raw.get("EndDate")),
        "publish_date":             _parse_dt(raw.get("PublishDate")),

        # Audience / visibility flags
        "only_for_contacts":        _bit(raw.get("OnlyForContacts")),
        "only_for_members":         _bit(raw.get("OnlyForMembers")),
        "allow_comments":           _bit(raw.get("AllowComments")),
        "enable_wait_list":         _bit(raw.get("EnableWaitList")),
        "show_event_attendees":     _bit(raw.get("ShowEventAttendees")),
        "show_in_home_page":        _bit(raw.get("ShowInHomePage")),
        "show_in_home_banner":      _bit(raw.get("ShowInHomeBanner")),

        # Recurrence
        "repeat_event":             _bit(raw.get("RepeatEvent")),
        "repeats":                  _int(raw.get("Repeats")),
        "repeat_every":             _int(raw.get("RepeatEvery")),
        "repeat_until":             _parse_dt(raw.get("RepeatUntil")),
        "repeat_series_unique_id":  _str(raw.get("RepeatSeriesUniqueId")),

        # Registration form
        "has_event_form":           _bit(raw.get("HasEventForm")),
        "form_page_id":             _int(raw.get("FormPageId")),
        "form_page_name":           _str(raw.get("FormPageName")),

        # Tickets / media
        "ticket_notes":             _str(raw.get("TicketNotes")),
        "large_logo_file_name":     _str(raw.get("LargeLogoFileName")),
        "small_logo_file_name":     _str(raw.get("SmallLogoFileName")),

        # Audit
        "updated_by":               _str(raw.get("UpdatedBy")),
        "created_on":               _parse_dt(raw.get("CreatedOn")),
        "updated_on":               _parse_dt(raw.get("UpdatedOn")),
    }
