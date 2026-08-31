"""
shared/nexudus/transformers/helpdesk_messages.py

Transforms raw bronze.nexudus_helpdesk_messages JSON into a typed dict
for silver.nexudus_helpdesk_messages.

Source endpoint: GET /api/support/helpdeskmessages

A HelpDeskMessage is a customer request ("ticket") raised by a member at a
location, routed to a department. Its reply thread lives in
support/helpdeskcomments — see helpdesk_comments.py.

Linking:
  - BusinessId            -> silver.nexudus_locations.source_id
  - CoworkerId            -> silver.nexudus_coworkers.source_id      (the requester)
  - HelpDeskDepartmentId  -> silver.nexudus_helpdesk_departments.source_id

`minutes_to_close` is DERIVED here from ClosedOn - CreatedOn. The source
field `MinutesToClose` is deliberately ignored: Nexudus computes it
backwards and returns a negative value (verified 2026-08-20 against a ticket
created 14:46 and closed 15:28, which the API reported as -41.65).

Fields deliberately excluded (verified against live payloads 2026-08-20):
  - MinutesToClose            -> broken at source, see above; recomputed
  - ClearImageFile, NewImageUrl -> write-only API fields
  - CustomFields, LocalizationDetails -> always null
  - IsNew, SystemId           -> internal
  - ToStringText              -> duplicate of Subject
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


def _minutes_between(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
    """Whole minutes from start to end, rounded to 2dp. None if either is missing.

    Returns None rather than a negative number if the timestamps are out of
    order, so a bad source record can't poison an AVG() downstream.
    """
    if start is None or end is None:
        return None
    delta = (end - start).total_seconds() / 60.0
    if delta < 0:
        return None
    return round(delta, 2)


def transform_helpdesk_message(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Transform one raw Nexudus HelpDeskMessage record into a silver row dict."""
    created_on = _parse_dt(raw.get("CreatedOn"))
    closed_on = _parse_dt(raw.get("ClosedOn"))

    return {
        # Source
        "source_id":                raw["Id"],
        "unique_id":                _str(raw.get("UniqueId")),
        "bronze_id":                bronze_id,
        "sync_run_id":              sync_run_id,

        # Location (soft FK -> silver.nexudus_locations.source_id)
        "location_source_id":       raw["BusinessId"],

        # Requester (soft FK -> silver.nexudus_coworkers.source_id)
        "coworker_source_id":       _int(raw.get("CoworkerId")),
        "coworker_full_name":       _str(raw.get("CoworkerFullName")),

        # Routing (soft FK -> silver.nexudus_helpdesk_departments.source_id)
        "department_source_id":     _int(raw.get("HelpDeskDepartmentId")),
        "department_name":          _str(raw.get("HelpDeskDepartmentName")),

        # Content
        "subject":                  _str(raw.get("Subject")),
        "message_text":             _str(raw.get("MessageText")),

        # Triage
        "priority":                 _int(raw.get("Priority")),

        # Lifecycle
        "is_closed":                _bit(raw.get("Closed")),
        "closed_on":                closed_on,

        # Assignment (a Nexudus user, not a coworker)
        "owner_source_id":          _int(raw.get("OwnerId")),
        "owner_full_name":          _str(raw.get("OwnerFullName")),

        # SLA
        "first_response_minutes":   _int(raw.get("FirstResponseTimeInMinutes")),
        "minutes_to_close":         _minutes_between(created_on, closed_on),

        # Nexudus AI help-desk integration
        "ai_processing_result":     _int(raw.get("AiProcessingResult")),
        "ai_channel_session_id":    _str(raw.get("AiChannelSessionId")),
        "support_issue_category":   _str(raw.get("SupportIssueCategory")),

        # Attachment
        "image_file_name":          _str(raw.get("ImageFileName")),

        # Audit
        "updated_by":               _str(raw.get("UpdatedBy")),
        "created_on":               created_on,
        "updated_on":               _parse_dt(raw.get("UpdatedOn")),
    }
