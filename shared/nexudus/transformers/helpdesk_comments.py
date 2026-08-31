"""
shared/nexudus/transformers/helpdesk_comments.py

Transforms raw bronze.nexudus_helpdesk_comments JSON into a typed dict
for silver.nexudus_helpdesk_comments.

Source endpoint: GET /api/support/helpdeskcomments

A HelpDeskComment is one reply on a help-desk ticket. Both the member and
the staff responder post comments, so `coworker_source_id` is "who wrote
it", not "the customer" — use `is_internal` / `updated_by` to distinguish.

Linking:
  - HelpDeskMessageId -> silver.nexudus_helpdesk_messages.source_id
  - CoworkerId        -> silver.nexudus_coworkers.source_id
  - The raw payload carries NO BusinessId; location_source_id is derived by
    the silver writer from the parent message's BusinessId and passed in
    here, so comments can be filtered by location without a join (same
    approach as event_products inheriting from its calendar event).

Fields deliberately excluded (verified against live payloads 2026-08-20):
  - ClearImageFile, NewImageUrl       -> write-only API fields
  - CustomFields, LocalizationDetails -> always null
  - IsNew, SystemId                   -> internal
  - ToStringText                      -> duplicate of MessageText
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


def transform_helpdesk_comment(
    raw: dict,
    bronze_id: int,
    sync_run_id: str,
    location_source_id: Optional[int] = None,
) -> dict:
    """Transform one raw Nexudus HelpDeskComment record into a silver row dict.

    `location_source_id` is resolved by the caller from the parent message —
    the comment payload has no BusinessId of its own.
    """
    return {
        # Source
        "source_id":                    raw["Id"],
        "unique_id":                    _str(raw.get("UniqueId")),
        "bronze_id":                    bronze_id,
        "sync_run_id":                  sync_run_id,

        # Parent ticket (soft FK -> silver.nexudus_helpdesk_messages.source_id)
        "helpdesk_message_source_id":   raw["HelpDeskMessageId"],

        # Location, inherited from the parent message
        "location_source_id":           _int(location_source_id),

        # Author (soft FK -> silver.nexudus_coworkers.source_id)
        "coworker_source_id":           _int(raw.get("CoworkerId")),
        "coworker_full_name":           _str(raw.get("CoworkerFullName")),

        # Content
        "message_text":                 _str(raw.get("MessageText")),
        "is_internal":                  _bit(raw.get("Internal")),

        # Attachment
        "image_file_name":              _str(raw.get("ImageFileName")),

        # Audit
        "updated_by":                   _str(raw.get("UpdatedBy")),
        "created_on":                   _parse_dt(raw.get("CreatedOn")),
        "updated_on":                   _parse_dt(raw.get("UpdatedOn")),
    }
