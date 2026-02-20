"""
shared/nexudus/transformers/locations.py

Transforms raw bronze.nexudus_locations JSON into
typed rows for silver.nexudus_locations and silver.nexudus_location_hours.

No I/O here — pure transformation functions.
Input:  raw dict from Nexudus API (parsed from bronze.raw_json)
Output: typed dicts ready to MERGE into silver tables
"""
import re
from datetime import datetime, timezone
from typing import Optional


# ── Locations to exclude from silver ─────────────────────────
# Root business account and demo locations that exist in Nexudus
# but are not real physical locations.
# Add source_id (Nexudus Id) here to exclude from silver.
EXCLUDED_SOURCE_IDS: set[int] = {
    1376491116,   # (beyond Global) — root business account
    1376491117,   # beyond Demo     — demo/test location
}

# ── Days config ───────────────────────────────────────────────
# (day_number, day_name, nexudus_open_key, nexudus_close_key, nexudus_closed_key)
DAYS = [
    (1, "Monday",    "MondayOpenTime",    "MondayCloseTime",    "MondayClosed"),
    (2, "Tuesday",   "TuesdayOpenTime",   "TuesdayCloseTime",   "TuesdayClosed"),
    (3, "Wednesday", "WednesdayOpenTime", "WednesdayCloseTime", "WednesdayClosed"),
    (4, "Thursday",  "ThursdayOpenTime",  "ThursdayCloseTime",  "ThursdayClosed"),
    (5, "Friday",    "FridayOpenTime",    "FridayCloseTime",    "FridayClosed"),
    (6, "Saturday",  "SaturdayOpenTime",  "SaturdayCloseTime",  "SaturdayClosed"),
    (7, "Sunday",    "SundayOpenTime",    "SundayCloseTime",    "SundayClosed"),
]

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


# ── Helpers ───────────────────────────────────────────────────

def _strip_html(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = _HTML_TAG_RE.sub(" ", value)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text or None


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        # Nexudus returns ISO 8601 with Z suffix
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _str(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


# ── Main transforms ───────────────────────────────────────────

def transform_location(
    raw: dict,
    bronze_id: int,
    sync_run_id: str,
) -> Optional[dict]:
    """
    Transform one raw Nexudus location record into a silver row dict.
    Returns None if the location should be excluded from silver.
    Keys match silver.nexudus_locations columns exactly.
    """
    source_id = raw.get("Id")
    if source_id in EXCLUDED_SOURCE_IDS:
        return None
    return {
        "source_id":    raw["Id"],
        "bronze_id":    bronze_id,
        "sync_run_id":  sync_run_id,

        # Identity
        "nexudus_uuid": _str(raw.get("UniqueId")),
        "name":         _str(raw.get("Name")) or _str(raw.get("ToStringText")) or "Unknown",
        "web_address":  _str(raw.get("WebAddress")),

        # Location
        "address":      _str(raw.get("Address")),
        "postal_code":  _str(raw.get("PostalCode")),
        "city":         _str(raw.get("TownCity")),
        "state":        _str(raw.get("State")),
        "country_name": _str(raw.get("CountryName")),
        "country_id":   raw.get("CountryId"),
        "latitude":     raw.get("Latitude"),
        "longitude":    raw.get("Longitude"),

        # Contact
        "phone":        _str(raw.get("Phone")),
        "email":        _str(raw.get("EmailContact")),
        "web_contact":  _str(raw.get("WebContact")),

        # Financial
        "currency_code": _str(raw.get("CurrencyCode")),

        # Content — strip HTML tags
        "description":  _strip_html(raw.get("AboutUs")),
        "short_intro":  _strip_html(raw.get("ShortIntroduction")),

        # Timestamps
        "created_on":   _parse_dt(raw.get("CreatedOn")),
        "updated_on":   _parse_dt(raw.get("UpdatedOn")),
    }


def transform_location_hours(raw: dict) -> Optional[list[dict]]:
    """
    Transform one raw location record into 7 opening-hours rows.
    Returns None if the location is excluded.
    """
    if raw.get("Id") in EXCLUDED_SOURCE_IDS:
        return None

    location_source_id = raw["Id"]
    rows = []

    for day_num, day_name, open_key, close_key, closed_key in DAYS:
        is_closed  = bool(raw.get(closed_key, False))
        open_time  = raw.get(open_key)    # int minutes, or None
        close_time = raw.get(close_key)   # int minutes, or None

        # Treat 0/0 as null (Nexudus sometimes sends 0 instead of None)
        if open_time == 0 and close_time == 0:
            open_time = close_time = None

        rows.append({
            "location_source_id": location_source_id,
            "day_of_week":        day_num,
            "day_name":           day_name,
            "is_closed":          1 if is_closed else 0,
            "open_time":          open_time,
            "close_time":         close_time,
        })

    return rows