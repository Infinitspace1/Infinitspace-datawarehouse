"""
shared/firebase/transformers/competence.py

Transforms raw Firestore `competence_new` JSON (as stored in the bronze layer)
into typed silver row dicts. Pure functions — no I/O.

Two record kinds:
  - the parent list document        -> silver.competence_lists
  - a competitor (subcollection doc) -> silver.competence_competitors

The bronze raw_json was written with json.dumps(default=str), so Firestore
Timestamps arrive here as strings (e.g. "2024-01-15 10:30:45.123456+00:00").
`_parse_dt` is deliberately tolerant of that form (space separator, trailing
nanoseconds, optional Z/offset).
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

# Trim sub-microsecond fractional digits (Firestore nanosecond timestamps emit
# 9 digits; datetime.fromisoformat only accepts up to 6).
_FRACTION_RE = re.compile(r"(\.\d{6})\d+")


def _parse_dt(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    s = _FRACTION_RE.sub(r"\1", s.replace("Z", "+00:00"))
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        try:
            from dateutil import parser as _dtparser
            return _dtparser.parse(s)
        except Exception:
            return None


def _str(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


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


def _bit_opt(value) -> Optional[int]:
    """Nullable BIT: None stays NULL; otherwise truthiness -> 1/0."""
    if value is None:
        return None
    return 1 if value else 0


def transform_competence_list(
    raw: dict, list_id: str, bronze_id: int, sync_run_id: str
) -> dict:
    """Transform one raw competence_new parent doc into a silver row dict."""
    return {
        "source_id":             list_id,  # Firestore doc id is authoritative
        "uid":                   _str(raw.get("uid")),
        "competitor_list_name":  _str(raw.get("competitor_list_name")),
        "country":               _str(raw.get("country")),
        "country_code":          _str(raw.get("country_code")),
        "auto_managed":          _bit_opt(raw.get("auto_managed")),
        "status":                _str(raw.get("status")),
        "competitor_count":      _int(raw.get("competitor_count")),
        "schema_version":        _int(raw.get("schema_version")),
        "last_error":            _str(raw.get("last_error")),
        "created_at":            _parse_dt(raw.get("created_at")),
        "updated_at":            _parse_dt(raw.get("updated_at")),
        "last_run_at":           _parse_dt(raw.get("last_run_at")),
        "bronze_id":             bronze_id,
        "sync_run_id":           sync_run_id,
    }


def transform_competitor(
    raw: dict, source_id: str, list_source_id: str, bronze_id: int, sync_run_id: str
) -> dict:
    """Transform one raw competitor record into a silver row dict.

    `source_id` is the composite `{list_id}::{competitor_doc_id}` built by the
    reader; `list_source_id` is the parent list's Firestore doc id.
    """
    return {
        "source_id":          source_id,
        "list_source_id":     list_source_id,
        "place_id":           _str(raw.get("placeId")),
        "title":              _str(raw.get("title")),
        "category_name":      _str(raw.get("categoryName")),
        "address":            _str(raw.get("address")),
        "street":             _str(raw.get("street")),
        "city":               _str(raw.get("city")),
        "postal_code":        _str(raw.get("postalCode")),
        "country_code":       _str(raw.get("last_seen_country_code")),
        "phone":              _str(raw.get("phone")),
        "website":            _str(raw.get("website")),
        "google_maps_url":    _str(raw.get("googleMapsUrl")),
        "latitude":           _decimal(raw.get("latitude")),
        "longitude":          _decimal(raw.get("longitude")),
        "last_seen_at":       _parse_dt(raw.get("last_seen_at")),
        "last_seen_in_city":  _str(raw.get("last_seen_in_city")),
        "created_at":         _parse_dt(raw.get("created_at")),
        "updated_at":         _parse_dt(raw.get("updated_at")),
        "bronze_id":          bronze_id,
        "sync_run_id":        sync_run_id,
    }
