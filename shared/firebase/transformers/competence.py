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


# ── Country enrichment ───────────────────────────────────────────────────────
# Competitor docs rarely carry a country of their own (last_seen_country_code is
# mostly empty), but every competitor belongs to a per-country parent list
# (e.g. NL_AUTO -> Netherlands / NL). `resolve_competitor_country` derives both
# the country name and ISO2 code from that parent list, falling back to the
# list-id prefix and a small ISO map. Pure logic — the parent list's country/
# country_code are passed in by the caller (the silver writer / backfill).
#
# The parent list's `country` NAME is the primary source for the name, so this
# map is only a fallback (used when a list lacks a name, or when a competitor's
# own observed code differs from its list). Extend as new markets are added.
_ISO2_TO_NAME = {
    "NL": "Netherlands", "ES": "Spain", "IT": "Italy", "PL": "Poland",
    "DE": "Germany", "GB": "United Kingdom", "US": "United States",
    "FR": "France", "BE": "Belgium", "PT": "Portugal", "IE": "Ireland",
    "AT": "Austria", "CH": "Switzerland", "LU": "Luxembourg",
    "SE": "Sweden", "DK": "Denmark", "NO": "Norway", "FI": "Finland",
    "CZ": "Czechia", "RO": "Romania", "GR": "Greece", "HU": "Hungary",
    "BG": "Bulgaria", "HR": "Croatia", "SK": "Slovakia", "SI": "Slovenia",
    "LT": "Lithuania", "LV": "Latvia", "EE": "Estonia", "IS": "Iceland",
    "CA": "Canada", "AU": "Australia", "AE": "United Arab Emirates",
}


def _normalize_cc(value) -> Optional[str]:
    """Clean a country code: upper-cased, UK->GB alias, capped to the column
    width (NVARCHAR(8)). Returns None for blanks."""
    s = _str(value)
    if not s:
        return None
    s = s.upper()
    return "GB" if s == "UK" else s[:8]


def _iso2_from_list_id(list_source_id) -> Optional[str]:
    """Pull an ISO2 code from a list id like ``NL_AUTO`` -> ``NL`` (last-resort
    fallback when neither the competitor nor its list carries a code)."""
    s = _str(list_source_id)
    if not s:
        return None
    prefix = s.split("_", 1)[0].upper()
    if prefix == "UK":
        return "GB"
    return prefix if len(prefix) == 2 and prefix.isalpha() else None


def _iso2_to_name(code: Optional[str]) -> Optional[str]:
    return _ISO2_TO_NAME.get(code.upper()) if code else None


# Name -> ISO2, built from the inverse of _ISO2_TO_NAME plus common aliases /
# spellings. In practice the competence_new parent lists carry a free-text
# country NAME and no code at all (e.g. "USA", "United States", "United
# Kingdom"), so this is the primary way a country_code gets filled. Lower-cased
# keys; matched case-insensitively.
_NAME_TO_ISO2 = {name.lower(): code for code, name in _ISO2_TO_NAME.items()}
_NAME_TO_ISO2.update({
    "usa": "US", "u.s.a.": "US", "u.s.": "US", "us": "US", "america": "US",
    "united states of america": "US",
    "uk": "GB", "u.k.": "GB", "great britain": "GB", "britain": "GB",
    "england": "GB", "scotland": "GB", "wales": "GB",
    "the netherlands": "NL", "holland": "NL",
    "deutschland": "DE",
    "espana": "ES", "españa": "ES",
    "polska": "PL",
    "czech republic": "CZ",
    "uae": "AE",
})


def _name_to_iso2(name) -> Optional[str]:
    s = _str(name)
    return _NAME_TO_ISO2.get(s.lower()) if s else None


def resolve_competitor_country(
    own_country_code,
    list_source_id,
    list_country_name=None,
    list_country_code=None,
) -> tuple[Optional[str], Optional[str]]:
    """Resolve ``(country_name, country_code)`` for a competitor.

    Code precedence: the competitor's own observed code (``last_seen_country_code``)
    wins when present, then the parent list's code, then the list's country NAME
    mapped to ISO2 (the usual path — lists carry a free-text name and no code),
    then the list-id prefix (``NL_AUTO`` -> ``NL``).
    Name precedence: the parent list's country name (authoritative) when its code
    agrees with the resolved code, otherwise the canonical ISO name (so "USA" and
    "United States" both normalise to "United States"), falling back to the raw
    list name when the code is unknown. Used identically by the silver writer and
    the backfill so both paths produce the same values.
    """
    own = _normalize_cc(own_country_code)
    list_code = _normalize_cc(list_country_code)
    list_name = _str(list_country_name)
    code = own or list_code or _name_to_iso2(list_name) or _iso2_from_list_id(list_source_id)

    if code and list_code and code == list_code:
        name = list_name or _iso2_to_name(code)
    elif code:
        name = _iso2_to_name(code) or list_name
    else:
        name = list_name
    return name, code


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
    raw: dict,
    source_id: str,
    list_source_id: str,
    bronze_id: int,
    sync_run_id: str,
    list_country_name=None,
    list_country_code=None,
) -> dict:
    """Transform one raw competitor record into a silver row dict.

    `source_id` is the composite `{list_id}::{competitor_doc_id}` built by the
    reader; `list_source_id` is the parent list's Firestore doc id.

    `list_country_name`/`list_country_code` are the parent list's country (passed
    in by the silver writer from silver.competence_lists). They drive the country
    enrichment — the competitor's own `last_seen_country_code` is mostly empty, so
    the country is derived from its per-country parent list.
    """
    country_name, country_code = resolve_competitor_country(
        raw.get("last_seen_country_code"),
        list_source_id,
        list_country_name,
        list_country_code,
    )
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
        "country":            country_name,
        "country_code":       country_code,
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
