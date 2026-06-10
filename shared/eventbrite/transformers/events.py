"""
shared/eventbrite/transformers/events.py

Transforms raw bronze.eventbrite_events JSON into a typed dict for
silver.eventbrite_events.

Pure function — no I/O.

The raw payload is an Eventbrite v3 event fetched with
expand=venue,ticket_availability,organizer,format,category — so venue
address, ticket price range and organizer name are embedded and fully
flattened here. No JSON columns in silver: the raw payload is always
available in bronze.eventbrite_events.raw_json.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional


def _parse_dt(value) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
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


def _multipart_text(value) -> Optional[str]:
    """Eventbrite text fields come as {"text": ..., "html": ...}."""
    if isinstance(value, dict):
        return _str(value.get("text"))
    return _str(value)


def _multipart_html(value) -> Optional[str]:
    if isinstance(value, dict):
        return _str(value.get("html"))
    return None


def _price(value) -> tuple[Optional[float], Optional[str]]:
    """Eventbrite money fields: {"currency", "value" (minor units),
    "major_value" (decimal string), "display"}."""
    if not isinstance(value, dict):
        return None, None
    major = _decimal(value.get("major_value"))
    if major is None and value.get("value") is not None:
        minor = _decimal(value.get("value"))
        major = minor / 100.0 if minor is not None else None
    return major, _str(value.get("display"))


def _price_currency(value) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    return _str(value.get("currency"))


def _price_minor(value) -> Optional[int]:
    if not isinstance(value, dict):
        return None
    return _int(value.get("value"))


def _joined_lines(value) -> Optional[str]:
    if not isinstance(value, list):
        return None
    lines = [_str(line) for line in value]
    lines = [line for line in lines if line]
    return "\n".join(lines) if lines else None


def transform_event(raw: dict, bronze_id: int, sync_run_id: str) -> dict:
    """Transform one raw Eventbrite event record into a silver row dict."""
    name = raw.get("name") or {}
    description = raw.get("description") or {}
    start = raw.get("start") or {}
    end = raw.get("end") or {}
    logo = raw.get("logo") or {}
    organizer = raw.get("organizer") or {}
    venue = raw.get("venue") or {}
    venue_address = venue.get("address") or {}
    ticket_availability = raw.get("ticket_availability") or {}
    min_ticket = ticket_availability.get("minimum_ticket_price")
    max_ticket = ticket_availability.get("maximum_ticket_price")
    min_price, min_price_display = _price(min_ticket)
    max_price, max_price_display = _price(max_ticket)
    sales_start = ticket_availability.get("start_sales_date") or {}

    return {
        # Source
        "source_id":                _str(raw.get("id")),
        "bronze_id":                bronze_id,
        "sync_run_id":              sync_run_id,

        # Organization / organizer
        "organization_id":          _str(raw.get("organization_id")),
        "organizer_id":             _str(raw.get("organizer_id")),
        "organizer_name":           _str(organizer.get("name")),

        # Identity
        "name":                     _multipart_text(name) or "",
        "summary":                  _str(raw.get("summary")),
        "description_text":         _multipart_text(description),
        "description_html":         _multipart_html(description),
        "url":                      _str(raw.get("url")),
        "status":                   _str(raw.get("status")),
        "currency":                 _str(raw.get("currency")),

        # Schedule
        "start_utc":                _parse_dt(start.get("utc")),
        "start_local":              _parse_dt(start.get("local")),
        "end_utc":                  _parse_dt(end.get("utc")),
        "end_local":                _parse_dt(end.get("local")),
        "timezone":                 _str(start.get("timezone")),

        # Lifecycle
        "created":                  _parse_dt(raw.get("created")),
        "changed":                  _parse_dt(raw.get("changed")),
        "published":                _parse_dt(raw.get("published")),

        # Flags
        "online_event":             _bit(raw.get("online_event")),
        "listed":                   _bit(raw.get("listed")),
        "shareable":                _bit(raw.get("shareable")),
        "is_free":                  _bit(raw.get("is_free")),
        "is_series":                _bit(raw.get("is_series")),
        "is_series_parent":         _bit(raw.get("is_series_parent")),
        "hide_start_date":          _bit(raw.get("hide_start_date")),
        "hide_end_date":            _bit(raw.get("hide_end_date")),

        # Capacity
        "capacity":                 _int(raw.get("capacity")),
        "capacity_is_custom":       _bit(raw.get("capacity_is_custom")),

        # Series link
        "series_id":                _str(raw.get("series_id")),

        # Classification
        "format_id":                _str(raw.get("format_id")),
        "format_name":              _str((raw.get("format") or {}).get("name")),
        "category_id":              _str(raw.get("category_id")),
        "category_name":            _str((raw.get("category") or {}).get("name")),
        "subcategory_id":           _str(raw.get("subcategory_id")),

        # Venue (from expand=venue, fully flattened)
        "venue_id":                 _str(raw.get("venue_id")) or _str(venue.get("id")),
        "venue_resource_uri":       _str(venue.get("resource_uri")),
        "venue_name":               _str(venue.get("name")),
        "venue_address":            _str(venue_address.get("localized_address_display"))
                                    or _str(venue_address.get("address_1")),
        "venue_address_1":          _str(venue_address.get("address_1")),
        "venue_address_2":          _str(venue_address.get("address_2")),
        "venue_city":               _str(venue_address.get("city")),
        "venue_region":             _str(venue_address.get("region")),
        "venue_postal_code":        _str(venue_address.get("postal_code")),
        "venue_country":            _str(venue_address.get("country")),
        "venue_address_latitude":   _decimal(venue_address.get("latitude")),
        "venue_address_longitude":  _decimal(venue_address.get("longitude")),
        "venue_localized_area":     _str(venue_address.get("localized_area_display")),
        "venue_multi_line_address": _joined_lines(venue_address.get("localized_multi_line_address_display")),
        "venue_latitude":           _decimal(venue.get("latitude") or venue_address.get("latitude")),
        "venue_longitude":          _decimal(venue.get("longitude") or venue_address.get("longitude")),
        "venue_capacity":           _int(venue.get("capacity")),
        "venue_age_restriction":    _str(venue.get("age_restriction")),

        # Tickets (from expand=ticket_availability, fully flattened)
        "has_available_tickets":    _bit(ticket_availability.get("has_available_tickets")),
        "is_sold_out":              _bit(ticket_availability.get("is_sold_out")),
        "waitlist_available":       _bit(ticket_availability.get("waitlist_available")),
        "minimum_ticket_price":     min_price,
        "minimum_ticket_price_display": min_price_display,
        "minimum_ticket_price_currency": _price_currency(min_ticket),
        "minimum_ticket_price_minor": _price_minor(min_ticket),
        "maximum_ticket_price":     max_price,
        "maximum_ticket_price_display": max_price_display,
        "maximum_ticket_price_currency": _price_currency(max_ticket),
        "maximum_ticket_price_minor": _price_minor(max_ticket),
        "ticket_currency":          _price_currency(min_ticket) or _price_currency(max_ticket),
        "sales_start_utc":          _parse_dt(sales_start.get("utc") if isinstance(sales_start, dict) else sales_start),
        "sales_start_local":        _parse_dt(sales_start.get("local") if isinstance(sales_start, dict) else None),
        "sales_start_timezone":     _str(sales_start.get("timezone") if isinstance(sales_start, dict) else None),

        # Media
        "logo_url":                 _str(logo.get("url"))
                                    or _str((logo.get("original") or {}).get("url")),
    }
