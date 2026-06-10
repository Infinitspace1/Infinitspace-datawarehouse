"""Nexudus source records intentionally hidden from silver outputs."""

from __future__ import annotations

from typing import Any


EXCLUDED_LOCATION_SOURCE_IDS: set[int] = {
    1376491116,  # (beyond Global) - root business account
    1376491117,  # beyond Demo - demo/test location
    1414964752,  # London - Holborn - 229-231 High Holborn / Kingsbourne House
}


def is_excluded_location_source_id(value: Any) -> bool:
    if value in (None, ""):
        return False
    try:
        return int(value) in EXCLUDED_LOCATION_SOURCE_IDS
    except (TypeError, ValueError):
        return False

