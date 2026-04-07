"""
shared/nexudus/schemas.py

Typed data structures used by Nexudus colleague-location sync.
"""
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class AccessibleBusiness:
    business_id: int
    name: Optional[str]
    slug: Optional[str]
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class ParsedCoworker:
    nexudus_coworker_id: int
    team_business_id: Optional[int]
    full_name: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    status: Optional[str]
    raw_payload: dict[str, Any]
    accessible_businesses: list[AccessibleBusiness]
