"""
Broker directory — persistent broker name -> email memory for LoopNet.

Why: the memo23 LoopNet actor has twice (2026-06-27, ~2026-07-13) shipped
payload changes that dropped broker contact fields. Every broker email ever
observed is remembered in silver.location_scraper_broker_directory, keyed by
normalized broker name, so a listing that arrives with a broker NAME but no
EMAIL can be back-filled from history (86% of today's name-without-email items
resolve). The directory is self-enriching: every globe materialization of a
LoopNet run upserts the (name, email) pairs it sees.

Resolution is deliberately conservative: a name mapping to several distinct
emails is only resolved when the listing's brokerCompany disambiguates it —
otherwise no email is returned (mailing the wrong person is worse than
mailing nobody).
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import pyodbc

logger = logging.getLogger(__name__)

_UPSERT_RECORD = """
MERGE silver.location_scraper_broker_directory WITH (HOLDLOCK) AS target
USING (SELECT ? AS name_normalized, ? AS email) AS src
    ON target.name_normalized = src.name_normalized AND target.email = src.email
WHEN NOT MATCHED THEN
    INSERT (name_normalized, name_display, email, company, phone, source)
    VALUES (src.name_normalized, ?, src.email, ?, ?, ?)
WHEN MATCHED THEN
    UPDATE SET
        seen_count = target.seen_count + 1,
        last_seen_at = GETUTCDATE(),
        name_display = COALESCE(?, target.name_display),
        company = COALESCE(?, target.company),
        phone = COALESCE(?, target.phone);
"""

_READ_DIRECTORY = """
SELECT name_normalized, name_display, email, company, phone, last_seen_at
FROM silver.location_scraper_broker_directory
"""


def normalize_broker_name(name: Any) -> Optional[str]:
    """Lowercased, whitespace-collapsed broker name; None when empty."""
    if name is None:
        return None
    normalized = " ".join(str(name).lower().split())
    return normalized or None


def _norm_company(company: Any) -> Optional[str]:
    if company is None:
        return None
    normalized = " ".join(str(company).lower().split())
    return normalized or None


def extract_broker_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """(name, email, company, phone) pairs observed in one LoopNet payload.

    Covers the flat fields (brokerName/brokerEmail/...) and the `brokers` list
    of co-brokers added by the actor's 2026-07 rework. Only pairs with BOTH a
    name and an email are directory-worthy.
    """
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def _add(name: Any, email: Any, company: Any = None, phone: Any = None) -> None:
        name_norm = normalize_broker_name(name)
        email_clean = str(email).strip().lower() if email else ""
        if not name_norm or not email_clean or "@" not in email_clean:
            return
        key = (name_norm, email_clean)
        if key in seen:
            return
        seen.add(key)
        records.append(
            {
                "name_normalized": name_norm,
                "name_display": " ".join(str(name).split()),
                "email": email_clean,
                "company": str(company).strip() if company else None,
                "phone": str(phone).strip() if phone else None,
            }
        )

    _add(
        payload.get("brokerName"),
        payload.get("brokerEmail"),
        payload.get("brokerCompany"),
        payload.get("brokerPhone"),
    )
    for broker in payload.get("brokers") or []:
        if isinstance(broker, dict):
            _add(broker.get("name"), broker.get("email"), broker.get("company"), broker.get("phone"))
    return records


def resolve_email(
    directory: dict[str, list[dict[str, Any]]],
    name: Any,
    company: Any = None,
) -> Optional[dict[str, Any]]:
    """Resolve a broker name to a single directory record, or None.

    - one distinct email for the name -> that record;
    - several -> only resolved when `company` matches exactly one email;
    - anything still ambiguous -> None (never guess).
    """
    name_norm = normalize_broker_name(name)
    if not name_norm:
        return None
    candidates = directory.get(name_norm)
    if not candidates:
        return None

    emails = {c["email"] for c in candidates}
    if len(emails) == 1:
        return candidates[0]

    company_norm = _norm_company(company)
    if company_norm:
        matching = [c for c in candidates if _norm_company(c.get("company")) == company_norm]
        if len({c["email"] for c in matching}) == 1:
            return matching[0]
    return None


def load_broker_directory(sql) -> dict[str, list[dict[str, Any]]]:
    """Full directory grouped by normalized name. Empty dict when the table
    is missing (deploy-before-apply convention)."""
    try:
        rows = sql.execute_query(_READ_DIRECTORY)
    except pyodbc.ProgrammingError as exc:
        sqlstate = exc.args[0] if exc.args else ""
        if sqlstate == "42S02":
            logger.warning(
                "broker directory table missing; run location_scraper_broker_directory.sql — lookups disabled"
            )
            return {}
        raise
    directory: dict[str, list[dict[str, Any]]] = {}
    for row in rows or []:
        directory.setdefault(row["name_normalized"], []).append(dict(row))
    return directory


def upsert_broker_records(sql, records: list[dict[str, Any]]) -> int:
    """MERGE observed (name, email) pairs into the directory. Returns the
    number of records attempted; 0 with a warning when the table is missing."""
    if not records:
        return 0
    params = [
        (
            r["name_normalized"],
            r["email"],
            r.get("name_display"),
            r.get("company"),
            r.get("phone"),
            r.get("source") or "loopnet",
            r.get("name_display"),
            r.get("company"),
            r.get("phone"),
        )
        for r in records
    ]
    try:
        sql.execute_many(_UPSERT_RECORD, params)
    except pyodbc.ProgrammingError as exc:
        sqlstate = exc.args[0] if exc.args else ""
        if sqlstate == "42S02":
            logger.warning(
                "broker directory table missing; run location_scraper_broker_directory.sql — upserts skipped"
            )
            return 0
        raise
    return len(params)
