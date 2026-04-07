"""
shared/xero/tenant_directory.py

Refreshes the warehouse tenant directory by combining Xero tenant metadata
with the best matching Nexudus silver location.
"""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse

from shared.xero.store import XeroStore

DEFAULT_OWNER_TYPE = "workspace"
DEFAULT_OWNER_ID = "default"

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_NOISY_MATCH_TOKENS = {
    "amsterdam",
    "and",
    "berlin",
    "beyond",
    "bv",
    "center",
    "centre",
    "company",
    "england",
    "germany",
    "gmbh",
    "infinitspace",
    "kingdom",
    "limited",
    "london",
    "ltd",
    "netherlands",
    "old",
    "organisation",
    "space",
    "street",
    "the",
    "uk",
    "united",
}

# Legal-entity tenant names do not always mirror the Nexudus marketing/location names.
# These aliases provide stable hints without hard-coding Nexudus source_ids.
_TENANT_ALIAS_HINTS: dict[str, set[str]] = {
    "beyond the bower": {"the bower", "thebower", "old street"},
    "beyond quartier heidestrasse gmbh": {
        "quartier heidestrasse",
        "heidestrasse",
        "qh",
    },
    "infinitspace aldgate uk ltd": {"aldgate", "aldgate tower", "leman street"},
    "infinitspace germany two gmbh c29": {"chausseestrasse", "c29", "29"},
    "infinitspace herengracht 471 bv": {"herengracht 471", "gouden bocht", "471"},
}


def _as_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_text(value: Any) -> str:
    text = _as_text(value)
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.lower().replace("&", " and ")
    ascii_text = _NON_ALNUM_RE.sub(" ", ascii_text)
    return " ".join(ascii_text.split())


def _compact_text(value: Any) -> str:
    return _NON_ALNUM_RE.sub("", _normalize_text(value))


def _tokenize(value: Any) -> set[str]:
    normalized = _normalize_text(value)
    if not normalized:
        return set()
    tokens = set(normalized.split())
    digits = {digit for token in tokens for digit in [_digits_only(token)] if digit}
    return tokens | digits


def _digits_only(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit())


def _email_aliases(email: Any) -> set[str]:
    text = _as_text(email)
    if not text or "@" not in text:
        return set()
    local = text.split("@", 1)[0]
    aliases = {local}
    for suffix in (".reception", ".frontdesk", ".team", ".manager"):
        if local.endswith(suffix):
            aliases.add(local[: -len(suffix)])
    return {alias for alias in aliases if alias}


def _url_aliases(url: Any) -> set[str]:
    text = _as_text(url)
    if not text:
        return set()
    parsed = urlparse(text)
    aliases: set[str] = set()
    if parsed.netloc:
        aliases.add(parsed.netloc)
    if parsed.path:
        parts = [part for part in parsed.path.split("/") if part]
        aliases.update(parts)
        if parts:
            aliases.add(parts[-1])
    return aliases


def _name_segments(name: Any) -> set[str]:
    text = _as_text(name)
    if not text:
        return set()
    return {segment.strip() for segment in text.split(" - ") if segment.strip()}


@dataclass(frozen=True)
class _SearchIndex:
    row: dict[str, Any]
    tokens: frozenset[str]
    compacts: frozenset[str]
    numeric_tokens: frozenset[str]


def _build_index(row: dict[str, Any], alias_values: set[str]) -> _SearchIndex:
    tokens: set[str] = set()
    compacts: set[str] = set()
    numeric_tokens: set[str] = set()

    for value in alias_values:
        normalized = _normalize_text(value)
        if not normalized:
            continue
        value_tokens = _tokenize(normalized)
        tokens.update(value_tokens)
        digits = {_digits_only(token) for token in value_tokens}
        numeric_tokens.update({digit for digit in digits if digit})

        compact = _compact_text(normalized)
        if compact:
            compacts.add(compact)

    return _SearchIndex(
        row=row,
        tokens=frozenset(tokens),
        compacts=frozenset(compacts),
        numeric_tokens=frozenset(numeric_tokens),
    )


def _location_alias_values(location: dict[str, Any]) -> set[str]:
    aliases: set[str] = set()
    for key in (
        "name",
        "web_address",
        "address",
        "postal_code",
        "city",
        "state",
        "country_name",
        "email",
        "web_contact",
    ):
        value = _as_text(location.get(key))
        if value:
            aliases.add(value)

    aliases.update(_name_segments(location.get("name")))
    aliases.update(_email_aliases(location.get("email")))
    aliases.update(_url_aliases(location.get("web_contact")))
    return aliases


def _tenant_alias_values(tenant: dict[str, Any]) -> set[str]:
    aliases: set[str] = set()
    tenant_name = _as_text(tenant.get("tenant_name"))
    if tenant_name:
        aliases.add(tenant_name)
    aliases.update(_TENANT_ALIAS_HINTS.get(_normalize_text(tenant_name), set()))
    raw_payload = tenant.get("raw_payload")
    if raw_payload:
        try:
            payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, dict):
            for key in ("tenantName", "tenantType", "tenantId"):
                value = _as_text(payload.get(key))
                if value:
                    aliases.add(value)
    return aliases


def _score_candidate(tenant_index: _SearchIndex, location_index: _SearchIndex) -> tuple[int, str]:
    exact_aliases = {
        alias
        for alias in tenant_index.compacts & location_index.compacts
        if len(alias) >= 3
    }
    if exact_aliases:
        alias = sorted(exact_aliases, key=len, reverse=True)[0]
        return 260 + len(alias), f"alias_exact:{alias}"

    contains_matches: set[str] = set()
    for tenant_alias in tenant_index.compacts:
        if len(tenant_alias) < 3:
            continue
        for location_alias in location_index.compacts:
            if len(location_alias) < 3:
                continue
            if tenant_alias in location_alias or location_alias in tenant_alias:
                contains_matches.add(
                    tenant_alias if len(tenant_alias) >= len(location_alias) else location_alias
                )
    if contains_matches:
        alias = sorted(contains_matches, key=len, reverse=True)[0]
        return 180 + len(alias), f"alias_contains:{alias}"

    shared_digits = tenant_index.numeric_tokens & location_index.numeric_tokens
    shared_tokens = {
        token
        for token in tenant_index.tokens & location_index.tokens
        if len(token) >= 3 and token not in _NOISY_MATCH_TOKENS
    }

    score = 0
    rules: list[str] = []
    if shared_digits:
        digits = ",".join(sorted(shared_digits))
        score += 120 * len(shared_digits)
        rules.append(f"digits:{digits}")
    if shared_tokens:
        tokens = ",".join(sorted(shared_tokens))
        score += 45 * len(shared_tokens)
        rules.append(f"tokens:{tokens}")

    return score, ";".join(rules)


def _select_location_match(
    tenant: dict[str, Any],
    location_indexes: list[_SearchIndex],
) -> tuple[Optional[dict[str, Any]], str]:
    tenant_index = _build_index(tenant, _tenant_alias_values(tenant))
    scored: list[tuple[int, str, dict[str, Any]]] = []

    for location_index in location_indexes:
        score, match_rule = _score_candidate(tenant_index, location_index)
        if score <= 0:
            continue
        scored.append((score, match_rule, location_index.row))

    if not scored:
        return None, "unmatched"

    scored.sort(key=lambda item: (item[0], int(item[2].get("source_id") or 0)), reverse=True)
    best_score, best_rule, best_row = scored[0]
    if best_score < 120:
        return None, "unmatched"
    if len(scored) > 1 and best_score == scored[1][0]:
        return None, f"ambiguous:{best_score}"
    return best_row, best_rule


class XeroTenantDirectoryService:
    def __init__(self, sql_client=None, store: Optional[XeroStore] = None):
        if sql_client is None:
            from shared.azure_clients.sql_client import get_sql_client

            sql_client = get_sql_client()
        self.sql = sql_client
        self.store = store or XeroStore(sql_client=sql_client)

    def build_directory_rows(
        self,
        tenants: list[dict[str, Any]],
        locations: Optional[list[dict[str, Any]]] = None,
    ) -> list[dict[str, Any]]:
        locations = locations or self._load_locations()
        location_indexes = [_build_index(location, _location_alias_values(location)) for location in locations]
        rows: list[dict[str, Any]] = []

        for tenant in tenants:
            matched_location, match_rule = _select_location_match(tenant, location_indexes)
            row = {
                "xero_connection_id": tenant["xero_connection_id"],
                "xero_tenant_id": tenant["xero_tenant_id"],
                "xero_tenant_type": tenant.get("xero_tenant_type"),
                "tenant_name": tenant.get("tenant_name"),
                "tenant_connection_id": tenant.get("connection_id"),
                "auth_event_id": tenant.get("auth_event_id"),
                "raw_tenant_payload": tenant.get("raw_payload"),
                "location_source_id": None,
                "location_nexudus_uuid": None,
                "location_name": None,
                "location_web_address": None,
                "location_address": None,
                "location_postal_code": None,
                "location_city": None,
                "location_state": None,
                "location_country_name": None,
                "location_currency_code": None,
                "location_email": None,
                "location_web_contact": None,
                "location_match_rule": match_rule,
            }

            if matched_location:
                row.update(
                    {
                        "location_source_id": matched_location.get("source_id"),
                        "location_nexudus_uuid": matched_location.get("nexudus_uuid"),
                        "location_name": matched_location.get("name"),
                        "location_web_address": matched_location.get("web_address"),
                        "location_address": matched_location.get("address"),
                        "location_postal_code": matched_location.get("postal_code"),
                        "location_city": matched_location.get("city"),
                        "location_state": matched_location.get("state"),
                        "location_country_name": matched_location.get("country_name"),
                        "location_currency_code": matched_location.get("currency_code"),
                        "location_email": matched_location.get("email"),
                        "location_web_contact": matched_location.get("web_contact"),
                    }
                )

            rows.append(row)

        return rows

    def refresh_tenants(
        self,
        owner_type: str = DEFAULT_OWNER_TYPE,
        owner_id: str = DEFAULT_OWNER_ID,
        connection_id: Optional[int] = None,
    ) -> dict[str, Any]:
        connection = self.store.get_connection(
            connection_id=connection_id,
            owner_type=None if connection_id is not None else owner_type,
            owner_id=None if connection_id is not None else owner_id,
        )
        if connection is None:
            raise ValueError("No Xero connection found for the requested owner")

        tenants = self.store.list_tenants(connection_id=connection.id)
        rows = self.build_directory_rows(tenants)

        for row in rows:
            self._upsert_directory_row(row)

        stale_deleted = self._delete_stale_rows(connection.id, [row["xero_tenant_id"] for row in rows])
        unmatched = [row for row in rows if row["location_source_id"] is None]

        return {
            "connection_id": connection.id,
            "tenant_count": len(rows),
            "matched_count": len(rows) - len(unmatched),
            "unmatched_tenant_ids": [row["xero_tenant_id"] for row in unmatched],
            "unmatched_tenant_names": [row.get("tenant_name") for row in unmatched],
            "deleted_stale_rows": stale_deleted,
        }

    def _load_locations(self) -> list[dict[str, Any]]:
        return self.sql.execute_query(
            """
            SELECT
                source_id,
                nexudus_uuid,
                name,
                web_address,
                address,
                postal_code,
                city,
                state,
                country_name,
                currency_code,
                email,
                web_contact
            FROM silver.nexudus_locations
            ORDER BY source_id
            """
        )

    def _upsert_directory_row(self, row: dict[str, Any]) -> None:
        self.sql.execute_non_query(
            """
            MERGE silver.xero_tenants AS target
            USING (
                SELECT ? AS xero_connection_id, ? AS xero_tenant_id
            ) AS source
                ON target.xero_connection_id = source.xero_connection_id
               AND target.xero_tenant_id = source.xero_tenant_id
            WHEN MATCHED THEN UPDATE SET
                xero_tenant_type = ?,
                tenant_name = ?,
                tenant_connection_id = ?,
                auth_event_id = ?,
                raw_tenant_payload = ?,
                location_source_id = ?,
                location_nexudus_uuid = ?,
                location_name = ?,
                location_web_address = ?,
                location_address = ?,
                location_postal_code = ?,
                location_city = ?,
                location_state = ?,
                location_country_name = ?,
                location_currency_code = ?,
                location_email = ?,
                location_web_contact = ?,
                location_match_rule = ?,
                community_manager_name = target.community_manager_name,
                last_synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                xero_connection_id,
                xero_tenant_id,
                xero_tenant_type,
                tenant_name,
                tenant_connection_id,
                auth_event_id,
                raw_tenant_payload,
                location_source_id,
                location_nexudus_uuid,
                location_name,
                location_web_address,
                location_address,
                location_postal_code,
                location_city,
                location_state,
                location_country_name,
                location_currency_code,
                location_email,
                location_web_contact,
                community_manager_name,
                location_match_rule
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            );
            """,
            (
                row["xero_connection_id"],
                row["xero_tenant_id"],
                row.get("xero_tenant_type"),
                row.get("tenant_name"),
                row.get("tenant_connection_id"),
                row.get("auth_event_id"),
                row.get("raw_tenant_payload"),
                row.get("location_source_id"),
                row.get("location_nexudus_uuid"),
                row.get("location_name"),
                row.get("location_web_address"),
                row.get("location_address"),
                row.get("location_postal_code"),
                row.get("location_city"),
                row.get("location_state"),
                row.get("location_country_name"),
                row.get("location_currency_code"),
                row.get("location_email"),
                row.get("location_web_contact"),
                row.get("location_match_rule"),
                row["xero_connection_id"],
                row["xero_tenant_id"],
                row.get("xero_tenant_type"),
                row.get("tenant_name"),
                row.get("tenant_connection_id"),
                row.get("auth_event_id"),
                row.get("raw_tenant_payload"),
                row.get("location_source_id"),
                row.get("location_nexudus_uuid"),
                row.get("location_name"),
                row.get("location_web_address"),
                row.get("location_address"),
                row.get("location_postal_code"),
                row.get("location_city"),
                row.get("location_state"),
                row.get("location_country_name"),
                row.get("location_currency_code"),
                row.get("location_email"),
                row.get("location_web_contact"),
                None,
                row.get("location_match_rule"),
            ),
        )

    def _delete_stale_rows(self, connection_id: int, tenant_ids: list[str]) -> int:
        if not tenant_ids:
            return self.sql.execute_non_query(
                """
                DELETE FROM silver.xero_tenants
                WHERE xero_connection_id = ?
                """,
                (connection_id,),
            )

        placeholders = ", ".join("?" for _ in tenant_ids)
        params = (connection_id, *tenant_ids)
        return self.sql.execute_non_query(
            f"""
            DELETE FROM silver.xero_tenants
            WHERE xero_connection_id = ?
              AND xero_tenant_id NOT IN ({placeholders})
            """,
            params,
        )
