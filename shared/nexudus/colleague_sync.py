"""
shared/nexudus/colleague_sync.py

Syncs Nexudus coworker access into the existing warehouse layers:
  - bronze.nexudus_coworkers: raw coworker payloads
  - silver.nexudus_colleagues: normalized colleague rows
  - silver.nexudus_colleague_location_access: colleague -> location access

Location access reuses silver.nexudus_locations.source_id as the location key.
No duplicate location table is created.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Optional

from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient
from shared.nexudus.schemas import AccessibleBusiness, ParsedCoworker

logger = logging.getLogger(__name__)

DEFAULT_TEAM_BUSINESS_ID = 1376491118
ACCESS_SOURCE = "nexudus_coworker_business_access"

_COWORKER_ID_KEYS = ("Id", "id", "CoworkerId", "coworkerId")
_FIRST_NAME_KEYS = ("FirstName", "firstName", "GivenName", "GuessedFirstName")
_LAST_NAME_KEYS = ("LastName", "lastName", "Surname", "GuessedLastName")
_FULL_NAME_KEYS = ("FullName", "fullName", "CoworkerFullName", "ToStringText")
_EMAIL_KEYS = ("Email", "email", "CoworkerEmail", "EmailAddress")
_STATUS_KEYS = ("Status", "status", "CoworkerStatus")
_TEAM_BUSINESS_KEYS = ("TeamBusinessId", "teamBusinessId", "BusinessId", "businessId")

_BUSINESS_LIST_KEYS = (
    "AccessibleBusinesses",
    "BusinessAccess",
    "Businesses",
    "BusinessesAccess",
    "CoworkerBusinesses",
    "VisibleBusinesses",
    "AllowedBusinesses",
    "Locations",
    "AccessibleLocations",
)

_BUSINESS_ID_KEYS = ("Id", "id", "BusinessId", "businessId", "BusinessID")
_BUSINESS_NAME_KEYS = ("Name", "name", "BusinessName", "ToStringText")
_BUSINESS_SLUG_KEYS = ("Slug", "slug", "WebAddress", "webAddress")


@dataclass
class NexudusColleagueSyncStats:
    coworker_ids_processed: int = 0
    coworker_success_count: int = 0
    coworker_failed_count: int = 0
    bronze_rows_created: int = 0
    bronze_rows_updated: int = 0
    colleague_rows_created: int = 0
    colleague_rows_updated: int = 0
    access_rows_added: int = 0
    access_rows_removed: int = 0
    location_links_missing: int = 0
    failed_coworker_ids: list[int] = None

    def __post_init__(self) -> None:
        if self.failed_coworker_ids is None:
            self.failed_coworker_ids = []

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _first_non_empty(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _build_full_name(first_name: Optional[str], last_name: Optional[str]) -> Optional[str]:
    if first_name and last_name:
        return f"{first_name} {last_name}"
    return first_name or last_name


def _normalize_status(payload: dict[str, Any]) -> Optional[str]:
    status = _to_text(_first_non_empty(payload, _STATUS_KEYS))
    if status:
        return status
    active = payload.get("Active")
    if active is True:
        return "active"
    if active is False:
        return "inactive"
    return None


def _normalize_business(
    item: Any,
    payload: Optional[dict[str, Any]] = None,
) -> Optional[AccessibleBusiness]:
    payload = payload or {}

    if isinstance(item, dict):
        business_id = _to_int(_first_non_empty(item, _BUSINESS_ID_KEYS))
        if business_id is None:
            return None
        name = _to_text(_first_non_empty(item, _BUSINESS_NAME_KEYS))
        slug = _to_text(_first_non_empty(item, _BUSINESS_SLUG_KEYS))
        raw_payload = item
    else:
        business_id = _to_int(item)
        if business_id is None:
            return None

        name = None
        if business_id == _to_int(payload.get("InvoicingBusinessId")):
            name = _to_text(payload.get("InvoicingBusinessName"))

        slug = None
        raw_payload = {"Id": business_id}

    return AccessibleBusiness(
        business_id=business_id,
        name=name,
        slug=slug,
        raw_payload=raw_payload,
    )


def _collect_business_candidates(payload: dict[str, Any]) -> list[Any]:
    candidates: list[Any] = []

    for key in _BUSINESS_LIST_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(
                v for v in value if isinstance(v, (dict, int, str))
            )
        elif isinstance(value, dict):
            for nested_key in ("Records", "Items", "Data", "Businesses", "Locations"):
                nested = value.get(nested_key)
                if isinstance(nested, list):
                    candidates.extend(
                        v for v in nested if isinstance(v, (dict, int, str))
                    )

    for key, value in payload.items():
        key_lower = key.lower()
        if "business" not in key_lower and "location" not in key_lower:
            continue
        if not isinstance(value, list):
            continue
        candidates.extend(
            v for v in value if isinstance(v, (dict, int, str))
        )

    return candidates


def extract_accessible_businesses(payload: dict[str, Any]) -> list[AccessibleBusiness]:
    if not isinstance(payload, dict):
        return []

    by_id: dict[int, AccessibleBusiness] = {}
    for item in _collect_business_candidates(payload):
        normalized = _normalize_business(item, payload=payload)
        if normalized is None:
            continue
        by_id[normalized.business_id] = normalized

    primary_business_id = _to_int(_first_non_empty(payload, ("BusinessId", "businessId")))
    if primary_business_id is not None and primary_business_id not in by_id:
        by_id[primary_business_id] = AccessibleBusiness(
            business_id=primary_business_id,
            name=_to_text(_first_non_empty(payload, ("BusinessName", "businessName"))),
            slug=_to_text(_first_non_empty(payload, ("BusinessSlug", "businessSlug", "WebAddress"))),
            raw_payload={},
        )

    return list(by_id.values())


def parse_coworker_payload(
    payload: dict[str, Any],
    default_team_business_id: int = DEFAULT_TEAM_BUSINESS_ID,
) -> ParsedCoworker:
    if not isinstance(payload, dict):
        raise ValueError("Coworker payload must be a JSON object")

    coworker_id = _to_int(_first_non_empty(payload, _COWORKER_ID_KEYS))
    if coworker_id is None:
        raise ValueError("Coworker payload is missing Id/CoworkerId")

    first_name = _to_text(_first_non_empty(payload, _FIRST_NAME_KEYS))
    last_name = _to_text(_first_non_empty(payload, _LAST_NAME_KEYS))
    full_name = _to_text(_first_non_empty(payload, _FULL_NAME_KEYS)) or _build_full_name(first_name, last_name)
    email = _to_text(_first_non_empty(payload, _EMAIL_KEYS))
    status = _normalize_status(payload)

    team_business_id = _to_int(_first_non_empty(payload, _TEAM_BUSINESS_KEYS))
    if team_business_id is None:
        team_business_id = default_team_business_id

    accessible_businesses = extract_accessible_businesses(payload)

    return ParsedCoworker(
        nexudus_coworker_id=coworker_id,
        team_business_id=team_business_id,
        full_name=full_name,
        first_name=first_name,
        last_name=last_name,
        email=email,
        status=status,
        raw_payload=payload,
        accessible_businesses=accessible_businesses,
    )


def compute_mapping_delta(
    existing_location_ids: set[int],
    desired_location_ids: set[int],
) -> tuple[set[int], set[int]]:
    return desired_location_ids - existing_location_ids, existing_location_ids - desired_location_ids


class NexudusColleagueSyncService:
    def __init__(self, team_business_id: int = DEFAULT_TEAM_BUSINESS_ID, sql_client=None):
        self.team_business_id = team_business_id
        if sql_client is None:
            from shared.azure_clients.sql_client import get_sql_client

            sql_client = get_sql_client()
        self.sql = sql_client

    async def sync_coworkers(
        self,
        coworker_ids: list[int],
        client: Optional[NexudusClient] = None,
    ) -> NexudusColleagueSyncStats:
        valid_ids = [int(cid) for cid in coworker_ids if cid is not None]
        stats = NexudusColleagueSyncStats(coworker_ids_processed=len(valid_ids))
        sync_run_id = str(uuid.uuid4())

        if client is not None:
            await self._sync_with_client(client, valid_ids, sync_run_id, stats)
            return stats

        token = get_bearer_token()
        async with NexudusClient(token) as owned_client:
            await self._sync_with_client(owned_client, valid_ids, sync_run_id, stats)
        return stats

    async def _sync_with_client(
        self,
        client: NexudusClient,
        coworker_ids: list[int],
        sync_run_id: str,
        stats: NexudusColleagueSyncStats,
    ) -> None:
        for coworker_id in coworker_ids:
            try:
                payload = await client.get_coworker(coworker_id)
                if payload is None:
                    raise ValueError(f"Coworker {coworker_id} was not found")
                self._sync_single_payload(payload=payload, sync_run_id=sync_run_id, stats=stats)
                stats.coworker_success_count += 1
            except Exception:
                stats.coworker_failed_count += 1
                stats.failed_coworker_ids.append(coworker_id)
                logger.exception(
                    "Failed syncing Nexudus coworker",
                    extra={"coworker_id": coworker_id},
                )

        logger.info("Nexudus colleague access sync complete", extra=stats.to_dict())

    def _sync_single_payload(
        self,
        payload: dict[str, Any],
        sync_run_id: str,
        stats: NexudusColleagueSyncStats,
    ) -> None:
        parsed = parse_coworker_payload(payload, default_team_business_id=self.team_business_id)
        payload_json = json.dumps(parsed.raw_payload, default=str, ensure_ascii=False)

        with self.sql.get_connection() as conn:
            cursor = conn.cursor()

            bronze_id, bronze_action = self._upsert_bronze_coworker(
                cursor=cursor,
                source_id=parsed.nexudus_coworker_id,
                sync_run_id=sync_run_id,
                payload_json=payload_json,
            )
            if bronze_action == "INSERT":
                stats.bronze_rows_created += 1
            else:
                stats.bronze_rows_updated += 1

            colleague_action = self._upsert_colleague(
                cursor=cursor,
                parsed=parsed,
                bronze_id=bronze_id,
                sync_run_id=sync_run_id,
            )
            if colleague_action == "INSERT":
                stats.colleague_rows_created += 1
            else:
                stats.colleague_rows_updated += 1

            desired_location_source_ids = {business.business_id for business in parsed.accessible_businesses}
            existing_location_source_ids = self._load_existing_location_source_ids(
                cursor=cursor,
                location_source_ids=desired_location_source_ids,
            )
            stats.location_links_missing += len(desired_location_source_ids - existing_location_source_ids)

            added, removed = self._replace_mappings(
                cursor=cursor,
                colleague_source_id=parsed.nexudus_coworker_id,
                desired_location_source_ids=existing_location_source_ids,
            )
            stats.access_rows_added += len(added)
            stats.access_rows_removed += len(removed)

    def _upsert_bronze_coworker(
        self,
        cursor,
        source_id: int,
        sync_run_id: str,
        payload_json: str,
    ) -> tuple[int, str]:
        cursor.execute(
            """
            MERGE bronze.nexudus_coworkers AS target
            USING (SELECT ? AS source_id) AS source
                ON target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET
                sync_run_id = ?,
                raw_json = ?,
                synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                sync_run_id,
                source_id,
                raw_json
            ) VALUES (?, ?, ?)
            OUTPUT $action, inserted.id;
            """,
            (
                source_id,
                sync_run_id,
                payload_json,
                sync_run_id,
                source_id,
                payload_json,
            ),
        )
        action, bronze_id = cursor.fetchone()
        return int(bronze_id), str(action)

    def _upsert_colleague(
        self,
        cursor,
        parsed: ParsedCoworker,
        bronze_id: int,
        sync_run_id: str,
    ) -> str:
        cursor.execute(
            """
            MERGE silver.nexudus_colleagues AS target
            USING (SELECT ? AS source_id) AS source
                ON target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET
                bronze_id = ?,
                sync_run_id = ?,
                team_business_id = ?,
                full_name = ?,
                first_name = ?,
                last_name = ?,
                email = ?,
                status = ?,
                last_synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                source_id,
                bronze_id,
                sync_run_id,
                team_business_id,
                full_name,
                first_name,
                last_name,
                email,
                status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            OUTPUT $action;
            """,
            (
                parsed.nexudus_coworker_id,
                bronze_id,
                sync_run_id,
                parsed.team_business_id,
                parsed.full_name,
                parsed.first_name,
                parsed.last_name,
                parsed.email,
                parsed.status,
                parsed.nexudus_coworker_id,
                bronze_id,
                sync_run_id,
                parsed.team_business_id,
                parsed.full_name,
                parsed.first_name,
                parsed.last_name,
                parsed.email,
                parsed.status,
            ),
        )
        row = cursor.fetchone()
        return str(row[0])

    def _load_existing_location_source_ids(self, cursor, location_source_ids: set[int]) -> set[int]:
        if not location_source_ids:
            return set()

        placeholders = ", ".join("?" for _ in location_source_ids)
        cursor.execute(
            f"""
            SELECT source_id
            FROM silver.nexudus_locations
            WHERE source_id IN ({placeholders})
            """,
            tuple(sorted(location_source_ids)),
        )
        return {int(row[0]) for row in cursor.fetchall()}

    def _replace_mappings(
        self,
        cursor,
        colleague_source_id: int,
        desired_location_source_ids: set[int],
    ) -> tuple[set[int], set[int]]:
        cursor.execute(
            """
            SELECT location_source_id
            FROM silver.nexudus_colleague_location_access
            WHERE colleague_source_id = ?
            """,
            (colleague_source_id,),
        )
        existing_location_source_ids = {int(row[0]) for row in cursor.fetchall()}

        to_add, to_remove = compute_mapping_delta(existing_location_source_ids, desired_location_source_ids)

        if to_add:
            cursor.executemany(
                """
                INSERT INTO silver.nexudus_colleague_location_access
                    (colleague_source_id, location_source_id, source)
                VALUES (?, ?, ?)
                """,
                [
                    (colleague_source_id, location_source_id, ACCESS_SOURCE)
                    for location_source_id in sorted(to_add)
                ],
            )

        if to_remove:
            placeholders = ", ".join("?" for _ in to_remove)
            params = [colleague_source_id, *sorted(to_remove)]
            cursor.execute(
                f"""
                DELETE FROM silver.nexudus_colleague_location_access
                WHERE colleague_source_id = ?
                  AND location_source_id IN ({placeholders})
                """,
                params,
            )

        return to_add, to_remove


async def sync_coworker_access(
    coworker_ids: list[int],
    client: Optional[NexudusClient] = None,
    team_business_id: int = DEFAULT_TEAM_BUSINESS_ID,
) -> dict[str, Any]:
    service = NexudusColleagueSyncService(team_business_id=team_business_id)
    stats = await service.sync_coworkers(coworker_ids, client=client)
    return stats.to_dict()


def sync_coworker_access_blocking(
    coworker_ids: list[int],
    team_business_id: int = DEFAULT_TEAM_BUSINESS_ID,
) -> dict[str, Any]:
    return asyncio.run(
        sync_coworker_access(
            coworker_ids=coworker_ids,
            team_business_id=team_business_id,
        )
    )
