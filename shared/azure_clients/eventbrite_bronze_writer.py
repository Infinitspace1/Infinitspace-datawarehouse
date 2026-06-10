"""
shared/azure_clients/eventbrite_bronze_writer.py

Writes raw Eventbrite event records to bronze.eventbrite_events.

Latest-payload-wins MERGE on the string source_id (like the competence
bronze writer), combined with SHA-256 payload_hash change detection so
unchanged records are skipped and `synced_at` only advances on a real
change — which keeps the silver watermark efficient.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
HASH_LOOKUP_BATCH = 500

_EVENTS_MERGE_SQL = """
    MERGE bronze.eventbrite_events AS target
    USING (SELECT ? AS source_id, ? AS organization_id, ? AS event_status,
                  ? AS raw_json, ? AS payload_hash, ? AS sync_run_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        organization_id = source.organization_id,
        event_status    = source.event_status,
        raw_json        = source.raw_json,
        payload_hash    = source.payload_hash,
        sync_run_id     = source.sync_run_id,
        synced_at       = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (source_id, organization_id, event_status, raw_json, payload_hash, sync_run_id)
        VALUES (source.source_id, source.organization_id, source.event_status,
                source.raw_json, source.payload_hash, source.sync_run_id);
"""


def _truncate(value, length: int):
    if value is None:
        return None
    s = str(value)
    return s[:length] if len(s) > length else s


class EventbriteBronzeWriter:
    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def _to_json(self, record: dict) -> str:
        return json.dumps(record, default=str, ensure_ascii=False)

    def _hash(self, raw_json: str) -> str:
        return hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

    def _load_existing_hashes(self, source_ids: list[str]) -> dict[str, str]:
        if not source_ids:
            return {}
        result: dict[str, str] = {}
        for i in range(0, len(source_ids), HASH_LOOKUP_BATCH):
            batch = source_ids[i : i + HASH_LOOKUP_BATCH]
            placeholders = ",".join("?" * len(batch))
            rows = self.sql.execute_query(
                f"SELECT source_id, payload_hash FROM bronze.eventbrite_events "
                f"WHERE source_id IN ({placeholders})",
                tuple(batch),
            )
            for row in rows:
                if row.get("payload_hash"):
                    result[row["source_id"]] = row["payload_hash"]
        return result

    def write_events(self, records: list[dict]) -> tuple[list[dict], int]:
        """Upsert raw event payloads. Returns (changed_records, rows_written)."""
        source_ids = [str(r["id"]) for r in records if r.get("id")]
        existing = self._load_existing_hashes(source_ids)

        changed: list[dict] = []
        rows: list[tuple] = []
        for r in records:
            sid = r.get("id")
            if not sid:
                continue
            sid = str(sid)
            raw = self._to_json(r)
            h = self._hash(raw)
            if existing.get(sid) == h:
                continue
            changed.append(r)
            rows.append((
                sid,
                _truncate(r.get("organization_id"), 64),
                _truncate(r.get("status"), 64),
                raw,
                h,
                self.sync_run_id,
            ))

        written = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            self.sql.execute_many(_EVENTS_MERGE_SQL, batch)
            written += len(batch)
        return changed, written
