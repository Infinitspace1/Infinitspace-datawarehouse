"""
shared/azure_clients/competence_bronze_writer.py

Writes raw Firestore competence_new records to the bronze layer:
  - bronze.competence_lists        (parent list documents)
  - bronze.competence_competitors  (competitor documents)

Latest-payload-wins MERGE on the string source_id (like bamboohr_bronze_writer),
combined with SHA-256 payload_hash change detection (like the Nexudus
BronzeWriter) so unchanged records are skipped and `synced_at` only advances on
a real change — which keeps the silver watermark efficient.
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

_LISTS_MERGE_SQL = """
    MERGE bronze.competence_lists AS target
    USING (SELECT ? AS source_id, ? AS country_code, ? AS raw_json,
                  ? AS payload_hash, ? AS sync_run_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        country_code = source.country_code,
        raw_json     = source.raw_json,
        payload_hash = source.payload_hash,
        sync_run_id  = source.sync_run_id,
        synced_at    = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (source_id, country_code, raw_json, payload_hash, sync_run_id)
        VALUES (source.source_id, source.country_code, source.raw_json,
                source.payload_hash, source.sync_run_id);
"""

_COMPETITORS_MERGE_SQL = """
    MERGE bronze.competence_competitors AS target
    USING (SELECT ? AS source_id, ? AS list_source_id, ? AS place_id,
                  ? AS raw_json, ? AS payload_hash, ? AS sync_run_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        list_source_id = source.list_source_id,
        place_id       = source.place_id,
        raw_json       = source.raw_json,
        payload_hash   = source.payload_hash,
        sync_run_id    = source.sync_run_id,
        synced_at      = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (source_id, list_source_id, place_id, raw_json, payload_hash, sync_run_id)
        VALUES (source.source_id, source.list_source_id, source.place_id,
                source.raw_json, source.payload_hash, source.sync_run_id);
"""


def _truncate(value, length: int):
    if value is None:
        return None
    s = str(value)
    return s[:length] if len(s) > length else s


class CompetenceBronzeWriter:
    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    # ── Helpers ──────────────────────────────────────────────

    def _to_json(self, record: dict) -> str:
        return json.dumps(record, default=str, ensure_ascii=False)

    def _hash(self, raw_json: str) -> str:
        return hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

    def _load_existing_hashes(self, table: str, source_ids: list[str]) -> dict[str, str]:
        """Load the stored payload_hash per (string) source_id from a bronze table."""
        if not source_ids:
            return {}
        result: dict[str, str] = {}
        for i in range(0, len(source_ids), HASH_LOOKUP_BATCH):
            batch = source_ids[i : i + HASH_LOOKUP_BATCH]
            placeholders = ",".join("?" * len(batch))
            rows = self.sql.execute_query(
                f"SELECT source_id, payload_hash FROM {table} "
                f"WHERE source_id IN ({placeholders})",
                tuple(batch),
            )
            for row in rows:
                if row.get("payload_hash"):
                    result[row["source_id"]] = row["payload_hash"]
        return result

    def _batch_upsert(self, sql: str, rows: list[tuple]) -> int:
        if not rows:
            return 0
        processed = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            self.sql.execute_many(sql, batch)
            processed += len(batch)
        return processed

    # ── Writers ──────────────────────────────────────────────
    # Each returns (changed_records, rows_written).

    def write_lists(
        self, lists: list[tuple[str, dict]]
    ) -> tuple[list[tuple[str, dict]], int]:
        """lists: [(list_source_id, parent_dict), ...]."""
        table = "bronze.competence_lists"
        existing = self._load_existing_hashes(table, [sid for sid, _ in lists])

        changed: list[tuple[str, dict]] = []
        rows: list[tuple] = []
        for sid, data in lists:
            raw = self._to_json(data)
            h = self._hash(raw)
            if existing.get(sid) == h:
                continue
            changed.append((sid, data))
            rows.append((
                sid,
                _truncate(data.get("country_code"), 8),
                raw,
                h,
                self.sync_run_id,
            ))
        written = self._batch_upsert(_LISTS_MERGE_SQL, rows)
        return changed, written

    def write_competitors(
        self, competitors: list[tuple[str, str, dict]]
    ) -> tuple[list[tuple[str, str, dict]], int]:
        """competitors: [(source_id, list_source_id, comp_dict), ...]."""
        table = "bronze.competence_competitors"
        existing = self._load_existing_hashes(table, [sid for sid, _, _ in competitors])

        changed: list[tuple[str, str, dict]] = []
        rows: list[tuple] = []
        for sid, list_sid, comp in competitors:
            raw = self._to_json(comp)
            h = self._hash(raw)
            if existing.get(sid) == h:
                continue
            changed.append((sid, list_sid, comp))
            rows.append((
                sid,
                _truncate(list_sid, 450),
                _truncate(comp.get("placeId"), 450),
                raw,
                h,
                self.sync_run_id,
            ))
        written = self._batch_upsert(_COMPETITORS_MERGE_SQL, rows)
        return changed, written
