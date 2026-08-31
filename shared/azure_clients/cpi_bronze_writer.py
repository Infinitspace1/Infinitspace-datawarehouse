"""
shared/azure_clients/cpi_bronze_writer.py

Writes raw CPI observations to bronze.cpi_series.

Same latest-payload-wins MERGE + SHA-256 hash dedup as the Eventbrite writer.
The unit here is ONE OBSERVATION (provider + geo + period), not one API
response, which is deliberate: statistics offices revise already-published
months, and keying bronze at observation level makes a revision show up as a
hash change on exactly the month that moved. Most days nothing changes and
nothing is written.
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

_SERIES_MERGE_SQL = """
    MERGE bronze.cpi_series AS target
    USING (SELECT ? AS source_id, ? AS provider, ? AS geo, ? AS period,
                  ? AS raw_json, ? AS payload_hash, ? AS sync_run_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        provider     = source.provider,
        geo          = source.geo,
        period       = source.period,
        raw_json     = source.raw_json,
        payload_hash = source.payload_hash,
        sync_run_id  = source.sync_run_id,
        synced_at    = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (source_id, provider, geo, period, raw_json, payload_hash, sync_run_id)
        VALUES (source.source_id, source.provider, source.geo, source.period,
                source.raw_json, source.payload_hash, source.sync_run_id);
"""


class CpiBronzeWriter:
    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def _to_json(self, record: dict) -> str:
        # sort_keys so an unchanged observation hashes identically between runs
        # regardless of dict ordering - otherwise every run would look changed.
        return json.dumps(record, default=str, ensure_ascii=False, sort_keys=True)

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
                f"SELECT source_id, payload_hash FROM bronze.cpi_series "
                f"WHERE source_id IN ({placeholders})",
                tuple(batch),
            )
            for row in rows:
                if row.get("payload_hash"):
                    result[row["source_id"]] = row["payload_hash"]
        return result

    def write_series(self, records: list[dict]) -> tuple[list[dict], int]:
        """Upsert raw observations. Returns (changed_records, rows_written)."""
        source_ids = [str(r["source_id"]) for r in records if r.get("source_id")]
        existing = self._load_existing_hashes(source_ids)

        changed: list[dict] = []
        rows: list[tuple] = []
        for r in records:
            sid = r.get("source_id")
            if not sid:
                continue
            sid = str(sid)
            raw = self._to_json(r)
            h = self._hash(raw)
            if existing.get(sid) == h:
                continue
            changed.append(r)
            rows.append((sid, r.get("provider"), r.get("geo"), r.get("period"),
                         raw, h, self.sync_run_id))

        written = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            self.sql.execute_many(_SERIES_MERGE_SQL, batch)
            written += len(batch)
        return changed, written
