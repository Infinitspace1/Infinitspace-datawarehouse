# shared/azure_clients/bamboohr_bronze_writer.py
"""
shared/azure_clients/bamboohr_bronze_writer.py

Writes raw BambooHR employee records to bronze.bamboohr_employees.
MERGE on source_id — latest payload wins.
"""
from __future__ import annotations

import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

BATCH_SIZE = 100

_MERGE_SQL = """
    MERGE bronze.bamboohr_employees AS target
    USING (SELECT ? AS source_id, ? AS raw_json, ? AS sync_run_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        raw_json    = source.raw_json,
        sync_run_id = source.sync_run_id,
        synced_at   = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (source_id, raw_json, sync_run_id)
        VALUES (source.source_id, source.raw_json, source.sync_run_id);
"""


class BambooHRBronzeWriter:
    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def write_employees(self, records: list[dict]) -> int:
        """Upsert raw employee dicts. Returns count of rows processed."""
        if not records:
            return 0
        rows = [
            (
                int(r["id"]),
                json.dumps(r, default=str, ensure_ascii=False),
                self.sync_run_id,
            )
            for r in records
        ]
        total = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            self.sql.execute_many(_MERGE_SQL, batch)
            total += len(batch)
        return total
