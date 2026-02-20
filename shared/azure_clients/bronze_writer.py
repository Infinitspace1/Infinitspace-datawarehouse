"""
shared/azure_clients/bronze_writer.py

Writes raw records to the bronze layer.
Accepts full dicts from the Nexudus API and stores them as JSON.

Design:
  - Each write operation takes a sync_run_id so all rows from one
    run are grouped together.
  - A small number of denormalised columns (source_id, location_id, etc.)
    are extracted for indexing — everything else lives in raw_json.
  - Uses batch inserts for performance.
"""
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

BATCH_SIZE = 100  # rows per INSERT batch


class BronzeWriter:
    """
    Writes raw Nexudus records to bronze.* tables.

    Usage:
        run_id = uuid.uuid4()
        writer = BronzeWriter(run_id)
        writer.write_locations(records)
        writer.write_products(records)
        ...
    """

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    # ── Helpers ──────────────────────────────────────────────

    def _to_json(self, record: dict) -> str:
        return json.dumps(record, default=str, ensure_ascii=False)

    def _batch_insert(self, table: str, columns: list[str], rows: list[tuple]) -> int:
        """Insert rows in batches. Returns total rows inserted."""
        if not rows:
            return 0

        placeholders = ", ".join(["?" for _ in columns])
        col_list = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

        inserted = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            for row in batch:
                self.sql.execute_non_query(sql, row)
            inserted += len(batch)
            logger.debug(f"{table}: inserted batch of {len(batch)}")

        return inserted

    # ── Entity writers ───────────────────────────────────────

    def write_locations(self, records: list[dict]) -> int:
        """
        bronze.nexudus_locations
        source_id = record["Id"]
        """
        rows = []
        for r in records:
            rows.append((
                self.sync_run_id,
                r.get("Id"),
                self._to_json(r),
            ))
        return self._batch_insert(
            "bronze.nexudus_locations",
            ["sync_run_id", "source_id", "raw_json"],
            rows,
        )

    def write_products(self, records: list[dict]) -> int:
        """
        bronze.nexudus_products
        source_id   = FloorPlanDesk Id
        location_id = FloorPlanBusinessId
        item_type   = ItemType
        """
        rows = []
        for r in records:
            rows.append((
                self.sync_run_id,
                r.get("Id"),
                r.get("FloorPlanBusinessId"),
                r.get("ItemType"),
                self._to_json(r),
            ))
        return self._batch_insert(
            "bronze.nexudus_products",
            ["sync_run_id", "source_id", "location_id", "item_type", "raw_json"],
            rows,
        )

    def write_contracts(self, records: list[dict], product_id: int = None, location_id: int = None) -> int:
        """
        bronze.nexudus_contracts
        source_id   = CoworkerContract Id
        product_id  = FloorPlanDesk Id (passed in, not in the contract record itself)
        location_id = FloorPlanBusinessId (passed in)
        """
        rows = []
        for r in records:
            rows.append((
                self.sync_run_id,
                r.get("id") or r.get("Id"),
                product_id,
                location_id,
                self._to_json(r),
            ))
        return self._batch_insert(
            "bronze.nexudus_contracts",
            ["sync_run_id", "source_id", "product_id", "location_id", "raw_json"],
            rows,
        )

    def write_resources(self, records: list[dict], location_id: int = None) -> int:
        """
        bronze.nexudus_resources
        source_id   = Resource Id
        location_id = BusinessId
        """
        rows = []
        for r in records:
            rows.append((
                self.sync_run_id,
                r.get("Id"),
                location_id,
                self._to_json(r),
            ))
        return self._batch_insert(
            "bronze.nexudus_resources",
            ["sync_run_id", "source_id", "location_id", "raw_json"],
            rows,
        )

    def write_extra_services(self, records: list[dict]) -> int:
        """
        bronze.nexudus_extra_services
        source_id   = ExtraService Id
        location_id = BusinessId
        """
        rows = []
        for r in records:
            rows.append((
                self.sync_run_id,
                r.get("Id"),
                r.get("BusinessId"),
                self._to_json(r),
            ))
        return self._batch_insert(
            "bronze.nexudus_extra_services",
            ["sync_run_id", "source_id", "location_id", "raw_json"],
            rows,
        )