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

from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

BATCH_SIZE = 100  # rows per upsert batch


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

    def _build_merge_sql(self, table: str, columns: list[str], update_columns: list[str]) -> str:
        source_projection = ", ".join([f"? AS {c}" for c in columns])
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{c}" for c in columns])
        update_set = ", ".join([f"target.{c} = source.{c}" for c in update_columns])

        return f"""
            MERGE {table} AS target
            USING (SELECT {source_projection}) AS source
                ON target.id = (
                    SELECT TOP 1 t.id
                    FROM {table} t
                    WHERE t.source_id = source.source_id
                    ORDER BY t.synced_at DESC, t.id DESC
                )
            WHEN MATCHED THEN UPDATE SET
                {update_set},
                target.synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
        """

    def _batch_upsert(
        self,
        table: str,
        columns: list[str],
        update_columns: list[str],
        rows: list[tuple],
    ) -> int:
        """Upsert rows in batches. Returns total rows processed."""
        if not rows:
            return 0

        sql = self._build_merge_sql(table, columns, update_columns)

        processed = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            for row in batch:
                self.sql.execute_non_query(sql, row)
            processed += len(batch)
            logger.debug(f"{table}: upserted batch of {len(batch)}")

        return processed

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
        return self._batch_upsert(
            "bronze.nexudus_locations",
            ["sync_run_id", "source_id", "raw_json"],
            ["sync_run_id", "raw_json"],
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
        return self._batch_upsert(
            "bronze.nexudus_products",
            ["sync_run_id", "source_id", "location_id", "item_type", "raw_json"],
            ["sync_run_id", "location_id", "item_type", "raw_json"],
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
        return self._batch_upsert(
            "bronze.nexudus_contracts",
            ["sync_run_id", "source_id", "product_id", "location_id", "raw_json"],
            ["sync_run_id", "product_id", "location_id", "raw_json"],
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
        return self._batch_upsert(
            "bronze.nexudus_resources",
            ["sync_run_id", "source_id", "location_id", "raw_json"],
            ["sync_run_id", "location_id", "raw_json"],
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
        return self._batch_upsert(
            "bronze.nexudus_extra_services",
            ["sync_run_id", "source_id", "location_id", "raw_json"],
            ["sync_run_id", "location_id", "raw_json"],
            rows,
        )