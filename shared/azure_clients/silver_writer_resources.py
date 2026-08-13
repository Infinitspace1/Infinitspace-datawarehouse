"""
shared/azure_clients/silver_writer_resources.py

Reads bronze.nexudus_resources, transforms, and MERGEs into
silver.nexudus_resources (single table, all columns).
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.exclusions import is_excluded_location_source_id
from shared.nexudus.transformers.resources import transform_resource

logger = logging.getLogger(__name__)

_COLUMN_KEYS = (
    ("bronze_id", "bronze_id"),
    ("sync_run_id", "sync_run_id"),
    ("location_source_id", "location_source_id"),
    ("nexudus_uuid", "nexudus_uuid"),
    ("name", "name"),
    ("description", "description"),
    ("resource_type_id", "resource_type_id"),
    ("resource_type_name", "resource_type_name"),
    ("system_resource_type", "system_resource_type"),
    ("is_archived", "is_archived"),
    ("group_id", "group_id"),
    ("group_name", "group_name"),
    ("is_visible", "is_visible"),
    ("online", "online"),
    ("visible_to_others", "visible_to_others"),
    ("available", "available"),
    ("allocation", "allocation"),
    ("size", "size"),
    ("floor_number", "floor_number"),
    ("accessible", "accessible"),
    ("created_on", "created_on"),
    ("updated_on", "updated_on"),
)


class SilverResourcesWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()
        self._merge_columns = self._load_merge_columns()
        self._merge_sql = self._build_merge_sql()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze resource records")

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                r = transform_resource(raw, row["id"], self.sync_run_id)
                if r is None:
                    continue
                if is_excluded_location_source_id(r["location_source_id"]):
                    continue
                params_list.append(self._make_params(r))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(self._merge_sql, params_list)

        ok = len(params_list)
        logger.info(f"Silver resources: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "resources": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_resources",
            source_name="nexudus",
            entity="resources",
        )

    def _load_merge_columns(self) -> list[tuple[str, str]]:
        rows = self.sql.execute_query(
            """
            SELECT name
            FROM sys.columns
            WHERE object_id = OBJECT_ID('silver.nexudus_resources')
            """
        )
        present = {row["name"] for row in rows}
        columns = [item for item in _COLUMN_KEYS if item[0] in present]
        if not columns:
            raise RuntimeError("silver.nexudus_resources has no compatible target columns")
        return columns

    def _build_merge_sql(self) -> str:
        update_set = ",\n        ".join(
            f"{column_name} = ?" for column_name, _ in self._merge_columns
        )
        insert_columns = ", ".join(["source_id", *[column_name for column_name, _ in self._merge_columns]])
        insert_values = ", ".join(["?"] * (1 + len(self._merge_columns)))
        return f"""
            MERGE silver.nexudus_resources AS target
            USING (SELECT ? AS source_id) AS source
                ON target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET
                {update_set},
                last_synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                {insert_columns}
            ) VALUES (
                {insert_values}
            );
        """

    def _make_params(self, r: dict) -> tuple:
        vals = tuple(r[key] for _, key in self._merge_columns)
        return (r["source_id"], *vals, r["source_id"], *vals)
