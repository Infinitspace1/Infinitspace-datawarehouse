"""
shared/azure_clients/silver_writer_resources.py

Reads bronze.nexudus_resources, transforms, and MERGEs into
silver.nexudus_resources (single table, all columns).
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.transformers.resources import transform_resource

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.nexudus_resources AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        bronze_id = ?, sync_run_id = ?,
        location_source_id = ?, nexudus_uuid = ?,
        name = ?, description = ?,
        resource_type_id = ?, resource_type_name = ?,
        group_id = ?, group_name = ?,
        visible = ?, online = ?, visible_to_others = ?, available = ?,
        capacity = ?, size = ?, floor_number = ?, accessible = ?,
        created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, bronze_id, sync_run_id,
        location_source_id, nexudus_uuid,
        name, description,
        resource_type_id, resource_type_name,
        group_id, group_name,
        visible, online, visible_to_others, available,
        capacity, size, floor_number, accessible,
        created_on, updated_on
    ) VALUES (
        ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?
    );
"""


class SilverResourcesWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

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
                params_list.append(self._make_params(r))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver resources: {ok} upserted, {errors} errors")
        return {"resources": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return self.sql.execute_query("""
            SELECT b.id, b.raw_json
            FROM bronze.nexudus_resources b
            INNER JOIN (
                SELECT source_id, MAX(synced_at) AS latest
                FROM bronze.nexudus_resources
                GROUP BY source_id
            ) latest ON b.source_id = latest.source_id
                    AND b.synced_at  = latest.latest
        """)

    def _make_params(self, r: dict) -> tuple:
        vals = (
            r["bronze_id"], r["sync_run_id"],
            r["location_source_id"], r["nexudus_uuid"],
            r["name"], r["description"],
            r["resource_type_id"], r["resource_type_name"],
            r["group_id"], r["group_name"],
            r["visible"], r["online"], r["visible_to_others"], r["available"],
            r["capacity"], r["size"], r["floor_number"], r["accessible"],
            r["created_on"], r["updated_on"],
        )
        return (r["source_id"], *vals, r["source_id"], *vals)
