"""
shared/azure_clients/silver_writer_helpdesk_departments.py

Reads bronze.nexudus_helpdesk_departments, transforms, and MERGEs into
silver.nexudus_helpdesk_departments.
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.exclusions import is_excluded_location_source_id
from shared.nexudus.transformers.helpdesk_departments import transform_helpdesk_department

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.nexudus_helpdesk_departments AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        location_source_id = ?,
        name = ?, description = ?,
        is_active = ?, task_list_id = ?,
        updated_by = ?,
        created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        location_source_id,
        name, description,
        is_active, task_list_id,
        updated_by,
        created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?,
        ?, ?,
        ?, ?,
        ?,
        ?, ?
    );
"""


class SilverHelpdeskDepartmentsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze help desk department records")

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                dept = transform_helpdesk_department(raw, row["id"], self.sync_run_id)
                if is_excluded_location_source_id(dept["location_source_id"]):
                    continue
                params_list.append(self._make_params(dept))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver help desk departments: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "helpdesk_departments": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_helpdesk_departments",
            source_name="nexudus",
            entity="helpdesk_departments",
        )

    def _make_params(self, d: dict) -> tuple:
        vals = (
            d["unique_id"],         d["bronze_id"],     d["sync_run_id"],
            d["location_source_id"],
            d["name"],              d["description"],
            d["is_active"],         d["task_list_id"],
            d["updated_by"],
            d["created_on"],        d["updated_on"],
        )
        return (d["source_id"], *vals, d["source_id"], *vals)
