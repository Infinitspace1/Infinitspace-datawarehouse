# shared/azure_clients/silver_writer_bamboohr.py
"""
Reads bronze.bamboohr_employees, transforms each record, and MERGEs
into silver.bamboohr_employees.
"""
from __future__ import annotations

import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.bamboohr.transformers.employees import transform_employee

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.bamboohr_employees AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        employee_number = ?, first_name = ?, last_name = ?,
        work_email = ?,
        job_title = ?, department = ?, division = ?, location = ?,
        manager_id = ?, manager_name = ?,
        hire_date = ?,
        work_phone = ?, work_phone_ext = ?,
        address1 = ?, city = ?,
        bronze_id = ?, sync_run_id = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id,
        employee_number, first_name, last_name,
        work_email,
        job_title, department, division, location,
        manager_id, manager_name,
        hire_date,
        work_phone, work_phone_ext,
        address1, city,
        bronze_id, sync_run_id
    ) VALUES (
        ?,
        ?, ?, ?,
        ?,
        ?, ?, ?, ?,
        ?, ?,
        ?,
        ?, ?,
        ?, ?,
        ?, ?
    );
"""


class SilverBambooHRWriter:
    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info("BambooHR silver: loaded %s bronze records", len(rows))

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                emp = transform_employee(raw, row["id"], self.sync_run_id)
                params_list.append(self._make_params(emp))
            except Exception as exc:
                logger.warning("BambooHR silver: failed source_id=%s: %s", raw.get("id"), exc)
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info("BambooHR silver: %s upserted, %s errors", ok, errors)
        return {"employees_read": len(rows), "employees_written": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return self.sql.execute_query(
            """
            SELECT b.id, b.raw_json
            FROM bronze.bamboohr_employees b
            INNER JOIN (
                SELECT source_id, MAX(synced_at) AS latest
                FROM bronze.bamboohr_employees
                GROUP BY source_id
            ) latest ON b.source_id = latest.source_id
                    AND b.synced_at  = latest.latest
            """
        )

    def _make_params(self, emp: dict) -> tuple:
        vals = (
            emp["employee_number"], emp["first_name"], emp["last_name"],
            emp["work_email"],
            emp["job_title"], emp["department"], emp["division"], emp["location"],
            emp["manager_id"], emp["manager_name"],
            emp["hire_date"],
            emp["work_phone"], emp["work_phone_ext"],
            emp["address1"], emp["city"],
            emp["bronze_id"], emp["sync_run_id"],
        )
        return (emp["source_id"], *vals, emp["source_id"], *vals)
