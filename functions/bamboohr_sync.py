# functions/bamboohr_sync.py
"""
functions/bamboohr_sync.py

Timer trigger: pulls all BambooHR employees daily and writes
bronze + silver layers in sequence, then reconciles silver against
the full fetched roster to soft-delete employees removed from BambooHR.

Default schedule: 05:00 UTC.
"""
from __future__ import annotations

import logging
import os
import uuid

import azure.functions as func

from shared.azure_clients.bamboohr_bronze_writer import BambooHRBronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_writer_bamboohr import SilverBambooHRWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.bamboohr.client import get_bamboohr_client

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("BAMBOOHR_SYNC_SCHEDULE", "0 0 5 * * *")
# Safety floor — if the fetched roster is smaller than this, abort reconcile
MIN_ACTIVE_IDS = int(os.getenv("BAMBOOHR_RECONCILE_MIN_IDS", "10"))


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def bamboohr_sync(timer: func.TimerRequest) -> None:
    logger.info("BambooHR sync started")
    run_id = uuid.uuid4()
    client = get_bamboohr_client()

    async with RunTracker("bamboohr", "bamboohr_employees", "bronze", metadata=str(run_id)) as bronze_run:
        employees = client.get_employees()
        bronze_run.rows_read = len(employees)
        bronze_writer = BambooHRBronzeWriter(run_id)
        bronze_run.rows_written = bronze_writer.write_employees(employees)
        logger.info(
            "BambooHR bronze: %s fetched, %s written",
            bronze_run.rows_read, bronze_run.rows_written,
        )

    async with RunTracker("bamboohr", "bamboohr_employees", "silver", metadata=str(run_id)) as silver_run:
        silver_writer = SilverBambooHRWriter(run_id)
        result = silver_writer.run()
        silver_run.rows_read = result["employees_read"]
        silver_run.rows_written = result["employees_written"]
        silver_run.rows_skipped = result["errors"]
        logger.info(
            "BambooHR silver: %s written, %s errors",
            silver_run.rows_written, silver_run.rows_skipped,
        )

    active_ids = {int(e["id"]) for e in employees if e.get("id")}
    if len(active_ids) < MIN_ACTIVE_IDS:
        logger.warning(
            "BambooHR reconcile skipped: only %s active ids fetched (threshold %s)",
            len(active_ids), MIN_ACTIVE_IDS,
        )
    else:
        async with RunTracker(
            "bamboohr", "bamboohr_employees_reconcile", "silver", metadata=str(run_id)
        ) as reconcile_run:
            reconcile_run.rows_read = len(active_ids)
            deleted, restored = _reconcile_employees(active_ids)
            reconcile_run.rows_written = deleted
            reconcile_run.rows_skipped = restored
            logger.info(
                "BambooHR reconcile: %s soft-deleted, %s restored",
                deleted, restored,
            )

    logger.info("BambooHR sync complete [run_id=%s]", run_id)


def _reconcile_employees(active_ids: set[int]) -> tuple[int, int]:
    """
    Soft-delete silver employees whose source_id is not in active_ids.
    Restore any previously-deleted employee whose source_id reappears.

    Returns (deleted_count, restored_count).
    """
    sql = get_sql_client()
    with sql.get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE #active_ids (source_id BIGINT PRIMARY KEY)")
        try:
            cursor.fast_executemany = True
        except AttributeError:
            pass
        cursor.executemany(
            "INSERT INTO #active_ids (source_id) VALUES (?)",
            [(i,) for i in active_ids],
        )

        cursor.execute(
            """
            UPDATE silver.bamboohr_employees
            SET is_deleted = 1, deleted_at = GETUTCDATE()
            WHERE is_deleted = 0
              AND source_id NOT IN (SELECT source_id FROM #active_ids)
            """
        )
        deleted = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute(
            """
            UPDATE silver.bamboohr_employees
            SET is_deleted = 0, deleted_at = NULL
            WHERE is_deleted = 1
              AND source_id IN (SELECT source_id FROM #active_ids)
            """
        )
        restored = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute("DROP TABLE #active_ids")
        return deleted, restored
