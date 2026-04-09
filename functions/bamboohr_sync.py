# functions/bamboohr_sync.py
"""
functions/bamboohr_sync.py

Timer trigger: pulls all BambooHR employees daily and writes
bronze + silver layers in sequence.
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
from shared.bamboohr.client import get_bamboohr_client

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("BAMBOOHR_SYNC_SCHEDULE", "0 0 5 * * *")


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

    logger.info("BambooHR sync complete [run_id=%s]", run_id)
