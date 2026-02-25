"""
functions/silver_nexudus.py

Blueprint: Timer trigger (daily) that reads bronze.nexudus_* tables
and upserts into silver.nexudus_* tables.

Runs 30 minutes after bronze sync completes (02:30 UTC by default).
"""
import logging
import os
import uuid

import azure.functions as func

from shared.azure_clients.silver_write_locations import SilverLocationsWriter
from shared.azure_clients.silver_writer_products import SilverProductsWriter
from shared.azure_clients.silver_writer_contracts import SilverContractsWriter
from shared.azure_clients.silver_writer_resources import SilverResourcesWriter
from shared.azure_clients.silver_writer_extra_services import SilverExtraServicesWriter
from shared.azure_clients.run_tracker import RunTracker

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("SILVER_SYNC_SCHEDULE", "0 30 2 * * *")


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def bronze_to_silver(timer: func.TimerRequest) -> None:
    """Transform bronze layer data into typed silver layer tables."""
    logger.info("Bronze -> Silver transformation started")

    sync_run_id = uuid.uuid4()

    try:
        logger.info(f"Starting silver transformation [sync_run_id={sync_run_id}]")

        async with RunTracker("nexudus", "locations", "silver", metadata=str(sync_run_id)) as run:
            writer = SilverLocationsWriter(sync_run_id)
            result = writer.run()
            run.rows_written = result.get("locations", 0) + result.get("location_hours", 0)
            logger.info(f"Silver locations: {result}")

        async with RunTracker("nexudus", "products", "silver", metadata=str(sync_run_id)) as run:
            writer = SilverProductsWriter(sync_run_id)
            result = writer.run()
            run.rows_written = result.get("products", 0)
            logger.info(f"Silver products: {result}")

        async with RunTracker("nexudus", "resources", "silver", metadata=str(sync_run_id)) as run:
            writer = SilverResourcesWriter(sync_run_id)
            result = writer.run()
            run.rows_written = result.get("resources", 0)
            logger.info(f"Silver resources: {result}")

        async with RunTracker("nexudus", "contracts", "silver", metadata=str(sync_run_id)) as run:
            writer = SilverContractsWriter(sync_run_id)
            result = writer.run()
            run.rows_written = result.get("contracts", 0)
            logger.info(f"Silver contracts: {result}")

        async with RunTracker("nexudus", "extra_services", "silver", metadata=str(sync_run_id)) as run:
            writer = SilverExtraServicesWriter(sync_run_id)
            result = writer.run()
            run.rows_written = result.get("extra_services", 0)
            logger.info(f"Silver extra_services: {result}")

        logger.info(f"Bronze -> Silver transformation complete [sync_run_id={sync_run_id}]")

    except Exception as e:
        logger.error(f"Bronze -> Silver transformation failed: {e}", exc_info=True)
        raise
