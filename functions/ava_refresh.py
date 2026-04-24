"""
functions/ava_refresh.py

Blueprint: Timer trigger (daily) that rebuilds the ava.product_availability
table by executing the stored procedure ava.sp_refresh_product_availability.

Runs 30 minutes after silver sync completes (03:00 UTC by default) to ensure
all silver tables are up to date before the ava layer is refreshed.
"""
import logging
import os

import azure.functions as func

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.run_tracker import RunTracker

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("AVA_REFRESH_SCHEDULE", "0 0 3 * * *")


def _assert_ava_objects_exist(sql) -> None:
    missing: list[str] = []

    table_exists = sql.execute_scalar(
        "SELECT CASE WHEN OBJECT_ID('ava.product_availability', 'U') IS NULL THEN 0 ELSE 1 END"
    )
    if not table_exists:
        missing.append(
            "table ava.product_availability (run scripts/sql_scripts/ava_product_availability_schema.sql)"
        )

    proc_exists = sql.execute_scalar(
        "SELECT CASE WHEN OBJECT_ID('ava.sp_refresh_product_availability', 'P') IS NULL THEN 0 ELSE 1 END"
    )
    if not proc_exists:
        missing.append(
            "procedure ava.sp_refresh_product_availability "
            "(run scripts/sql_scripts/ava_sp_refresh_product_availability.sql)"
        )

    if missing:
        raise RuntimeError(
            "AVA refresh prerequisites are missing or inaccessible: " + "; ".join(missing)
        )


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def refresh_ava_availability(timer: func.TimerRequest) -> None:
    """Rebuild ava.product_availability from silver tables via stored procedure."""
    logger.info("AVA refresh started")

    try:
        async with RunTracker("ava", "product_availability", "ava") as run:
            sql = get_sql_client()
            _assert_ava_objects_exist(sql)

            # Count rows before to detect if SP produced output
            before = sql.execute_scalar("SELECT COUNT(1) FROM ava.product_availability")

            sql.execute_non_query("EXEC ava.sp_refresh_product_availability")

            after = sql.execute_scalar("SELECT COUNT(1) FROM ava.product_availability")
            run.rows_written = int(after) if after is not None else 0

            logger.info(
                f"AVA refresh complete: {before} rows → {after} rows in ava.product_availability"
            )

    except Exception as e:
        logger.error(f"AVA refresh failed: {e}", exc_info=True)
        raise
