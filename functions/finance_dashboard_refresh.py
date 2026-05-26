"""
functions/finance_dashboard_refresh.py

Timer trigger: rebuilds the finance dashboard gold tables from silver/meta data
by executing gold.sp_refresh_finance_dashboard.

Runs after the Xero sync (04:00 UTC) and BambooHR sync (05:00 UTC) so both the
invoice worklist and user-access tables are current before the dashboard reads
from gold.
"""
from __future__ import annotations

import logging
import os

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("FINANCE_DASHBOARD_REFRESH_SCHEDULE", "0 30 5 * * *")


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def refresh_finance_dashboard(timer: func.TimerRequest) -> None:
    """Rebuild the finance dashboard gold tables from silver tables."""
    logger.info("Finance dashboard gold refresh started")

    try:
        async with RunTracker("finance_dashboard", "finance_dashboard", "gold") as run:
            sql = get_sql_client()

            before_invoice_rows = sql.execute_scalar(
                "SELECT COUNT(1) FROM gold.finance_dashboard_invoice_worklist"
            )
            before_access_rows = sql.execute_scalar(
                "SELECT COUNT(1) FROM gold.finance_dashboard_user_access"
            )
            before_revenue_occupancy_rows = sql.execute_scalar(
                "SELECT COUNT(1) FROM gold.finance_dashboard_revenue_occupancy"
            )

            sql.execute_non_query("EXEC gold.sp_refresh_finance_dashboard")

            after_invoice_rows = sql.execute_scalar(
                "SELECT COUNT(1) FROM gold.finance_dashboard_invoice_worklist"
            )
            after_access_rows = sql.execute_scalar(
                "SELECT COUNT(1) FROM gold.finance_dashboard_user_access"
            )
            after_revenue_occupancy_rows = sql.execute_scalar(
                "SELECT COUNT(1) FROM gold.finance_dashboard_revenue_occupancy"
            )

            run.rows_written = (
                int(after_invoice_rows or 0)
                + int(after_access_rows or 0)
                + int(after_revenue_occupancy_rows or 0)
            )

            logger.info(
                "Finance dashboard gold refresh complete: invoice_worklist %s -> %s, user_access %s -> %s, revenue_occupancy %s -> %s",
                before_invoice_rows,
                after_invoice_rows,
                before_access_rows,
                after_access_rows,
                before_revenue_occupancy_rows,
                after_revenue_occupancy_rows,
            )

    except Exception as exc:
        logger.error("Finance dashboard gold refresh failed: %s", exc, exc_info=True)
        raise
