"""
Activity: write_logs

Ports the n8n "Insert logs in Microsoft SQL" node.
Updates the run row to 'completed' and writes an Application Insights custom event.
"""
from __future__ import annotations

import logging
import os
from datetime import date

from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

_UPDATE_LOG = """
UPDATE bronze.n8n_location_scraper_logs
SET
    run_date         = ?,
    source           = ?,
    buildings_found  = ?,
    buildings_new    = ?,
    buildings_updated= ?,
    status           = 'completed',
    updated_at       = GETDATE()
WHERE run_id = ?
"""

_INSERT_LOG = """
INSERT INTO bronze.n8n_location_scraper_logs
    (run_id, city, run_date, source, buildings_found, buildings_new, buildings_updated, status)
VALUES (?, ?, ?, ?, ?, ?, ?, 'completed')
"""


def init_run_log(run_id: str, city: str) -> None:
    """Insert a RUNNING row at the start of a scrape job."""
    sql = get_sql_client()
    sql.execute_non_query(
        "INSERT INTO bronze.n8n_location_scraper_logs "
        "(run_id, city, run_date, source, buildings_found, buildings_new, buildings_updated, status) "
        "VALUES (?, ?, ?, '', 0, 0, 0, 'running')",
        (run_id, city, date.today().isoformat()),
    )


def write_logs(stats: dict) -> None:
    """
    Mark the run as completed and persist stats to SQL.
    Also emits an Application Insights custom event if the
    APPLICATIONINSIGHTS_CONNECTION_STRING env var is configured.
    """
    run_id = stats["run_id"]
    today = date.today().isoformat()
    source = stats.get("source", "")
    buildings_found = stats.get("buildings_found", 0)
    buildings_new = stats.get("buildings_new", 0)
    buildings_updated = stats.get("buildings_updated", 0)

    sql = get_sql_client()
    affected = sql.execute_non_query(
        _UPDATE_LOG,
        (today, source, buildings_found, buildings_new, buildings_updated, run_id),
    )
    if affected == 0:
        sql.execute_non_query(
            _INSERT_LOG,
            (run_id, stats.get("city", ""), today, source, buildings_found, buildings_new, buildings_updated),
        )

    logger.info(
        "location_scraper run_id=%s city=%s source=%s "
        "found=%d new=%d updated=%d",
        run_id, stats.get("city"), source,
        buildings_found, buildings_new, buildings_updated,
    )

    _emit_app_insights(stats)


def mark_run_failed(run_id: str, error: str) -> None:
    """Update the log row to 'failed' on orchestrator error."""
    sql = get_sql_client()
    sql.execute_non_query(
        "UPDATE bronze.n8n_location_scraper_logs "
        "SET status = 'failed', updated_at = GETDATE() "
        "WHERE run_id = ?",
        (run_id,),
    )
    logger.error("location_scraper run_id=%s failed: %s", run_id, error)


def _emit_app_insights(stats: dict) -> None:
    conn_str = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if not conn_str:
        return
    try:
        from azure.monitor.opentelemetry import configure_azure_monitor
        from opentelemetry import trace

        configure_azure_monitor(connection_string=conn_str)
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("location_scraper.run_complete") as span:
            for k, v in stats.items():
                span.set_attribute(f"location_scraper.{k}", str(v))
    except ImportError:
        logger.debug("azure-monitor-opentelemetry not installed; skipping App Insights event")
    except Exception:
        logger.exception("App Insights emit failed")
