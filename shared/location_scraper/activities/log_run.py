"""
Activity: write_logs

Ports the n8n "Insert logs in Microsoft SQL" node.
Updates the run row to 'completed' and writes an Application Insights custom event.
"""
from __future__ import annotations

import json
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

_MARK_STALE_RUNNING_FAILED = """
UPDATE bronze.n8n_location_scraper_logs
SET
    status = 'failed',
    updated_at = GETDATE()
WHERE status = 'running'
  AND created_at < DATEADD(hour, -?, GETDATE())
"""

_UPSERT_RUNNING_LOG = """
MERGE bronze.n8n_location_scraper_logs WITH (HOLDLOCK) AS target
USING (SELECT ? AS run_id) AS src
    ON target.run_id = src.run_id
WHEN MATCHED THEN
    UPDATE SET
        city = ?,
        run_date = ?,
        source = ISNULL(NULLIF(target.source, ''), ''),
        buildings_found = 0,
        buildings_new = 0,
        buildings_updated = 0,
        status = 'running',
        updated_at = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (run_id, city, run_date, source, buildings_found, buildings_new, buildings_updated, status, updated_at)
    VALUES (?, ?, ?, '', 0, 0, 0, 'running', GETDATE());
"""


def _stale_running_hours() -> int:
    raw = (os.environ.get("LOCATION_SCRAPER_STALE_RUNNING_HOURS") or "2").strip()
    try:
        hours = int(raw)
    except ValueError:
        return 2
    return max(1, hours)


def mark_stale_running_failed(hours: int | None = None) -> int:
    """Mark abandoned local/Durable runs as failed after a safety timeout."""
    threshold_hours = hours or _stale_running_hours()
    sql = get_sql_client()
    affected = sql.execute_non_query(_MARK_STALE_RUNNING_FAILED, (threshold_hours,))
    if affected:
        logger.warning(
            "Marked %d stale location_scraper running logs as failed (threshold=%dh)",
            affected,
            threshold_hours,
        )
    return affected


def init_run_log(run_id: str, city: str) -> None:
    """Upsert a RUNNING row at the start of a scrape job."""
    sql = get_sql_client()
    mark_stale_running_failed()
    today = date.today().isoformat()
    sql.execute_non_query(_UPSERT_RUNNING_LOG, (run_id, city, today, run_id, city, today))


_DELETE_RUN_QUALITY = "DELETE FROM bronze.location_scraper_run_quality WHERE run_id = ?"

_INSERT_RUN_QUALITY = """
INSERT INTO bronze.location_scraper_run_quality (
    run_id, source, city,
    raw_item_count, normalized_count, with_coords_count, with_phone_count, with_name_or_company_count,
    lusha_email_slots, buildings_found, buildings_new, buildings_updated,
    agencies_total, agencies_with_contacts, enrichment_diagnostics_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_DELETE_LUSHA_DIAGNOSTICS = "DELETE FROM bronze.location_scraper_lusha_diagnostics WHERE run_id = ?"

_INSERT_LUSHA_DIAGNOSTIC = """
INSERT INTO bronze.location_scraper_lusha_diagnostics (
    run_id, source, city,
    agency_name, first_name, last_name,
    path, allow_company_fallback, has_contact, reason, individual_email_source,
    company_name_cleaned, lusha_search_mode, domain_used,
    domains_found, google_domains_json, raw_contacts_found, final_contacts_found
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def write_lusha_diagnostics(payload: dict) -> dict[str, int]:
    """
    Persist one diagnostic row per Lusha enrichment candidate for a run.

    payload = {
        "run_id": str,
        "source": str,
        "city": str,
        "enriched_agencies": [Agency.to_dict() + {"_diagnostics": ...}, ...],
    }
    """
    run_id = payload["run_id"]
    source = payload.get("source", "")
    city = payload.get("city", "")
    enriched_agencies = payload.get("enriched_agencies") or []

    sql = get_sql_client()
    sql.execute_non_query(_DELETE_LUSHA_DIAGNOSTICS, (run_id,))

    if not enriched_agencies:
        return {"rows_written": 0}

    rows = []
    for agency in enriched_agencies:
        diagnostics = agency.get("_diagnostics") or {}
        rows.append(
            (
                run_id,
                source,
                city,
                agency.get("company_name") or "",
                agency.get("first_name") or "",
                agency.get("last_name") or "",
                diagnostics.get("path") or "",
                1 if agency.get("allow_company_fallback") else 0,
                1 if diagnostics.get("has_contact") else 0,
                diagnostics.get("reason") or "",
                diagnostics.get("individual_email_source") or "",
                diagnostics.get("company_name_cleaned") or "",
                diagnostics.get("lusha_search_mode") or "",
                diagnostics.get("domain_used") or "",
                int(diagnostics.get("domains_found") or 0),
                json.dumps(diagnostics.get("google_domains") or [], ensure_ascii=False),
                int(diagnostics.get("raw_contacts_found") or 0),
                int(diagnostics.get("final_contacts_found") or 0),
            )
        )

    written = sql.execute_many(_INSERT_LUSHA_DIAGNOSTIC, rows)
    logger.info(
        "location_scraper Lusha diagnostics written run_id=%s rows=%d",
        run_id,
        len(rows),
    )
    return {"rows_written": len(rows) if written == -1 else int(written)}


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

    if "raw_item_count" in stats:
        try:
            sql.execute_non_query(_DELETE_RUN_QUALITY, (run_id,))
            sql.execute_non_query(
                _INSERT_RUN_QUALITY,
                (
                    run_id,
                    source,
                    stats.get("city", ""),
                    int(stats.get("raw_item_count", 0)),
                    int(stats.get("normalized_count", 0)),
                    int(stats.get("with_coords_count", 0)),
                    int(stats.get("with_phone_count", 0)),
                    int(stats.get("with_name_or_company_count", 0)),
                    int(stats.get("lusha_email_slots", 0)),
                    int(buildings_found),
                    int(buildings_new),
                    int(buildings_updated),
                    int(stats.get("agencies_total", 0)),
                    int(stats.get("agencies_with_contacts", 0)),
                    stats.get("enrichment_diagnostics_json") or "",
                ),
            )
        except Exception:
            logger.exception(
                "Could not write bronze.location_scraper_run_quality. run_id=%s",
                run_id,
            )

    _emit_app_insights(stats)


def mark_run_failed(run_id: str, error: str) -> None:
    """Update the log row to 'failed' on orchestrator error.

    Persists the error text to ``error_message`` so failures are visible in SQL.
    Falls back to a column-less update if the migration
    (``location_scraper_logs_error_message.sql``) has not been applied yet.
    """
    sql = get_sql_client()
    error_text = (error or "")[:4000]
    try:
        affected = sql.execute_non_query(
            "UPDATE bronze.n8n_location_scraper_logs "
            "SET status = 'failed', error_message = ?, updated_at = GETDATE() "
            "WHERE run_id = ?",
            (error_text, run_id),
        )
        insert_with_error = True
    except Exception:
        # error_message column not present yet — degrade gracefully.
        affected = sql.execute_non_query(
            "UPDATE bronze.n8n_location_scraper_logs "
            "SET status = 'failed', updated_at = GETDATE() "
            "WHERE run_id = ?",
            (run_id,),
        )
        insert_with_error = False

    if affected == 0:
        if insert_with_error:
            sql.execute_non_query(
                "INSERT INTO bronze.n8n_location_scraper_logs "
                "(run_id, city, run_date, source, buildings_found, buildings_new, buildings_updated, status, error_message, updated_at) "
                "VALUES (?, '', ?, '', 0, 0, 0, 'failed', ?, GETDATE())",
                (run_id, date.today().isoformat(), error_text),
            )
        else:
            sql.execute_non_query(
                "INSERT INTO bronze.n8n_location_scraper_logs "
                "(run_id, city, run_date, source, buildings_found, buildings_new, buildings_updated, status, updated_at) "
                "VALUES (?, '', ?, '', 0, 0, 0, 'failed', GETDATE())",
                (run_id, date.today().isoformat()),
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
