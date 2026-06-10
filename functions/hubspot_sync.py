"""
functions/hubspot_sync.py

Timer trigger: pulls ALL HubSpot marketing emails (content + embedded
KPI stats) daily and writes bronze + silver in sequence, then reconciles
silver against the full fetched id set to soft-delete emails removed
from HubSpot.

Mirrors the self-contained bamboohr_sync pattern. Full fetch by design:
stats for already-sent emails keep changing, so an incremental watermark
would miss KPI updates; bronze hash-dedup keeps unchanged rows cheap.

Gated behind ENABLE_HUBSPOT_FUNCTIONS; needs HUBSPOT_ACCESS_TOKEN
(private app token with the `content` scope).

Default schedule: 05:45 UTC.
"""
from __future__ import annotations

import logging
import os
import uuid

import azure.functions as func

from shared.azure_clients.hubspot_bronze_writer import HubspotBronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_writer_hubspot import SilverHubspotMarketingEmailsWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.hubspot.client import fetch_marketing_emails

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("HUBSPOT_SYNC_SCHEDULE", "0 45 5 * * *")
# Safety floor — if the fetched email set is smaller than this, skip reconcile
MIN_IDS = int(os.getenv("HUBSPOT_RECONCILE_MIN_IDS", "5"))


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def hubspot_sync(timer: func.TimerRequest) -> None:
    await run_hubspot_sync()


async def run_hubspot_sync(run_id: uuid.UUID | None = None) -> dict:
    """Run the HubSpot marketing email sync.

    Shared by the timer trigger and the local validation script so every
    path runs the identical RunTracker-tracked logic. Returns a summary dict.
    """
    run_id = run_id or uuid.uuid4()
    logger.info("HubSpot marketing email sync started [run_id=%s]", run_id)

    summary: dict = {"emails_read": 0, "bronze_written": 0, "silver": {}, "reconcile": {}}

    # ── Bronze ───────────────────────────────────────────────
    async with RunTracker("hubspot", "marketing_emails", "bronze", metadata=str(run_id)) as bronze_run:
        emails = fetch_marketing_emails(include_stats=True)
        bronze_run.rows_read = len(emails)
        writer = HubspotBronzeWriter(run_id)
        _changed, bronze_run.rows_written = writer.write_marketing_emails(emails)
        bronze_run.rows_skipped = bronze_run.rows_read - bronze_run.rows_written
        summary["emails_read"] = bronze_run.rows_read
        summary["bronze_written"] = bronze_run.rows_written
        logger.info(
            "HubSpot bronze: %s fetched, %s written, %s unchanged",
            bronze_run.rows_read, bronze_run.rows_written, bronze_run.rows_skipped,
        )

    # ── Silver ───────────────────────────────────────────────
    async with RunTracker("hubspot", "marketing_emails", "silver", metadata=str(run_id)) as silver_run:
        result = SilverHubspotMarketingEmailsWriter(run_id).run()
        silver_run.rows_read = result["rows_read"]
        silver_run.rows_written = result["marketing_emails"]
        silver_run.rows_skipped = result["errors"]
        summary["silver"] = result
        logger.info("HubSpot silver complete: %s", result)

    # ── Reconcile (soft-delete emails removed from HubSpot) ──
    active_ids = {str(e["id"]) for e in emails if e.get("id")}
    if len(active_ids) < MIN_IDS:
        logger.warning(
            "HubSpot reconcile skipped: only %s ids fetched (threshold %s)",
            len(active_ids), MIN_IDS,
        )
        summary["reconcile"] = {"skipped": True, "ids": len(active_ids)}
    else:
        async with RunTracker(
            "hubspot", "marketing_emails_reconcile", "silver", metadata=str(run_id)
        ) as reconcile_run:
            reconcile_run.rows_read = len(active_ids)
            deleted, restored = _reconcile_emails(active_ids)
            reconcile_run.rows_written = deleted
            reconcile_run.rows_skipped = restored
            summary["reconcile"] = {"deleted": deleted, "restored": restored}
            logger.info(
                "HubSpot reconcile: %s soft-deleted, %s restored", deleted, restored,
            )

    logger.info("HubSpot marketing email sync complete [run_id=%s]: %s", run_id, summary)
    return summary


def _reconcile_emails(active_ids: set[str]) -> tuple[int, int]:
    """
    Soft-delete silver emails whose source_id is not in active_ids.
    Restore any previously-deleted email whose source_id reappears.

    Returns (deleted_count, restored_count).
    """
    sql = get_sql_client()
    with sql.get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE #active_ids (source_id NVARCHAR(64) NOT NULL PRIMARY KEY)")
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
            UPDATE silver.hubspot_marketing_emails
            SET is_deleted = 1, deleted_at = GETUTCDATE()
            WHERE is_deleted = 0
              AND source_id NOT IN (SELECT source_id FROM #active_ids)
            """
        )
        deleted = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute(
            """
            UPDATE silver.hubspot_marketing_emails
            SET is_deleted = 0, deleted_at = NULL
            WHERE is_deleted = 1
              AND source_id IN (SELECT source_id FROM #active_ids)
            """
        )
        restored = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute("DROP TABLE #active_ids")
        return deleted, restored
