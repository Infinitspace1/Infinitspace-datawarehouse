"""
functions/eventbrite_sync.py

Timer trigger: pulls ALL Eventbrite events (every organization the token
can access, status=all, with venue/ticket/organizer expansions) daily and
writes bronze + silver in sequence, then reconciles silver against the
full fetched id set to soft-delete events removed from Eventbrite.

Mirrors the self-contained bamboohr_sync pattern. Full fetch by design:
event status, capacity and ticket availability keep changing, so an
incremental watermark would miss updates; bronze hash-dedup keeps
unchanged rows cheap.

Gated behind ENABLE_EVENTBRITE_FUNCTIONS; needs EVENTBRITE_PRIVATE_TOKEN
(see shared/eventbrite/client.py for the one-time auth setup).

Default schedule: 05:50 UTC.
"""
from __future__ import annotations

import logging
import os
import uuid

import azure.functions as func

from shared.azure_clients.eventbrite_bronze_writer import EventbriteBronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_writer_eventbrite import SilverEventbriteEventsWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.eventbrite.client import fetch_events, fetch_organizations

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("EVENTBRITE_SYNC_SCHEDULE", "0 50 5 * * *")
# Safety floor — if the fetched event set is smaller than this, skip reconcile.
# Default 1: with zero events fetched we cannot tell "all deleted" from
# "API returned nothing", so we never wipe the table on an empty response.
MIN_IDS = int(os.getenv("EVENTBRITE_RECONCILE_MIN_IDS", "1"))


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def eventbrite_sync(timer: func.TimerRequest) -> None:
    await run_eventbrite_sync()


async def run_eventbrite_sync(run_id: uuid.UUID | None = None) -> dict:
    """Run the Eventbrite events sync.

    Shared by the timer trigger and the local validation script so every
    path runs the identical RunTracker-tracked logic. Returns a summary dict.
    """
    run_id = run_id or uuid.uuid4()
    logger.info("Eventbrite events sync started [run_id=%s]", run_id)

    summary: dict = {
        "organizations": 0, "events_read": 0, "bronze_written": 0,
        "silver": {}, "reconcile": {},
    }

    # ── Bronze ───────────────────────────────────────────────
    events: list[dict] = []
    async with RunTracker("eventbrite", "events", "bronze", metadata=str(run_id)) as bronze_run:
        organizations = fetch_organizations()
        summary["organizations"] = len(organizations)
        logger.info("Eventbrite: %s organization(s) accessible", len(organizations))

        for org in organizations:
            org_id = str(org.get("id"))
            org_events = fetch_events(org_id, status="all")
            logger.info(
                "Eventbrite org %s (%s): %s events fetched",
                org_id, org.get("name"), len(org_events),
            )
            events.extend(org_events)

        bronze_run.rows_read = len(events)
        writer = EventbriteBronzeWriter(run_id)
        _changed, bronze_run.rows_written = writer.write_events(events)
        bronze_run.rows_skipped = bronze_run.rows_read - bronze_run.rows_written
        summary["events_read"] = bronze_run.rows_read
        summary["bronze_written"] = bronze_run.rows_written
        logger.info(
            "Eventbrite bronze: %s fetched, %s written, %s unchanged",
            bronze_run.rows_read, bronze_run.rows_written, bronze_run.rows_skipped,
        )

    # ── Silver ───────────────────────────────────────────────
    async with RunTracker("eventbrite", "events", "silver", metadata=str(run_id)) as silver_run:
        result = SilverEventbriteEventsWriter(run_id).run()
        silver_run.rows_read = result["rows_read"]
        silver_run.rows_written = result["events"]
        silver_run.rows_skipped = result["errors"]
        summary["silver"] = result
        logger.info("Eventbrite silver complete: %s", result)

    # ── Reconcile (soft-delete events removed from Eventbrite) ──
    active_ids = {str(e["id"]) for e in events if e.get("id")}
    if len(active_ids) < MIN_IDS:
        logger.warning(
            "Eventbrite reconcile skipped: only %s ids fetched (threshold %s)",
            len(active_ids), MIN_IDS,
        )
        summary["reconcile"] = {"skipped": True, "ids": len(active_ids)}
    else:
        async with RunTracker(
            "eventbrite", "events_reconcile", "silver", metadata=str(run_id)
        ) as reconcile_run:
            reconcile_run.rows_read = len(active_ids)
            deleted, restored = _reconcile_events(active_ids)
            reconcile_run.rows_written = deleted
            reconcile_run.rows_skipped = restored
            summary["reconcile"] = {"deleted": deleted, "restored": restored}
            logger.info(
                "Eventbrite reconcile: %s soft-deleted, %s restored", deleted, restored,
            )

    logger.info("Eventbrite events sync complete [run_id=%s]: %s", run_id, summary)
    return summary


def _reconcile_events(active_ids: set[str]) -> tuple[int, int]:
    """
    Soft-delete silver events whose source_id is not in active_ids.
    Restore any previously-deleted event whose source_id reappears.

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
            UPDATE silver.eventbrite_events
            SET is_deleted = 1, deleted_at = GETUTCDATE()
            WHERE is_deleted = 0
              AND source_id NOT IN (SELECT source_id FROM #active_ids)
            """
        )
        deleted = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute(
            """
            UPDATE silver.eventbrite_events
            SET is_deleted = 0, deleted_at = NULL
            WHERE is_deleted = 1
              AND source_id IN (SELECT source_id FROM #active_ids)
            """
        )
        restored = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute("DROP TABLE #active_ids")
        return deleted, restored
