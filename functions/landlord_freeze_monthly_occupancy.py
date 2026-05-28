"""
functions/landlord_freeze_monthly_occupancy.py

Phase 3 (2026-05-28): Monthly freeze of past occupancy for the landlord dashboard.

What it does:
    On the 1st of every month it computes the previous month's occupied
    workstations per location from gold.vw_landlord_membership_book_monthly
    and appends rows to silver.landlord_frozen_monthly_occupancy. From that
    point on, gold.vw_landlord_occupancy_combined returns the frozen number
    for that month — it's immune to retroactive changes in
    silver.nexudus_contracts.

Schedule:
    LANDLORD_FREEZE_OCCUPANCY_SCHEDULE  (NCRONTAB; default "0 0 4 1 * *")
    = 04:00 UTC on the 1st of every month. Runs AFTER:
      - Bronze sync       (02:00 UTC)
      - Silver sync       (02:30 UTC) — so membership_book_monthly is fresh
      - Sync health email (06:00 UTC) — happens after, picks up our row

Idempotency:
    INSERT WHERE NOT EXISTS — running twice on the same day is a no-op for
    rows we already wrote. The cron is safe to retry.

Backfill safety:
    Daniel's manual backfill rows (source='daniel_backfill') are NEVER
    touched by this cron. The WHERE NOT EXISTS guards against duplicates
    on (location_source_id, period).
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("LANDLORD_FREEZE_OCCUPANCY_SCHEDULE", "0 0 4 1 * *")


def _previous_month_period(today: datetime) -> str:
    """For today=2026-06-01, returns '2026-05'. The month we're freezing."""
    y, m = today.year, today.month - 1
    if m == 0:
        m = 12
        y -= 1
    return f"{y}-{m:02d}"


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def landlord_freeze_monthly_occupancy(timer: func.TimerRequest) -> None:
    """Freeze last month's occupancy per location."""
    today = datetime.now(timezone.utc)
    period_to_freeze = _previous_month_period(today)
    run_id = uuid.uuid4()

    logger.info(
        "Landlord freeze starting: period=%s run_id=%s",
        period_to_freeze, run_id,
    )

    async with RunTracker(
        "landlord_dashboard", "freeze_monthly_occupancy", "silver",
        metadata=f"period={period_to_freeze} run_id={run_id}",
    ) as run:
        sql = get_sql_client()

        # Pull last month's occupancy per location from the membership view.
        # Only locations with at least one membership-fee contract appear.
        source_rows = sql.execute_query("""
            SELECT
                location_source_id,
                location_name,
                period,
                occupied_workstations
            FROM gold.vw_landlord_membership_book_monthly
            WHERE period = ?
              AND location_source_id IS NOT NULL
        """, (period_to_freeze,))

        run.rows_read = len(source_rows)
        if not source_rows:
            logger.warning(
                "No rows found in vw_landlord_membership_book_monthly for "
                "period=%s — nothing to freeze. Check that silver is fresh.",
                period_to_freeze,
            )
            return

        # Insert each (location, period) row if it doesn't already exist.
        # The unique constraint on (location_source_id, period) protects
        # against accidental duplicates if the cron fires twice.
        inserted = 0
        skipped = 0
        for r in source_rows:
            try:
                affected = sql.execute_non_query("""
                    INSERT INTO silver.landlord_frozen_monthly_occupancy
                        (location_source_id, period, occupied_workstations, source, notes)
                    SELECT ?, ?, ?, 'cron', ?
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM silver.landlord_frozen_monthly_occupancy
                        WHERE location_source_id = ?
                          AND period = ?
                    )
                """, (
                    r["location_source_id"],
                    r["period"],
                    int(r["occupied_workstations"] or 0),
                    f"Frozen by cron at {today.isoformat()} run_id={run_id}",
                    r["location_source_id"],
                    r["period"],
                ))
                if affected > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as exc:
                logger.warning(
                    "Failed to freeze %s/%s: %s",
                    r["location_name"], r["period"], exc,
                )
                run.log_error(
                    str(r["location_source_id"]),
                    exc,
                    raw_payload=f"period={r['period']}",
                )

        run.rows_written = inserted
        run.rows_skipped = skipped

        logger.info(
            "Landlord freeze complete: period=%s, locations_read=%d, "
            "inserted=%d, skipped_existing=%d",
            period_to_freeze, run.rows_read, inserted, skipped,
        )
