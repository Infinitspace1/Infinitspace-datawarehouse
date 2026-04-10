"""
functions/replyio_sync.py

Timer trigger: pulls Reply.io sequence steps and daily step performance
stats, then writes them to the bronze layer.

Default schedule: 05:30 UTC (after BambooHR sync).

Entities written:
  - bronze.replyio_sequence_steps       (step definitions per sequence)
  - bronze.replyio_sequence_step_performance  (daily stats per step)
"""
from __future__ import annotations

import logging
import os
import uuid

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.sql_client import get_sql_client
from shared.replyio.client import (
    SEQUENCE_IDS,
    SEQUENCE_META,
    date_window,
    fetch_sequence_steps,
    fetch_step_stats,
)

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("REPLYIO_SYNC_SCHEDULE", "0 30 5 * * *")


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def replyio_stats_sync(timer: func.TimerRequest) -> None:
    logger.info("Reply.io stats sync started")
    run_id = uuid.uuid4()
    run_date, from_ts, to_ts = date_window()

    sql = get_sql_client()

    # --- 1. Sync sequence steps -----------------------------------------
    total_steps_written = 0
    async with RunTracker("replyio", "sequence_steps", "bronze", metadata=str(run_id)) as steps_run:
        for sequence_id in SEQUENCE_IDS:
            steps = fetch_sequence_steps(sequence_id)
            steps_run.rows_read += len(steps)

            for step in steps:
                _upsert_sequence_step(sql, step, sequence_id)
                total_steps_written += 1

        steps_run.rows_written = total_steps_written
        logger.info(
            "Reply.io sequence steps: %s fetched, %s written across %s sequences",
            steps_run.rows_read, steps_run.rows_written, len(SEQUENCE_IDS),
        )

    # --- 2. Sync step performance stats ---------------------------------
    total_stats_written = 0
    async with RunTracker("replyio", "step_performance", "bronze", metadata=str(run_id)) as perf_run:
        for sequence_id in SEQUENCE_IDS:
            meta = SEQUENCE_META[sequence_id]
            steps = fetch_sequence_steps(sequence_id)

            for step in steps:
                step_id = step.get("id")
                if not step_id:
                    perf_run.rows_skipped += 1
                    continue

                perf_run.rows_read += 1
                try:
                    stats = fetch_step_stats(sequence_id, step_id, from_ts, to_ts)
                    _upsert_step_performance(sql, stats, run_date, meta)
                    total_stats_written += 1
                except Exception as exc:
                    logger.warning(
                        "Reply.io stats failed for sequence=%s step=%s: %s",
                        sequence_id, step_id, exc,
                    )
                    perf_run.rows_skipped += 1

        perf_run.rows_written = total_stats_written
        logger.info(
            "Reply.io step performance: %s fetched, %s written, %s skipped [date=%s]",
            perf_run.rows_read, perf_run.rows_written, perf_run.rows_skipped, run_date,
        )

    logger.info("Reply.io stats sync complete [run_id=%s, date=%s]", run_id, run_date)


# ---------------------------------------------------------------------------
# SQL upserts (DELETE + INSERT, matching original GCF behaviour)
# ---------------------------------------------------------------------------

def _upsert_sequence_step(sql, step: dict, sequence_id: int) -> None:
    """Upsert a step definition into bronze.replyio_sequence_steps."""
    step_id = step.get("id")
    with sql.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM bronze.replyio_sequence_steps WHERE step_id = ?",
            (step_id,),
        )
        cursor.execute(
            """
            INSERT INTO bronze.replyio_sequence_steps
                (step_id, sequence_id, step_number, step_type, subject, body)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                step_id,
                sequence_id,
                step.get("stepNumber"),
                step.get("type"),
                step.get("subject"),
                step.get("body"),
            ),
        )


def _upsert_step_performance(sql, stats: dict, run_date, meta: dict) -> None:
    """Upsert yesterday's performance row into bronze.replyio_sequence_step_performance."""
    campaign_id = stats.get("campaignId")
    step_id = stats.get("stepId")
    contacted = stats.get("contactedPeople") or 0
    interested = round((stats.get("interestedRate") or 0) * contacted / 100)

    with sql.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            DELETE FROM bronze.replyio_sequence_step_performance
            WHERE date = ? AND sequence_id = ? AND step_id = ?
            """,
            (run_date, campaign_id, step_id),
        )
        cursor.execute(
            """
            INSERT INTO bronze.replyio_sequence_step_performance
                (date, sequence_id, sequence_name, step_id,
                 ab_variant, language, sequence_type,
                 sent, delivered, opened, clicked, replied,
                 interested, bounced, opted_out, out_of_office)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                run_date,
                campaign_id,
                stats.get("campaignName"),
                step_id,
                meta.get("ab_variant"),
                meta.get("language"),
                meta.get("sequence_type"),
                stats.get("peopleSentTo") or 0,
                contacted,
                stats.get("openedPeople") or 0,
                stats.get("clickedPeople") or 0,
                stats.get("repliedPeople") or 0,
                interested,
                stats.get("bouncedPeople") or 0,
            ),
        )
