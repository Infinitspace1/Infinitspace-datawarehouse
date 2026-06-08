"""
functions/competence_sync.py

Sync of the Firestore `competence_new` collection (TeamAndy lead-gen competitor
lists + competitors) into bronze + silver.

Two timer triggers (both registered from this blueprint):
  - competence_sync           (Mon-Sat 04:30 UTC) — INCREMENTAL: read all parent
      lists (cheap, ~tens of docs) + only competitors changed since the last run
      (collection-group `updated_at` watermark). No reconcile.
  - competence_full_reconcile (Sun 04:00 UTC)      — FULL: read every list +
      competitor, then soft-delete reconcile against the complete Firestore id
      set. This is also the safety net for deletions and anything the
      incremental query might miss.

The bronze writers hash-dedup and the silver writer is watermark-driven, so even
a full read only WRITES new/changed rows. The incremental read additionally
avoids re-fetching unchanged competitors from Firestore.

Incremental reads need a Firestore COLLECTION_GROUP index on
`competitors.updated_at` (Firestore returns a creation link on first use). While
that index is missing/building the incremental run logs a warning and falls back
to a full competitor read, so the sync keeps working.

Mirrors the repo's incremental-daily + periodic-full-reconcile pattern
(cf. nexudus bronze + nexudus_silver_reconcile). Gated behind
ENABLE_COMPETENCE_FUNCTIONS; needs FIREBASE_CREDENTIALS (or
FIREBASE_SERVICE_ACCOUNT_KEY_FILE).
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import timedelta, timezone

import azure.functions as func
from google.api_core.exceptions import FailedPrecondition

from shared.azure_clients.competence_bronze_writer import CompetenceBronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_sync import get_last_successful_run_started_at
from shared.azure_clients.silver_writer_competence import SilverCompetenceWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.firebase.client import get_firestore_client
from shared.firebase.competence import (
    compose_competitor_source_id,
    competitor_doc_id,
    read_competence,
    read_competence_lists,
    read_competitors_since,
)

logger = logging.getLogger(__name__)

bp = func.Blueprint()

# Incremental Mon-Sat; full reconcile on Sunday — so the two never overlap.
SCHEDULE = os.getenv("COMPETENCE_SYNC_SCHEDULE", "0 30 4 * * 1-6")
FULL_SCHEDULE = os.getenv("COMPETENCE_FULL_SYNC_SCHEDULE", "0 0 4 * * 0")
# Re-read this many minutes before the last run's start to absorb clock skew
# between Firestore (updated_at) and the warehouse (sync_runs.started_at).
INCREMENTAL_OVERLAP_MINUTES = int(os.getenv("COMPETENCE_INCREMENTAL_OVERLAP_MINUTES", "60"))
# Safety floor — skip the reconcile if fewer than this many competitors were read
# (protects against a failed/empty full read soft-deleting the whole table).
MIN_IDS = int(os.getenv("COMPETENCE_RECONCILE_MIN_IDS", "10"))


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def competence_sync(timer: func.TimerRequest) -> None:
    """Daily incremental sync (no reconcile)."""
    await run_competence_sync(mode="incremental")


@bp.timer_trigger(schedule=FULL_SCHEDULE, arg_name="timer", run_on_startup=False)
async def competence_full_reconcile(timer: func.TimerRequest) -> None:
    """Weekly full read + soft-delete reconcile."""
    await run_competence_sync(mode="full")


def _flatten_competitors(records: list[dict]) -> list[tuple[str, str, dict]]:
    return [
        (sid, r["list_id"], comp)
        for r in records
        for (sid, comp) in r["competitors"]
    ]


async def run_competence_sync(mode: str = "incremental", run_id: uuid.UUID | None = None) -> dict:
    """Run the competence_new sync.

    mode="incremental": read all parent lists + only competitors changed since the
        last successful run (updated_at watermark; the first run, with no
        watermark yet, does a full competitor read). No reconcile.
    mode="full": read every list + competitor, then reconcile (soft-delete records
        absent from Firestore).

    Shared by the timer triggers and the local validation script so every path
    runs the identical RunTracker-tracked logic. Returns a summary dict.
    """
    run_id = run_id or uuid.uuid4()
    full = mode == "full"
    logger.info("Competence sync started [mode=%s run_id=%s]", mode, run_id)

    db = get_firestore_client()

    # ── Read from Firestore ──────────────────────────────────
    active_list_ids = active_comp_ids = None  # set only in full mode (for reconcile)

    if full:
        records = read_competence(db)
        lists_input = [(r["list_id"], r["data"]) for r in records]
        competitors_input = _flatten_competitors(records)
        active_list_ids = {r["list_id"] for r in records}
        active_comp_ids = {sid for r in records for sid, _ in r["competitors"]}
    else:
        lists_input = read_competence_lists(db)
        watermark = _competitor_watermark()
        if watermark is None:
            logger.info("Competence incremental: no watermark yet, doing initial full competitor read")
            competitors_input = _flatten_competitors(read_competence(db))
        else:
            try:
                competitors_input = read_competitors_since(db, watermark)
                # Legacy v1 lists keep competitors in an in-doc array (no
                # subcollection), so the collection-group query misses them; pull
                # them from the parents we already read. Hash-dedup skips unchanged
                # rows, and the composite source_id matches the v2 scheme so there
                # is no double-count for mid-migration lists.
                for list_id, data in lists_input:
                    for comp in (data.get("competitors") or []):
                        competitors_input.append(
                            (compose_competitor_source_id(list_id, competitor_doc_id(comp)), list_id, comp)
                        )
            except FailedPrecondition as exc:
                logger.warning(
                    "Competence incremental query needs a Firestore index (missing or "
                    "still building) — falling back to a full competitor read this run. %s",
                    exc,
                )
                competitors_input = _flatten_competitors(read_competence(db))

    summary = {
        "mode": mode,
        "lists_read": len(lists_input),
        "competitors_read": len(competitors_input),
        "lists_written": 0,
        "competitors_written": 0,
        "silver": {},
        "reconcile": {},
    }

    writer = CompetenceBronzeWriter(run_id)

    # ── Bronze: lists ────────────────────────────────────────
    async with RunTracker("competence", "competence_lists", "bronze", metadata=str(run_id)) as run:
        run.rows_read = len(lists_input)
        _changed, run.rows_written = writer.write_lists(lists_input)
        run.rows_skipped = run.rows_read - run.rows_written
        summary["lists_written"] = run.rows_written
        logger.info(
            "Competence bronze lists: %s read, %s written, %s unchanged",
            run.rows_read, run.rows_written, run.rows_skipped,
        )

    # ── Bronze: competitors ──────────────────────────────────
    async with RunTracker("competence", "competence_competitors", "bronze", metadata=str(run_id)) as run:
        run.rows_read = len(competitors_input)
        _changed, run.rows_written = writer.write_competitors(competitors_input)
        run.rows_skipped = run.rows_read - run.rows_written
        summary["competitors_written"] = run.rows_written
        logger.info(
            "Competence bronze competitors: %s read, %s written, %s unchanged",
            run.rows_read, run.rows_written, run.rows_skipped,
        )

    # ── Silver ───────────────────────────────────────────────
    async with RunTracker("competence", "competence", "silver", metadata=str(run_id)) as run:
        result = SilverCompetenceWriter(run_id).run()
        run.rows_read = result["rows_read"]
        run.rows_written = result["lists_written"] + result["competitors_written"]
        run.rows_skipped = result["errors"]
        summary["silver"] = result
        logger.info("Competence silver complete: %s", result)

    # ── Reconcile (full mode only) ───────────────────────────
    if full:
        if len(active_comp_ids) < MIN_IDS:
            logger.warning(
                "Competence reconcile skipped: only %s competitor ids fetched (threshold %s)",
                len(active_comp_ids), MIN_IDS,
            )
            summary["reconcile"] = {"skipped": True, "competitor_ids": len(active_comp_ids)}
        else:
            async with RunTracker("competence", "competence_reconcile", "silver", metadata=str(run_id)) as run:
                run.rows_read = len(active_list_ids) + len(active_comp_ids)
                del_lists, res_lists = _reconcile("silver.competence_lists", active_list_ids)
                del_comps, res_comps = _reconcile("silver.competence_competitors", active_comp_ids)
                run.rows_written = del_lists + del_comps   # soft-deleted
                run.rows_skipped = res_lists + res_comps   # restored
                summary["reconcile"] = {
                    "lists_deleted": del_lists, "lists_restored": res_lists,
                    "competitors_deleted": del_comps, "competitors_restored": res_comps,
                }
                logger.info(
                    "Competence reconcile: lists soft-deleted=%s restored=%s, "
                    "competitors soft-deleted=%s restored=%s",
                    del_lists, res_lists, del_comps, res_comps,
                )

    logger.info("Competence sync complete [mode=%s run_id=%s]: %s", mode, run_id, summary)
    return summary


def _competitor_watermark():
    """Incremental read floor: the last successful competitors-bronze run start,
    minus an overlap buffer (returned tz-aware UTC). None -> do a full read."""
    ts = get_last_successful_run_started_at("competence", "competence_competitors", "bronze")
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts - timedelta(minutes=INCREMENTAL_OVERLAP_MINUTES)


def _reconcile(table: str, active_ids: set[str]) -> tuple[int, int]:
    """
    Soft-delete rows in `table` whose source_id is not in active_ids, and restore
    any previously-deleted row whose source_id reappears.

    `table` is a hard-coded silver table name (not user input). Returns
    (deleted_count, restored_count).
    """
    if not active_ids:
        return 0, 0

    sql = get_sql_client()
    with sql.get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE #active_ids (source_id NVARCHAR(450) NOT NULL PRIMARY KEY)")
        try:
            cursor.fast_executemany = True
        except AttributeError:
            pass
        cursor.executemany(
            "INSERT INTO #active_ids (source_id) VALUES (?)",
            [(i,) for i in active_ids],
        )

        cursor.execute(
            f"""
            UPDATE {table}
            SET is_deleted = 1, deleted_at = GETUTCDATE()
            WHERE is_deleted = 0
              AND source_id NOT IN (SELECT source_id FROM #active_ids)
            """
        )
        deleted = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute(
            f"""
            UPDATE {table}
            SET is_deleted = 0, deleted_at = NULL
            WHERE is_deleted = 1
              AND source_id IN (SELECT source_id FROM #active_ids)
            """
        )
        restored = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute("DROP TABLE #active_ids")
        return deleted, restored
