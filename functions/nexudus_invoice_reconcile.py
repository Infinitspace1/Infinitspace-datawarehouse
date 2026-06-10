"""
functions/nexudus_invoice_reconcile.py

Timer trigger (daily): reconciles silver.nexudus_coworker_invoices against
the Nexudus API. Invoices deleted at the source are soft-deleted in silver
(is_deleted = 1, deleted_at = now). Invoice_lines belonging to deleted
invoices are cascaded.

Why this exists:
  The regular bronze sync uses a 2-day UpdatedOn lookback and therefore
  cannot see deletions (a deleted record simply stops appearing). This
  function fetches the full current invoice set within a bounded window
  and flags anything in silver that is no longer returned by the API.

Schedule: 05:15 UTC, after bronze/silver at 02:00 / 02:30 and before the
finance dashboard refresh at 05:30.

Reconcile window:
  Only invoices with due_date within the last N days are reconciled
  (default 365). Older invoices are out of scope for the finance
  dashboard and not worth the API load.

Safety:
  If the Nexudus fetch returns fewer than MIN_ACTIVE_IDS records the run
  aborts without soft-deleting anything — this protects against an empty
  API response wiping out silver.
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient
from shared.nexudus.exclusions import EXCLUDED_LOCATION_SOURCE_IDS

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("NEXUDUS_INVOICE_RECONCILE_SCHEDULE", "0 15 5 * * *")
RECONCILE_LOOKBACK_DAYS = int(
    os.getenv("NEXUDUS_INVOICE_RECONCILE_LOOKBACK_DAYS", "365")
)
# Safety floor — if Nexudus returns fewer than this many invoices, abort
MIN_ACTIVE_IDS = int(os.getenv("NEXUDUS_INVOICE_RECONCILE_MIN_IDS", "100"))


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def nexudus_invoice_reconcile(timer: func.TimerRequest) -> None:
    logger.info(
        "Nexudus invoice reconcile started (lookback=%s days)",
        RECONCILE_LOOKBACK_DAYS,
    )

    try:
        bearer_token = get_bearer_token()
    except EnvironmentError as e:
        logger.error("Auth failed: %s", e)
        raise

    run_id = uuid.uuid4()
    cutoff = datetime.now(timezone.utc) - timedelta(days=RECONCILE_LOOKBACK_DAYS)

    async with RunTracker(
        "nexudus", "coworker_invoices_reconcile", "silver", metadata=str(run_id)
    ) as run:
        async with NexudusClient(bearer_token) as client:
            active_ids = await _fetch_active_invoice_ids(client, cutoff)

        run.rows_read = len(active_ids)
        logger.info(
            "Nexudus invoice reconcile: fetched %s active invoice ids",
            len(active_ids),
        )

        if len(active_ids) < MIN_ACTIVE_IDS:
            raise RuntimeError(
                f"Safety abort: Nexudus returned only {len(active_ids)} invoice ids "
                f"(threshold {MIN_ACTIVE_IDS}). Refusing to soft-delete anything."
            )

        deleted_invoices, restored_invoices = _reconcile_invoices(active_ids, cutoff)
        deleted_lines, restored_lines = _reconcile_invoice_lines()

        run.rows_written = deleted_invoices + deleted_lines
        run.rows_skipped = restored_invoices + restored_lines

        logger.info(
            "Nexudus invoice reconcile complete: "
            "invoices [deleted=%s, restored=%s], "
            "invoice_lines [deleted=%s, restored=%s] [run_id=%s]",
            deleted_invoices, restored_invoices,
            deleted_lines, restored_lines, run_id,
        )


async def _fetch_active_invoice_ids(
    client: NexudusClient,
    cutoff: datetime,
) -> set[int]:
    """Fetch all currently-active invoice IDs within the reconcile window."""
    since = cutoff.strftime("%Y-%m-%dT%H:%M:%S")
    extra_params = {"from_CoworkerInvoice_DueDate": since}
    logger.info("Fetching active invoice ids (due_date >= %s)", since)

    records = await client.get_all(
        "billing/coworkerinvoices", extra_params=extra_params
    )
    return {int(r["Id"]) for r in records if r.get("Id")}


def _reconcile_invoices(active_ids: set[int], cutoff: datetime) -> tuple[int, int]:
    """
    Soft-delete silver invoices (within window) whose source_id is not in active_ids.
    Restore any previously-deleted invoice whose source_id reappears.

    Returns (deleted_count, restored_count).
    """
    sql = get_sql_client()
    with sql.get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE #active_ids (source_id BIGINT PRIMARY KEY)")
        try:
            cursor.fast_executemany = True
        except AttributeError:
            pass
        cursor.executemany(
            "INSERT INTO #active_ids (source_id) VALUES (?)",
            [(i,) for i in active_ids],
        )
        cursor.execute("CREATE TABLE #excluded_location_ids (source_id BIGINT PRIMARY KEY)")
        cursor.executemany(
            "INSERT INTO #excluded_location_ids (source_id) VALUES (?)",
            [(i,) for i in EXCLUDED_LOCATION_SOURCE_IDS],
        )

        cursor.execute(
            """
            UPDATE silver.nexudus_coworker_invoices
            SET is_deleted = 1, deleted_at = GETUTCDATE()
            WHERE is_deleted = 0
              AND due_date >= ?
              AND (
                    source_id NOT IN (SELECT source_id FROM #active_ids)
                 OR location_source_id IN (SELECT source_id FROM #excluded_location_ids)
              )
            """,
            (cutoff,),
        )
        deleted = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute(
            """
            UPDATE silver.nexudus_coworker_invoices
            SET is_deleted = 0, deleted_at = NULL
            WHERE is_deleted = 1
              AND source_id IN (SELECT source_id FROM #active_ids)
              AND (
                    location_source_id IS NULL
                 OR location_source_id NOT IN (SELECT source_id FROM #excluded_location_ids)
              )
            """
        )
        restored = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute("DROP TABLE #excluded_location_ids")
        cursor.execute("DROP TABLE #active_ids")

        logger.info("Invoices: %s soft-deleted, %s restored", deleted, restored)
        return deleted, restored


def _reconcile_invoice_lines() -> tuple[int, int]:
    """
    Cascade: align invoice_lines.is_deleted with their parent invoice.

    Returns (deleted_count, restored_count).
    """
    sql = get_sql_client()
    with sql.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE #excluded_location_ids (source_id BIGINT PRIMARY KEY)")
        cursor.executemany(
            "INSERT INTO #excluded_location_ids (source_id) VALUES (?)",
            [(i,) for i in EXCLUDED_LOCATION_SOURCE_IDS],
        )

        cursor.execute(
            """
            UPDATE ncil
            SET ncil.is_deleted = 1, ncil.deleted_at = GETUTCDATE()
            FROM silver.nexudus_coworker_invoice_lines ncil
            INNER JOIN silver.nexudus_coworker_invoices nci
                ON nci.source_id = ncil.invoice_source_id
            WHERE ncil.is_deleted = 0
              AND (
                    nci.is_deleted = 1
                 OR ncil.location_source_id IN (SELECT source_id FROM #excluded_location_ids)
              )
            """
        )
        deleted = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute(
            """
            UPDATE ncil
            SET ncil.is_deleted = 0, ncil.deleted_at = NULL
            FROM silver.nexudus_coworker_invoice_lines ncil
            INNER JOIN silver.nexudus_coworker_invoices nci
                ON nci.source_id = ncil.invoice_source_id
            WHERE ncil.is_deleted = 1
              AND nci.is_deleted = 0
              AND (
                    ncil.location_source_id IS NULL
                 OR ncil.location_source_id NOT IN (SELECT source_id FROM #excluded_location_ids)
              )
            """
        )
        restored = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute("DROP TABLE #excluded_location_ids")
        logger.info("Invoice lines: %s soft-deleted, %s restored", deleted, restored)
        return deleted, restored
