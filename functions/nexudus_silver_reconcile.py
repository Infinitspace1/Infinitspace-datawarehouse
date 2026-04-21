"""
functions/nexudus_silver_reconcile.py

Timer trigger (weekly): reconciles the slower-moving Nexudus silver tables
against the live API. Any row whose source_id no longer appears in the
current API response is soft-deleted (is_deleted = 1, deleted_at = now).
Rows that reappear after being marked deleted are restored.

Entities covered here:
  - silver.nexudus_locations       (sys/businesses)
  - silver.nexudus_products        (sys/floorplandesks)
  - silver.nexudus_contracts       (billing/coworkercontracts)
  - silver.nexudus_extra_services  (billing/extraservices)
  - silver.nexudus_resources       (spaces/resources)
  - silver.nexudus_coworkers       (spaces/coworkers)

Not covered here (handled elsewhere):
  - silver.nexudus_coworker_invoices       -> nexudus_invoice_reconcile (daily)
  - silver.nexudus_coworker_invoice_lines  -> nexudus_invoice_reconcile (daily)
  - silver.bamboohr_employees              -> bamboohr_sync (daily, embedded)

Schedule default: Sunday 01:00 UTC. Run once per week, before Monday's
regular bronze sync at 02:00.

Safety:
  Per-entity `min_ids` floor aborts that entity's reconcile if the API
  returns fewer records than expected — prevents an empty response from
  wiping the silver table.
"""
from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any

import azure.functions as func

from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("NEXUDUS_SILVER_RECONCILE_SCHEDULE", "0 0 1 * * 0")


@dataclass(frozen=True)
class EntityConfig:
    entity: str          # logging/meta name
    silver_table: str    # full silver table name
    endpoint: str        # Nexudus list endpoint
    min_ids: int         # safety floor — abort if fewer active ids fetched


ENTITIES: tuple[EntityConfig, ...] = (
    EntityConfig("locations",      "silver.nexudus_locations",      "sys/businesses",           min_ids=1),
    EntityConfig("products",       "silver.nexudus_products",       "sys/floorplandesks",       min_ids=10),
    EntityConfig("contracts",      "silver.nexudus_contracts",      "billing/coworkercontracts", min_ids=10),
    EntityConfig("extra_services", "silver.nexudus_extra_services", "billing/extraservices",    min_ids=1),
    EntityConfig("resources",      "silver.nexudus_resources",      "spaces/resources",         min_ids=10),
    EntityConfig("coworkers",      "silver.nexudus_coworkers",      "spaces/coworkers",         min_ids=10),
)


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def nexudus_silver_reconcile(timer: func.TimerRequest) -> None:
    logger.info("Nexudus silver reconcile started (weekly)")

    try:
        bearer_token = get_bearer_token()
    except EnvironmentError as e:
        logger.error("Auth failed: %s", e)
        raise

    run_id = uuid.uuid4()
    totals = {"deleted": 0, "restored": 0, "entities_ok": 0, "entities_failed": 0}

    async with NexudusClient(bearer_token) as client:
        for config in ENTITIES:
            try:
                deleted, restored = await _reconcile_entity(client, config, run_id)
                totals["deleted"] += deleted
                totals["restored"] += restored
                totals["entities_ok"] += 1
            except Exception as exc:
                logger.error(
                    "Nexudus silver reconcile: %s failed — %s",
                    config.entity, exc,
                )
                totals["entities_failed"] += 1

    logger.info(
        "Nexudus silver reconcile complete: "
        "entities_ok=%s, entities_failed=%s, total_deleted=%s, total_restored=%s [run_id=%s]",
        totals["entities_ok"], totals["entities_failed"],
        totals["deleted"], totals["restored"], run_id,
    )


async def _reconcile_entity(
    client: NexudusClient,
    config: EntityConfig,
    run_id: uuid.UUID,
) -> tuple[int, int]:
    """Reconcile one entity. Returns (deleted, restored)."""
    async with RunTracker(
        "nexudus", f"{config.entity}_reconcile", "silver", metadata=str(run_id)
    ) as run:
        records = await client.get_all(config.endpoint)
        active_ids = {int(r["Id"]) for r in records if r.get("Id")}
        run.rows_read = len(active_ids)

        logger.info(
            "Reconcile %s: fetched %s active ids",
            config.entity, len(active_ids),
        )

        if len(active_ids) < config.min_ids:
            raise RuntimeError(
                f"Safety abort for {config.entity}: {len(active_ids)} ids fetched "
                f"(threshold {config.min_ids})"
            )

        deleted, restored = _soft_delete_missing(config.silver_table, active_ids)
        run.rows_written = deleted
        run.rows_skipped = restored

        logger.info(
            "Reconcile %s: %s soft-deleted, %s restored",
            config.entity, deleted, restored,
        )
        return deleted, restored


def _soft_delete_missing(
    silver_table: str,
    active_ids: set[int],
) -> tuple[int, int]:
    """
    Generic soft-delete / restore against `silver_table.source_id`.

    `silver_table` is interpolated into the SQL — must be an allowlisted
    name, never user-supplied. All ENTITIES values are hard-coded above.
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

        cursor.execute(
            f"""
            UPDATE {silver_table}
            SET is_deleted = 1, deleted_at = GETUTCDATE()
            WHERE is_deleted = 0
              AND source_id NOT IN (SELECT source_id FROM #active_ids)
            """
        )
        deleted = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute(
            f"""
            UPDATE {silver_table}
            SET is_deleted = 0, deleted_at = NULL
            WHERE is_deleted = 1
              AND source_id IN (SELECT source_id FROM #active_ids)
            """
        )
        restored = cursor.rowcount if cursor.rowcount is not None else 0

        cursor.execute("DROP TABLE #active_ids")
        return deleted, restored
