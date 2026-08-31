"""
functions/nexudus_helpdesk_sync.py

Blueprint: Timer trigger (every 15 minutes by default) that keeps the Nexudus
help desk ("customer requests") current in bronze + silver.

Entities (see shared/nexudus/helpdesk.py for the endpoint/filter registry):
  1. helpdesk_departments  -- GET /api/support/helpdeskdepartments
  2. helpdesk_messages     -- GET /api/support/helpdeskmessages
  3. helpdesk_comments     -- GET /api/support/helpdeskcomments

Order matters: messages are written before comments, because the comment
silver writer resolves each comment's location from its parent message's
bronze row (comment payloads carry no BusinessId).

This is the RECONCILING half of a hybrid design. The webhook
(functions/nexudus_helpdesk_webhook.py) delivers new tickets and replies in
seconds; this poll exists because Nexudus has no update/close/assign webhook
event at all, so ticket lifecycle — Closed, ClosedOn, OwnerId,
FirstResponseTimeInMinutes — only ever arrives by re-reading. It also
backstops webhook delivery failure, which Nexudus punishes by silently
disabling the hook after 10 consecutive errors.

Self-contained (bronze + silver inline in one invocation) rather than going
through the silver-sync-tasks queue fanout, because that fanout is tied to
the 02:30 nightly orchestrator and the whole point here is freshness.

Cost: each run is ~3 API calls returning near-zero rows. The SHA-256 payload
hash in BronzeWriter means unchanged records are not written, and the silver
watermark means unchanged bronze rows are not re-transformed.
"""
from __future__ import annotations

import logging
import os
import uuid

import azure.functions as func

from shared.azure_clients.bronze_writer import BronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.silver_sync import get_last_successful_run_started_at
from shared.azure_clients.silver_writer_helpdesk_comments import SilverHelpdeskCommentsWriter
from shared.azure_clients.silver_writer_helpdesk_departments import SilverHelpdeskDepartmentsWriter
from shared.azure_clients.silver_writer_helpdesk_messages import SilverHelpdeskMessagesWriter
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient
from shared.nexudus.helpdesk import (
    COMMENTS,
    DEPARTMENTS,
    ENTITIES,
    MESSAGES,
    check_webhook_health,
    incremental_params,
)

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("NEXUDUS_HELPDESK_SYNC_SCHEDULE", "0 */15 * * * *")

# Departments first (tiny reference table), then messages, then comments —
# comments depend on messages being in bronze to inherit their location.
SYNC_ORDER = (DEPARTMENTS, MESSAGES, COMMENTS)

_SILVER_WRITERS = {
    MESSAGES: (SilverHelpdeskMessagesWriter, "helpdesk_messages"),
    COMMENTS: (SilverHelpdeskCommentsWriter, "helpdesk_comments"),
    DEPARTMENTS: (SilverHelpdeskDepartmentsWriter, "helpdesk_departments"),
}


def _has_previous_bronze_run(entity: str) -> bool:
    """True once this entity has completed at least one successful bronze run."""
    try:
        return get_last_successful_run_started_at("nexudus", entity, "bronze") is not None
    except Exception as exc:  # noqa: BLE001 — never let this decide the run fails
        logger.warning("Could not read %s bronze watermark (%s) — assuming full fetch", entity, exc)
        return False


async def sync_helpdesk_entity(
    client: NexudusClient,
    writer: BronzeWriter,
    entity_key: str,
    run_id: uuid.UUID,
    force_full: bool = False,
    triggered_by: str = "timer",
) -> dict[str, int]:
    """Bronze fetch/write + silver promote for one help-desk entity."""
    entity = ENTITIES[entity_key]
    result: dict[str, int] = {"fetched": 0, "changed": 0, "bronze_written": 0, "silver_written": 0}

    # ── Bronze ──
    has_previous = False if force_full else _has_previous_bronze_run(entity_key)
    extra_params = incremental_params(entity, has_previous)

    async with RunTracker(
        "nexudus", entity_key, "bronze", triggered_by=triggered_by, metadata=str(run_id)
    ) as run:
        records = await client.get_all(entity.endpoint, extra_params=extra_params)
        run.rows_read = len(records)
        changed, run.rows_written = getattr(writer, entity.bronze_method)(records)
        run.rows_skipped = len(records) - len(changed)
        result["fetched"] = run.rows_read
        result["changed"] = len(changed)
        result["bronze_written"] = run.rows_written
        logger.info(
            "%s: %s fetched, %s changed, %s skipped, %s written to bronze",
            entity_key, run.rows_read, len(changed), run.rows_skipped, run.rows_written,
        )

    # ── Silver ──
    writer_cls, result_key = _SILVER_WRITERS[entity_key]
    async with RunTracker(
        "nexudus", entity_key, "silver", triggered_by=triggered_by, metadata=str(run_id)
    ) as run:
        silver_result = writer_cls(run_id).run()
        run.rows_read = int(silver_result.get("rows_read") or 0)
        run.rows_written = int(silver_result.get(result_key) or 0)
        result["silver_written"] = run.rows_written
        logger.info("%s: %s upserted to silver", entity_key, run.rows_written)

    return result


async def sync_helpdesk(
    client: NexudusClient,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    entities: tuple[str, ...] = SYNC_ORDER,
    force_full: bool = False,
    triggered_by: str = "timer",
) -> dict[str, dict[str, int]]:
    """Run the full help-desk sync. One entity failing does not stop the rest."""
    totals: dict[str, dict[str, int]] = {}
    for entity_key in entities:
        try:
            totals[entity_key] = await sync_helpdesk_entity(
                client, writer, entity_key, run_id,
                force_full=force_full, triggered_by=triggered_by,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Help desk sync: %s failed — %s", entity_key, exc, exc_info=True)
            totals[entity_key] = {"error": str(exc)}  # type: ignore[dict-item]
    return totals


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def nexudus_helpdesk_sync(timer: func.TimerRequest) -> None:
    """Reconciling poll: keeps help-desk tickets, replies and departments fresh."""
    logger.info("Nexudus help desk sync started")

    try:
        bearer_token = get_bearer_token()
    except EnvironmentError as e:
        logger.error("Auth failed: %s", e)
        raise

    run_id = uuid.uuid4()
    async with NexudusClient(bearer_token) as client:
        totals = await sync_helpdesk(client, BronzeWriter(run_id), run_id)
        # Cheap tripwire: warns if Nexudus has auto-disabled the push hooks,
        # so a silent push outage surfaces in 15 minutes rather than never.
        await check_webhook_health(client)

    failed = [k for k, v in totals.items() if "error" in v]
    logger.info("Nexudus help desk sync complete: %s [run_id=%s]", totals, run_id)

    # Fail the invocation only if EVERY entity failed. A single entity failing
    # is logged and left for the next run 15 minutes later — but a total
    # failure means auth or the API is down and should reach the health report.
    if failed and len(failed) == len(totals):
        raise RuntimeError(f"Help desk sync failed for all entities: {failed}")
