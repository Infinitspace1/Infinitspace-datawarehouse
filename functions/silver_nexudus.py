"""
functions/silver_nexudus.py

Blueprint: Timer trigger (daily) that enqueues one processing task per
Nexudus entity onto the "silver-sync-tasks" Azure Storage Queue.

Runs 30 minutes after bronze sync completes (02:30 UTC by default).

Previously this function ran all five entity transformations sequentially
in a single invocation, which routinely exceeded the 10-minute Function
timeout on larger datasets. The refactored design separates concerns:

  1. This orchestrator — fires at 02:30 UTC, enqueues 5 messages, exits
     in under 1 second.
  2. silver_worker.py (queue trigger) — one invocation per message, runs
     a single entity transformation in isolation (< 10 min each).

Benefits:
  - Five entities processed in parallel rather than serially.
  - One entity failing does not block the others.
  - Azure Queue auto-retries failed messages (up to 5 times by default).
  - All MERGE upserts are idempotent, so retries are safe.
"""
import logging
import os
import uuid

import azure.functions as func

from shared.azure_clients.queue_client import SilverTaskQueue

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("SILVER_SYNC_SCHEDULE", "0 30 2 * * *")

ENTITIES = [
    "locations",
    "products",
    "contracts",
    "coworker_invoices",
    "coworkers",
    "resources",
    "extra_services",
    "coworker_invoice_lines",
    # Phase 2 reference data (2026-05-28) — small tables that drive the
    # landlord dashboard's tariff → financial-account filter.
    "tariffs",
    "financial_accounts",
    # Events (2026-06-10) — calendar events + attendees + ticket products.
    "calendar_events",
    "event_attendees",
    "event_products",
]


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def bronze_to_silver(timer: func.TimerRequest) -> None:
    """Enqueue one silver transformation task per Nexudus entity."""
    sync_run_id = str(uuid.uuid4())
    logger.info(f"Bronze -> Silver orchestrator started [sync_run_id={sync_run_id}]")

    try:
        task_queue = SilverTaskQueue()
        for entity in ENTITIES:
            task_queue.enqueue_entity(entity, sync_run_id)

        logger.info(
            f"Bronze -> Silver: {len(ENTITIES)} tasks enqueued "
            f"[sync_run_id={sync_run_id}]"
        )

    except Exception as e:
        logger.error(f"Bronze -> Silver orchestrator failed to enqueue tasks: {e}", exc_info=True)
        raise
