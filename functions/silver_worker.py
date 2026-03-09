"""
functions/silver_worker.py

Blueprint: Queue-triggered worker that processes a single Nexudus entity
from Bronze → Silver. Triggered by messages on the "silver-sync-tasks"
Azure Storage Queue (enqueued by bronze_to_silver in silver_nexudus.py).

Message format (JSON):
    {
        "entity": "locations|products|contracts|resources|extra_services",
        "sync_run_id": "<uuid-str>",
        "enqueued_at": "<iso-datetime>"
    }

Design properties:
  - Each invocation handles exactly one entity.
  - Five entities → five parallel queue messages → five worker invocations.
  - MERGE-based upserts are idempotent: Azure Queue retries are fully safe.
  - One entity failing raises an exception so Azure re-queues the message.
  - dequeue_count is logged so repeated failures are visible in App Insights.
  - Azure default: message is dead-lettered after 5 failed delivery attempts
    (configurable via maxDequeueCount in host.json).

Connection binding:
  - Uses AzureWebJobsStorage — the Function App's default storage account.
  - This is automatically set by the Azure Functions runtime and must match
    the storage account used by SilverTaskQueue (AZURE_STORAGE_ACCOUNT_NAME).
  - For local development, set AzureWebJobsStorage in local.settings.json
    (see .env.example for the storage account name).
"""
import json
import logging
import uuid

import azure.functions as func

from shared.azure_clients.silver_write_locations import SilverLocationsWriter
from shared.azure_clients.silver_writer_products import SilverProductsWriter
from shared.azure_clients.silver_writer_contracts import SilverContractsWriter
from shared.azure_clients.silver_writer_resources import SilverResourcesWriter
from shared.azure_clients.silver_writer_extra_services import SilverExtraServicesWriter
from shared.azure_clients.run_tracker import RunTracker

logger = logging.getLogger(__name__)

bp = func.Blueprint()

QUEUE_NAME = "silver-sync-tasks"

# Maps entity name → (WriterClass, rows_written_extractor)
# rows_written_extractor converts the writer's result dict to an integer count.
_ENTITY_MAP = {
    "locations": (
        SilverLocationsWriter,
        lambda r: r.get("locations", 0) + r.get("location_hours", 0),
    ),
    "products": (
        SilverProductsWriter,
        lambda r: r.get("products", 0),
    ),
    "contracts": (
        SilverContractsWriter,
        lambda r: r.get("contracts", 0),
    ),
    "resources": (
        SilverResourcesWriter,
        lambda r: r.get("resources", 0),
    ),
    "extra_services": (
        SilverExtraServicesWriter,
        lambda r: r.get("extra_services", 0),
    ),
}


@bp.queue_trigger(
    arg_name="msg",
    queue_name=QUEUE_NAME,
    connection="AzureWebJobsStorage",
)
async def silver_entity_worker(msg: func.QueueMessage) -> None:
    """
    Process one entity's Bronze → Silver transformation.

    Raises on failure so Azure re-queues the message for retry.
    After maxDequeueCount retries the message is moved to the poison queue
    (<queue-name>-poison) for manual inspection.
    """
    raw_body = msg.get_body().decode("utf-8")
    payload = json.loads(raw_body)

    entity: str = payload.get("entity", "")
    sync_run_id_str: str = payload.get("sync_run_id") or str(uuid.uuid4())
    enqueued_at: str = payload.get("enqueued_at", "unknown")
    dequeue_count: int = msg.dequeue_count

    logger.info(
        f"Silver worker received: entity={entity} "
        f"sync_run_id={sync_run_id_str} "
        f"enqueued_at={enqueued_at} "
        f"dequeue_count={dequeue_count}"
    )

    if entity not in _ENTITY_MAP:
        # Unrecognised entity — raising here will dead-letter the message
        # after maxDequeueCount retries so it can be inspected manually.
        logger.error(
            f"Unknown entity '{entity}' in queue message — "
            "message will be dead-lettered after max retries"
        )
        raise ValueError(f"Unknown silver entity: {entity!r}")

    writer_cls, rows_fn = _ENTITY_MAP[entity]
    sync_run_id = uuid.UUID(sync_run_id_str)

    async with RunTracker(
        "nexudus",
        entity,
        "silver",
        triggered_by="queue",
        metadata=sync_run_id_str,
    ) as run:
        writer = writer_cls(sync_run_id)
        result = writer.run()
        run.rows_written = rows_fn(result)
        logger.info(
            f"Silver worker complete: entity={entity} "
            f"result={result} "
            f"sync_run_id={sync_run_id_str}"
        )
