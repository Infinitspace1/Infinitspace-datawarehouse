"""
shared/azure_clients/queue_client.py

Sends messages to Azure Storage Queue using DefaultAzureCredential.
Used by the silver orchestrator to enqueue one task per entity so that
workers can run in parallel on separate function invocations.

The queue "silver-sync-tasks" is created automatically on first use.
Auth: DefaultAzureCredential — consistent with blob_writer.py.
Account: AZURE_STORAGE_ACCOUNT_NAME (same storage account as blob snapshots).
"""
import json
import logging
import os
from datetime import datetime, timezone

from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueServiceClient

logger = logging.getLogger(__name__)

SILVER_TASK_QUEUE = "silver-sync-tasks"


class SilverTaskQueue:
    """Sends silver transformation tasks to the Azure Storage Queue."""

    def __init__(self):
        account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "staccinfinitspaceprod001")
        if not account_name:
            raise EnvironmentError("AZURE_STORAGE_ACCOUNT_NAME is required for queue client")

        account_url = f"https://{account_name}.queue.core.windows.net"
        credential = DefaultAzureCredential()
        service = QueueServiceClient(account_url=account_url, credential=credential)
        self._queue = service.get_queue_client(SILVER_TASK_QUEUE)

        try:
            self._queue.create_queue()
            logger.info(f"Created queue '{SILVER_TASK_QUEUE}'")
        except Exception:
            pass  # Queue already exists — this is the normal case

    def enqueue_entity(self, entity: str, sync_run_id: str) -> None:
        """
        Send one silver transformation task to the queue.

        Message format:
            {
                "entity": "locations|products|contracts|resources|extra_services",
                "sync_run_id": "<uuid-str>",
                "enqueued_at": "<iso-datetime>"
            }

        The Azure SDK base64-encodes the message body automatically.
        """
        message = json.dumps({
            "entity": entity,
            "sync_run_id": sync_run_id,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        })
        self._queue.send_message(message)
        logger.info(f"Enqueued silver task: entity={entity} sync_run_id={sync_run_id}")
