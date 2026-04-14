"""
shared/azure_clients/queue_client.py

Sends messages to Azure Storage Queue using DefaultAzureCredential.
Used by the silver orchestrator to enqueue one task per entity so that
workers can run in parallel on separate function invocations.

The queue "silver-sync-tasks" is created automatically on first use.
Auth: DefaultAzureCredential - consistent with blob_writer.py.
Account: AZURE_STORAGE_ACCOUNT_NAME (same storage account as blob snapshots).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueClient

logger = logging.getLogger(__name__)

SILVER_TASK_QUEUE = "silver-sync-tasks"


class SilverTaskQueue:
    """Sends silver transformation tasks to the Azure Storage Queue."""

    def __init__(self) -> None:
        account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "staccinfinitspaceprod001")
        if not account_name:
            raise EnvironmentError("AZURE_STORAGE_ACCOUNT_NAME is required for queue client")

        # QueueClient expects the storage account root URL here; queue_name is
        # appended internally when building /<queue>/messages requests.
        account_url = f"https://{account_name}.queue.core.windows.net"
        credential = DefaultAzureCredential()
        # QueueClient directly - not via QueueServiceClient.get_queue_client(),
        # which silently ignores encode/decode policy kwargs.
        # message_encode_policy=None disables base64 encoding so the message
        # is stored as plain text, matching host.json messageEncoding: "none".
        self._queue = QueueClient(
            account_url=account_url,
            queue_name=SILVER_TASK_QUEUE,
            credential=credential,
            message_encode_policy=None,
            message_decode_policy=None,
        )

        try:
            self._queue.create_queue()
            logger.info("Created queue '%s'", SILVER_TASK_QUEUE)
        except Exception:
            pass  # Queue already exists - this is the normal case

    def enqueue_entity(self, entity: str, sync_run_id: str) -> None:
        """
        Send one silver transformation task to the queue.

        Message format:
            {
                "entity": "locations|products|contracts|resources|extra_services",
                "sync_run_id": "<uuid-str>",
                "enqueued_at": "<iso-datetime>"
            }

        Messages are sent as plain text (no base64) to match host.json
        messageEncoding: "none".
        """
        message = json.dumps(
            {
                "entity": entity,
                "sync_run_id": sync_run_id,
                "enqueued_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        self._queue.send_message(message)
        logger.info("Enqueued silver task: entity=%s sync_run_id=%s", entity, sync_run_id)
