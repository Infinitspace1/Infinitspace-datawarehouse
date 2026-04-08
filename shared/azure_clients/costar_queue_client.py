"""
shared/azure_clients/costar_queue_client.py

Sends CoStar PDF extraction job messages to the Azure Storage Queue.
Used by the real_estate_costar HTTP handler so it can return 202 immediately
and let the queue-triggered worker handle the long-running extraction.

Queue: costar-extraction-tasks  (auto-created on first use)
Poison queue: costar-extraction-tasks-poison  (managed by Azure)

Auth: DefaultAzureCredential — consistent with queue_client.py and blob_writer.py.
Account: AZURE_STORAGE_ACCOUNT_NAME (same storage account as the rest of the platform).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueServiceClient

logger = logging.getLogger(__name__)

COSTAR_TASK_QUEUE = "costar-extraction-tasks"


class CostarTaskQueue:
    """Sends CoStar extraction jobs to the Azure Storage Queue."""

    def __init__(self):
        account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "staccinfinitspaceprod001")
        if not account_name:
            raise EnvironmentError("AZURE_STORAGE_ACCOUNT_NAME is required for CostarTaskQueue")

        account_url = f"https://{account_name}.queue.core.windows.net"
        credential = DefaultAzureCredential()
        service = QueueServiceClient(account_url=account_url, credential=credential)
        self._queue = service.get_queue_client(COSTAR_TASK_QUEUE)

        try:
            self._queue.create_queue()
            logger.info(f"Created queue '{COSTAR_TASK_QUEUE}'")
        except Exception:
            pass  # Queue already exists — normal case

    def enqueue_job(
        self,
        job_id: str,
        blob_name_upload: str,
        start_page: int,
        end_page: int,
    ) -> None:
        """
        Send one CoStar extraction job to the queue.

        Message format:
            {
                "job_id":           "<uuid>",
                "blob_name_upload": "<filename.pdf>",
                "start_page":       1,
                "end_page":         100,
                "enqueued_at":      "<iso-datetime-utc>"
            }

        The Azure SDK base64-encodes the message body automatically.
        """
        message = json.dumps({
            "job_id": job_id,
            "blob_name_upload": blob_name_upload,
            "start_page": start_page,
            "end_page": end_page,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        })
        self._queue.send_message(message)
        logger.info(
            "Enqueued CoStar job: job_id=%s blob=%s pages=%s-%s",
            job_id, blob_name_upload, start_page, end_page,
        )