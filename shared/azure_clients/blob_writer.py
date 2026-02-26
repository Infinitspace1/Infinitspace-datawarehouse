"""
shared/azure_clients/blob_writer.py

Writes raw Nexudus snapshots to Azure Blob Storage using
date-partitioned paths for cheap, durable historical retention.
"""
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings


class BlobWriter:
    """
    Stores raw API snapshots in Blob Storage.

    Blob path format:
        nexudus/{entity}/{yyyy}/{mm}/{dd}/{run_id}.json
    """

    def __init__(self):
        self.account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "staccinfinitspaceprod001")
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_RAW_NEXUDUS", "nexudus-raw-snapshots")
        if not self.account_name:
            raise EnvironmentError("AZURE_STORAGE_ACCOUNT_NAME is required")

        account_url = f"https://{self.account_name}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        self._service = BlobServiceClient(account_url=account_url, credential=credential)
        self._container = self._service.get_container_client(self.container_name)

        try:
            self._container.create_container()
        except ResourceExistsError:
            pass

    def write_snapshot(self, entity: str, records: list[dict[str, Any]], run_id: uuid.UUID | str) -> str:
        now = datetime.now(timezone.utc)
        run_id_str = str(run_id)
        blob_name = (
            f"nexudus/{entity}/{now:%Y}/{now:%m}/{now:%d}/{run_id_str}.json"
        )

        payload = {
            "source": "nexudus",
            "entity": entity,
            "run_id": run_id_str,
            "snapshot_at_utc": now.isoformat(),
            "row_count": len(records),
            "records": records,
        }
        body = json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8")

        metadata = {
            "source": "nexudus",
            "entity": entity,
            "run_id": run_id_str,
            "row_count": str(len(records)),
            "snapshot_date": now.strftime("%Y-%m-%d"),
        }
        content_settings = ContentSettings(content_type="application/json; charset=utf-8")

        self._container.upload_blob(
            name=blob_name,
            data=body,
            overwrite=True,
            metadata=metadata,
            content_settings=content_settings,
        )
        return blob_name
