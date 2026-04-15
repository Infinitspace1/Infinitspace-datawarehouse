"""
shared/azure_clients/blob_writer.py

Writes raw Nexudus snapshots and Xero invoice PDFs to Azure Blob Storage.
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
    Stores raw API snapshots and PDFs in Blob Storage.

    Nexudus snapshot blob path:
        nexudus/{entity}/{yyyy}/{mm}/{dd}/{run_id}.json

    Xero PDF blob path:
        {xero_tenant_id}/{yyyy}/{mm}/{invoice_source_id}.pdf
    """

    def __init__(self):
        self.account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "staccinfinitspaceprod001")
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_RAW_NEXUDUS", "nexudus-raw-snapshots")
        self.pdf_container_name = os.getenv("AZURE_STORAGE_CONTAINER_XERO_PDFS", "xero-invoice-pdfs")
        self.nexudus_pdf_container_name = os.getenv("AZURE_STORAGE_CONTAINER_NEXUDUS_PDFS", "nexudus-invoice-pdfs")
        if not self.account_name:
            raise EnvironmentError("AZURE_STORAGE_ACCOUNT_NAME is required")

        account_url = f"https://{self.account_name}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        self._service = BlobServiceClient(account_url=account_url, credential=credential)
        self._container = self._service.get_container_client(self.container_name)
        self._pdf_container = self._service.get_container_client(self.pdf_container_name)
        self._nexudus_pdf_container = self._service.get_container_client(self.nexudus_pdf_container_name)

        for container in (self._container, self._pdf_container, self._nexudus_pdf_container):
            try:
                container.create_container()
            except ResourceExistsError:
                pass

    def write_pdf(
        self,
        tenant_id: str,
        invoice_source_id: str,
        pdf_bytes: bytes,
        content_type: str = "application/pdf",
    ) -> str:
        """
        Upload a PDF to the xero-invoice-pdfs container.

        Blob path: {tenant_id}/{yyyy}/{mm}/{invoice_source_id}.pdf
        Returns the blob path (stored in SQL as the reference).
        """
        now = datetime.now(timezone.utc)
        blob_name = f"{tenant_id}/{now:%Y}/{now:%m}/{invoice_source_id}.pdf"
        content_settings = ContentSettings(content_type=content_type)
        self._pdf_container.upload_blob(
            name=blob_name,
            data=pdf_bytes,
            overwrite=True,
            content_settings=content_settings,
        )
        return blob_name

    def write_nexudus_pdf(
        self,
        location_source_id: int,
        invoice_source_id: int,
        pdf_bytes: bytes,
        content_type: str = "application/pdf",
    ) -> str:
        """
        Upload a PDF to the nexudus-invoice-pdfs container.

        Blob path: {location_source_id}/{yyyy}/{mm}/{invoice_source_id}.pdf
        Returns the blob path (stored in SQL as the reference).
        """
        now = datetime.now(timezone.utc)
        blob_name = f"{location_source_id}/{now:%Y}/{now:%m}/{invoice_source_id}.pdf"
        content_settings = ContentSettings(content_type=content_type)
        self._nexudus_pdf_container.upload_blob(
            name=blob_name,
            data=pdf_bytes,
            overwrite=True,
            content_settings=content_settings,
        )
        return blob_name

    def read_pdf(self, blob_path: str) -> bytes:
        """Download a PDF by its stored blob path."""
        blob_client = self._pdf_container.get_blob_client(blob_path)
        return blob_client.download_blob().readall()

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
