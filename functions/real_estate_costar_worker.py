"""
functions/real_estate_costar_worker.py

Blueprint: Queue-triggered worker that processes one CoStar PDF extraction job.
Triggered by messages on the "costar-extraction-tasks" Azure Storage Queue,
enqueued by the real_estate_costar HTTP handler.

Message format (JSON):
    {
        "job_id":           "<uuid>",
        "blob_name_upload": "<filename.pdf>",
        "start_page":       1,
        "end_page":         100,
        "enqueued_at":      "<iso-datetime-utc>"
    }

Design properties:
  - Raising on failure causes Azure to re-queue the message automatically.
  - After maxDequeueCount retries (default 5) the message moves to the poison
    queue (costar-extraction-tasks-poison) for manual inspection.
  - dequeue_count is logged so repeated failures are visible in App Insights.
  - All SQL updates go to the Real Estate DB (AZURE_SQL_PDF_JOBS_CONNECTION_STRING),
    not the datawarehouse DB.

Environment variables:
  ANTHROPIC_API_KEY
  AZURE_STORAGE_ACCOUNT_NAME             -- shared storage account (staccinfinitspaceprod001)
  AZURE_SQL_PDF_JOBS_CONNECTION_STRING   -- Real Estate SQL DB
  AZURE_SQL_PDF_JOBS_TABLE               -- default: bronze.costar_pdf_extractor_logs
  BLOB_CONTAINER_NAME                    -- default: real-estate-costar-automation
  BLOB_FOLDER_UPLOADS                    -- default: pdf-uploads
  BLOB_FOLDER_OUTPUTS                    -- default: excel-outputs

Connection binding:
  Uses the "Storage" connection name, resolved to AzureWebJobsStorage by the
  Functions host — same as silver_entity_worker.

Extractor dependency:
  shared/real_estate/building_contact_extractor.py
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Optional

import azure.functions as func
import pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

bp = func.Blueprint()

QUEUE_NAME = "costar-extraction-tasks"

# ---------------------------------------------------------------------------
# Column allowlist — guards against SQL injection via dynamic SET clauses
# ---------------------------------------------------------------------------
_PDF_JOB_UPDATE_COLUMNS = frozenset({
    "filename", "start_page", "end_page", "status", "progress", "current_page",
    "created_at", "started_at", "completed_at", "blob_name_upload", "blob_name_output",
    "error", "hubspot_status", "hubspot_progress", "hubspot_started_at",
    "hubspot_completed_at", "hubspot_error", "hubspot_stats", "blob_name_cleaned",
})


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is not None and str(v).strip() != "":
        return v.strip()
    return default


# ---------------------------------------------------------------------------
# SQL helpers  (Real Estate DB — separate from the datawarehouse DB)
# ---------------------------------------------------------------------------

def _connect_sql() -> pyodbc.Connection:
    conn_str = _env("AZURE_SQL_PDF_JOBS_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("AZURE_SQL_PDF_JOBS_CONNECTION_STRING is not set")
    if "Driver=" not in conn_str and "DRIVER=" not in conn_str:
        for driver_name in (
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
        ):
            if driver_name in pyodbc.drivers():
                conn_str = f"Driver={{{driver_name}}};{conn_str}"
                break
    return pyodbc.connect(conn_str, timeout=30)


def _pdf_table() -> str:
    t = _env("AZURE_SQL_PDF_JOBS_TABLE", "bronze.costar_pdf_extractor_logs")
    if not t or "." not in t:
        raise ValueError("AZURE_SQL_PDF_JOBS_TABLE must be schema.table")
    return t


def _update_job(job_id: str, conn: pyodbc.Connection, **kwargs: Any) -> None:
    """Update job row using a shared connection."""
    bad = set(kwargs) - _PDF_JOB_UPDATE_COLUMNS
    if bad:
        raise ValueError(f"Invalid columns: {bad}")
    keys = list(kwargs.keys())
    if not keys:
        return
    table = _pdf_table()
    set_clause = ", ".join(f"{k} = ?" for k in keys)
    values = [kwargs[k] for k in keys]
    values.append(job_id)
    cur = conn.cursor()
    cur.execute(f"UPDATE {table} SET {set_clause} WHERE id = ?", values)
    conn.commit()


# ---------------------------------------------------------------------------
# Blob helpers
# ---------------------------------------------------------------------------

def _blob_service() -> BlobServiceClient:
    account_name = _env("AZURE_STORAGE_ACCOUNT_NAME", "staccinfinitspaceprod001")
    if not account_name:
        raise RuntimeError("AZURE_STORAGE_ACCOUNT_NAME is not set")
    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())


def _container_name() -> str:
    return _env("BLOB_CONTAINER_NAME", "real-estate-costar-automation")


def _download_blob(blob_svc: BlobServiceClient, blob_name: str, local_path: str) -> None:
    container = blob_svc.get_container_client(_container_name())
    folder = _env("BLOB_FOLDER_UPLOADS", "pdf-uploads")
    bc = container.get_blob_client(f"{folder}/{blob_name}")
    with open(local_path, "wb") as f:
        f.write(bc.download_blob().readall())


def _upload_blob(blob_svc: BlobServiceClient, local_path: str, blob_name: str) -> None:
    container = blob_svc.get_container_client(_container_name())
    folder = _env("BLOB_FOLDER_OUTPUTS", "excel-outputs")
    bc = container.get_blob_client(f"{folder}/{blob_name}")
    with open(local_path, "rb") as f:
        bc.upload_blob(f, overwrite=True)


# ---------------------------------------------------------------------------
# Write extraction results to bronze tables
# ---------------------------------------------------------------------------

def _write_extraction_results(conn: pyodbc.Connection, job_id: str, extractor: Any) -> None:
    """Insert buildings and contacts from the extractor into the Real Estate DB.

    Tables: bronze.costar_building, bronze.costar_building_contacts
    """
    cur = conn.cursor()

    # Map building_name -> generated DB id
    building_id_map: dict[str, int] = {}

    # Insert buildings
    for b in extractor.buildings:
        cur.execute(
            """
            INSERT INTO bronze.costar_building
                (job_id, building_name, building_address, city, state, zip_code,
                 county, submarket, property_type, star_rating,
                 latitude, longitude, geocoded_at, source_page)
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            job_id,
            b.get("building_name"),
            b.get("building_address"),
            b.get("city"),
            b.get("state"),
            b.get("zip_code"),
            b.get("county"),
            b.get("submarket"),
            b.get("property_type"),
            b.get("star_rating"),
            b.get("latitude"),
            b.get("longitude"),
            b.get("geocoded_at"),
            b.get("source_page"),
        )
        row = cur.fetchone()
        if row:
            building_id_map[b["building_name"]] = row[0]

    conn.commit()
    logger.info("CoStar job %s: inserted %s buildings", job_id, len(building_id_map))

    # Insert contacts
    contacts_inserted = 0
    for r in extractor.all_rows:
        building_name = r.get("Building Name")
        building_id = building_id_map.get(building_name)
        if building_id is None:
            logger.warning("No building_id for contact row: %s", building_name)
            continue

        # Skip rows with no contact info (empty placeholder rows)
        if not r.get("Contact Type") and not r.get("Contact Name") and not r.get("Contact Company"):
            continue

        cur.execute(
            """
            INSERT INTO bronze.costar_building_contacts
                (job_id, building_id, contact_type, contact_name, contact_company,
                 contact_job_title, contact_email, contact_phone,
                 company_address, company_city, company_state, company_zip,
                 company_country, company_phone, company_website, source_page)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            job_id,
            building_id,
            r.get("Contact Type"),
            r.get("Contact Name"),
            r.get("Contact Company"),
            r.get("Contact Job Title"),
            r.get("Contact Email"),
            r.get("Contact Phone"),
            r.get("Company Address"),
            r.get("Company City"),
            r.get("Company State"),
            r.get("Company Zip"),
            r.get("Company Country"),
            r.get("Company Phone"),
            r.get("Company Website"),
            r.get("Page"),
        )
        contacts_inserted += 1

    conn.commit()
    logger.info("CoStar job %s: inserted %s contacts", job_id, contacts_inserted)


# ---------------------------------------------------------------------------
# Core extraction logic
# ---------------------------------------------------------------------------

def _run_extraction_job(
    job_id: str,
    blob_name_upload: str,
    start_page: int,
    end_page: int,
) -> None:
    """Download PDF, run extractor, upload results, update job status throughout."""
    from shared.real_estate.building_contact_extractor import BuildingContactExtractor

    temp_pdf = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    temp_pdf.close()
    temp_xlsx = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    temp_xlsx.close()
    temp_pdf_path = temp_pdf.name
    temp_output_path = temp_xlsx.name

    blob_svc = _blob_service()
    conn = _connect_sql()

    try:
        _download_blob(blob_svc, blob_name_upload, temp_pdf_path)

        _update_job(
            job_id, conn,
            status="processing",
            progress=0,
            started_at=datetime.now(timezone.utc).isoformat(),
        )

        extractor = BuildingContactExtractor(
            pdf_path=temp_pdf_path,
            start_page=start_page,
            end_page=end_page,
            output_file=temp_output_path,
        )
        original_extract = extractor.extract_from_page

        def extract_with_progress(image, page_num):
            result = original_extract(image, page_num)
            total_pages = end_page - start_page + 1
            current = page_num - start_page + 1
            progress = int((current / total_pages) * 100)
            _update_job(job_id, conn, progress=progress, current_page=page_num)
            logger.info("CoStar job %s: page %s/%s (%s%%)", job_id, page_num, end_page, progress)
            return result

        extractor.extract_from_page = extract_with_progress
        extractor.process_pdf()

        # Write extracted buildings & contacts to DB
        _write_extraction_results(conn, job_id, extractor)

        blob_name_output = f"{job_id}_contacts.xlsx"
        _upload_blob(blob_svc, temp_output_path, blob_name_output)

        _update_job(
            job_id, conn,
            status="completed",
            blob_name_output=blob_name_output,
            completed_at=datetime.now(timezone.utc).isoformat(),
            progress=100,
        )
        logger.info("CoStar job %s completed", job_id)

    except Exception as e:
        logger.exception("CoStar job %s failed: %s", job_id, e)
        _update_job(job_id, conn, status="failed", error=str(e))
        raise

    finally:
        conn.close()
        for p in (temp_pdf_path, temp_output_path):
            try:
                if p and os.path.exists(p):
                    os.unlink(p)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Queue-triggered handler
# ---------------------------------------------------------------------------

@bp.queue_trigger(
    arg_name="msg",
    queue_name=QUEUE_NAME,
    connection="Storage",
)
def costar_extraction_worker(msg: func.QueueMessage) -> None:
    """
    Process one CoStar PDF extraction job from the queue.

    Raises on failure so Azure re-queues the message for retry.
    After maxDequeueCount retries the message is moved to the poison queue
    (costar-extraction-tasks-poison) for manual inspection.
    """
    raw_body = msg.get_body().decode("utf-8")
    payload = json.loads(raw_body)

    job_id: str = payload.get("job_id", "")
    blob_name_upload: str = payload.get("blob_name_upload", "")
    start_page: int = int(payload.get("start_page", 0))
    end_page: int = int(payload.get("end_page", 0))
    enqueued_at: str = payload.get("enqueued_at", "unknown")
    dequeue_count: int = msg.dequeue_count

    logger.info(
        "CoStar worker received: job_id=%s blob=%s pages=%s-%s "
        "enqueued_at=%s dequeue_count=%s",
        job_id, blob_name_upload, start_page, end_page,
        enqueued_at, dequeue_count,
    )

    if not job_id or not blob_name_upload or start_page < 1 or end_page < start_page:
        logger.error(
            "Malformed CoStar queue message (job_id=%s) — will be dead-lettered after max retries",
            job_id,
        )
        raise ValueError(f"Malformed CoStar queue message: {payload!r}")

    _run_extraction_job(job_id, blob_name_upload, start_page, end_page)
