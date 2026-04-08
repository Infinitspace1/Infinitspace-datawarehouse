"""
functions/real_estate_costar.py

Blueprint: HTTP POST handler that accepts a CoStar PDF extraction job,
validates it, enqueues it, and returns 202 immediately.

The actual extraction is handled by real_estate_costar_worker.py,
which is triggered by the costar-extraction-tasks queue.
This split avoids the 5-minute Consumption plan timeout for long PDFs.

Registered only when ENABLE_REAL_ESTATE_FUNCTIONS=1.

HTTP: POST /api/real-estate/costar/run?code=<function_key>
  Body JSON:
    {
      "job_id":           "<uuid>",
      "blob_name_upload": "<filename.pdf>",
      "start_page":       1,
      "end_page":         100
    }

Response 202:
    {"ok": true, "job_id": "<uuid>", "status": "queued"}

Auth: FUNCTION key (?code= or x-functions-key header).

Status polling:
  The Real Estate app polls bronze.costar_pdf_extractor_logs by job_id.
  The worker updates that record throughout processing.
"""
from __future__ import annotations

import json
import logging

import azure.functions as func

from shared.azure_clients.costar_queue_client import CostarTaskQueue

logger = logging.getLogger(__name__)

bp = func.Blueprint()


def _json(payload: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload),
        status_code=status_code,
        mimetype="application/json",
    )


@bp.route(
    route="real-estate/costar/run",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
def run_costar_extractor(req: func.HttpRequest) -> func.HttpResponse:
    """Accept a CoStar extraction job and enqueue it for async processing."""
    try:
        body = req.get_json()
    except ValueError:
        return _json({"error": "Invalid JSON body"}, 400)

    if not isinstance(body, dict):
        return _json({"error": "Body must be a JSON object"}, 400)

    job_id = body.get("job_id")
    blob_name_upload = body.get("blob_name_upload")

    # Validate required string fields before attempting int conversion
    if not job_id or not blob_name_upload:
        return _json({"error": "job_id and blob_name_upload are required"}, 400)

    try:
        start_page = int(body.get("start_page"))
        end_page = int(body.get("end_page"))
    except (TypeError, ValueError):
        return _json({"error": "start_page and end_page must be integers"}, 400)

    if start_page < 1 or end_page < start_page:
        return _json({"error": "Invalid page range: start_page must be >= 1 and <= end_page"}, 400)

    logger.info(
        "CoStar job received: job_id=%s blob=%s pages=%s-%s — enqueuing",
        job_id, blob_name_upload, start_page, end_page,
    )

    try:
        queue = CostarTaskQueue()
        queue.enqueue_job(
            job_id=job_id,
            blob_name_upload=blob_name_upload,
            start_page=start_page,
            end_page=end_page,
        )
    except Exception as e:
        logger.exception("Failed to enqueue CoStar job %s: %s", job_id, e)
        return _json({"ok": False, "job_id": job_id, "error": str(e)}, 500)

    return _json({"ok": True, "job_id": job_id, "status": "queued"}, 202)