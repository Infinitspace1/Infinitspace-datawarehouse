"""
Persist full Apify dataset rows to bronze.location_scraper_raw (JSON payload).

Called once per run after ls_fetch_dataset, before normalization.
Idempotent per run_id: deletes existing rows for the same run_id on retry.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

_BATCH_SIZE = 200

_INSERT_RAW = """
INSERT INTO bronze.location_scraper_raw (run_id, source, city, item_index, payload_json)
VALUES (?, ?, ?, ?, ?)
"""

_DELETE_RUN = "DELETE FROM bronze.location_scraper_raw WHERE run_id = ?"


def persist_raw_items(payload: dict[str, Any]) -> dict[str, Any]:
    """
    payload = {
        "run_id": str,
        "source": str,   # idealista | otodom | immobilienscout
        "city": str,
        "items": list[dict],  # raw Apify dataset items
    }
    Returns {"rows_inserted": int}.
    """
    run_id = payload["run_id"]
    source = payload["source"]
    city = payload["city"].lower().strip()
    items: list[dict] = payload.get("items") or []

    sql = get_sql_client()
    try:
        sql.execute_non_query(_DELETE_RUN, (run_id,))
    except Exception:
        logger.exception(
            "Could not delete prior raw rows (table missing?). run_id=%s", run_id
        )
        raise

    if not items:
        return {"rows_inserted": 0}

    params_list: list[tuple] = []
    for idx, item in enumerate(items):
        try:
            payload_json = json.dumps(item, ensure_ascii=False, default=str)
        except TypeError:
            payload_json = json.dumps({"_error": "non_serializable"}, ensure_ascii=False)
        params_list.append((run_id, source, city, idx, payload_json))

    inserted = 0
    for i in range(0, len(params_list), _BATCH_SIZE):
        chunk = params_list[i : i + _BATCH_SIZE]
        inserted += sql.execute_many(_INSERT_RAW, chunk)

    logger.info(
        "location_scraper raw rows inserted run_id=%s source=%s city=%s count=%d",
        run_id,
        source,
        city,
        inserted,
    )
    return {"rows_inserted": inserted}
