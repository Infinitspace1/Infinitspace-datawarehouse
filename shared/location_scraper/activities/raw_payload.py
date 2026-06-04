"""
Persist full Apify dataset rows to bronze.location_scraper_raw (JSON payload).

Called once per run after ls_fetch_dataset, before normalization.
Idempotent per run_id: deletes existing rows for the same run_id on retry.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Iterator

from shared.azure_clients.sql_client import get_sql_client
from shared.location_scraper.clients import apify as apify_client

logger = logging.getLogger(__name__)

_BATCH_SIZE = 200

_INSERT_RAW = """
INSERT INTO bronze.location_scraper_raw (run_id, source, city, item_index, payload_json)
VALUES (?, ?, ?, ?, ?)
"""

_DELETE_RUN = "DELETE FROM bronze.location_scraper_raw WHERE run_id = ?"

_SELECT_RAW_PAGE = (
    "SELECT payload_json FROM bronze.location_scraper_raw "
    "WHERE run_id = ? ORDER BY item_index "
    "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
)


def _dump_item(item: Any) -> str:
    try:
        return json.dumps(item, ensure_ascii=False, default=str)
    except TypeError:
        return json.dumps({"_error": "non_serializable"}, ensure_ascii=False)


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


def fetch_and_persist_raw(payload: dict[str, Any]) -> dict[str, Any]:
    """Stream an Apify dataset straight into ``bronze.location_scraper_raw``.

    This replaces the old ``fetch_dataset`` -> orchestrator -> ``persist_raw``
    path. The full dataset is NOT returned to (and never transits) the Durable
    orchestrator — that path serialised the whole payload into the orchestration
    history and re-materialised it on every replay, which is what OOM-killed the
    worker on large cities. Here items are streamed page-by-page from Apify and
    written to SQL in batches, so peak memory stays at ~one batch.

    Idempotent per ``run_id``: existing rows for the run are deleted first, so a
    retry (or a re-run with the same monthly run_id) re-streams cleanly.

    payload = {"run_id": str, "dataset_id": str, "source": str, "city": str}
    Returns {"item_count": int, "rows_inserted": int}.
    """
    run_id = payload["run_id"]
    dataset_id = payload["dataset_id"]
    source = payload["source"]
    city = payload["city"].lower().strip()

    sql = get_sql_client()
    try:
        sql.execute_non_query(_DELETE_RUN, (run_id,))
    except Exception:
        logger.exception(
            "Could not delete prior raw rows (table missing?). run_id=%s", run_id
        )
        raise

    batch: list[tuple] = []
    count = 0
    inserted = 0
    for item in apify_client.iterate_dataset(dataset_id):
        batch.append((run_id, source, city, count, _dump_item(item)))
        count += 1
        if len(batch) >= _BATCH_SIZE:
            inserted += sql.execute_many(_INSERT_RAW, batch)
            batch = []
    if batch:
        inserted += sql.execute_many(_INSERT_RAW, batch)

    logger.info(
        "location_scraper raw streamed run_id=%s source=%s city=%s items=%d inserted=%d",
        run_id,
        source,
        city,
        count,
        inserted,
    )
    return {"item_count": count, "rows_inserted": inserted}


def read_raw_items(run_id: str, batch_size: int = _BATCH_SIZE) -> Iterator[dict]:
    """Yield raw payload dicts for a run from SQL, paged to keep memory low.

    Used by ``normalize_listings`` so the dataset is read back from
    ``bronze.location_scraper_raw`` instead of being passed through the Durable
    orchestrator. Pages via OFFSET/FETCH so only ``batch_size`` rows are held at
    a time."""
    sql = get_sql_client()
    offset = 0
    while True:
        rows = sql.execute_query(_SELECT_RAW_PAGE, (run_id, offset, batch_size))
        if not rows:
            break
        for row in rows:
            raw = row.get("payload_json")
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except (ValueError, TypeError):
                logger.warning("Skipping unparseable raw payload row run_id=%s", run_id)
        if len(rows) < batch_size:
            break
        offset += batch_size
