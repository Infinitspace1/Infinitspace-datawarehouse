"""
shared/azure_clients/silver_writer_competence.py

Reads the latest bronze competence rows, transforms them, and MERGEs into
silver.competence_lists and silver.competence_competitors.

Both tables share the "competence" silver watermark (they are written together
in one competence_sync run), so load_latest_bronze_rows only re-transforms rows
that changed since the last successful silver run. A re-seen record clears
is_deleted/deleted_at on MATCH (restore-on-reappearance); rows that vanish from
Firestore are soft-deleted by the reconcile step in competence_sync.py.
"""
from __future__ import annotations

import json
import logging
import uuid

from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.azure_clients.sql_client import get_sql_client
from shared.firebase.transformers.competence import (
    transform_competence_list,
    transform_competitor,
)

logger = logging.getLogger(__name__)

# Shared silver entity/watermark name for both competence tables.
SILVER_ENTITY = "competence"

_LISTS_MERGE_SQL = """
    MERGE silver.competence_lists AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        uid = ?, competitor_list_name = ?, country = ?, country_code = ?,
        auto_managed = ?, status = ?, competitor_count = ?, schema_version = ?,
        last_error = ?, created_at = ?, updated_at = ?, last_run_at = ?,
        bronze_id = ?, sync_run_id = ?,
        is_deleted = 0, deleted_at = NULL,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, uid, competitor_list_name, country, country_code,
        auto_managed, status, competitor_count, schema_version,
        last_error, created_at, updated_at, last_run_at,
        bronze_id, sync_run_id
    ) VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?
    );
"""

_COMPETITORS_MERGE_SQL = """
    MERGE silver.competence_competitors AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        list_source_id = ?, place_id = ?, title = ?, category_name = ?,
        address = ?, street = ?, city = ?, postal_code = ?, country_code = ?,
        phone = ?, website = ?, google_maps_url = ?,
        latitude = ?, longitude = ?,
        last_seen_at = ?, last_seen_in_city = ?,
        created_at = ?, updated_at = ?,
        bronze_id = ?, sync_run_id = ?,
        is_deleted = 0, deleted_at = NULL,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, list_source_id, place_id, title, category_name,
        address, street, city, postal_code, country_code,
        phone, website, google_maps_url,
        latitude, longitude,
        last_seen_at, last_seen_in_city,
        created_at, updated_at,
        bronze_id, sync_run_id
    ) VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?
    );
"""


class SilverCompetenceWriter:
    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        lists_read, lists_written, lists_err = self._sync_lists()
        comps_read, comps_written, comps_err = self._sync_competitors()
        return {
            "rows_read": lists_read + comps_read,
            "lists_read": lists_read,
            "lists_written": lists_written,
            "competitors_read": comps_read,
            "competitors_written": comps_written,
            "errors": lists_err + comps_err,
        }

    # ── Lists ────────────────────────────────────────────────

    def _sync_lists(self) -> tuple[int, int, int]:
        rows = load_latest_bronze_rows(
            "bronze.competence_lists",
            source_name="competence",
            entity=SILVER_ENTITY,
            columns="b.id, b.source_id, b.raw_json",
        )
        params_list, errors = [], 0
        for row in rows:
            try:
                raw = json.loads(row["raw_json"])
                rec = transform_competence_list(
                    raw, row["source_id"], row["id"], self.sync_run_id
                )
                params_list.append(self._list_params(rec))
            except Exception as exc:
                logger.warning(
                    "competence_lists silver failed source_id=%s: %s",
                    row.get("source_id"), exc,
                )
                errors += 1
        if params_list:
            self.sql.execute_many(_LISTS_MERGE_SQL, params_list)
        logger.info("Silver competence_lists: %s upserted, %s errors", len(params_list), errors)
        return len(rows), len(params_list), errors

    # ── Competitors ──────────────────────────────────────────

    def _sync_competitors(self) -> tuple[int, int, int]:
        rows = load_latest_bronze_rows(
            "bronze.competence_competitors",
            source_name="competence",
            entity=SILVER_ENTITY,
            columns="b.id, b.source_id, b.list_source_id, b.raw_json",
        )
        params_list, errors = [], 0
        for row in rows:
            try:
                raw = json.loads(row["raw_json"])
                rec = transform_competitor(
                    raw, row["source_id"], row["list_source_id"], row["id"], self.sync_run_id
                )
                params_list.append(self._competitor_params(rec))
            except Exception as exc:
                logger.warning(
                    "competence_competitors silver failed source_id=%s: %s",
                    row.get("source_id"), exc,
                )
                errors += 1
        if params_list:
            self.sql.execute_many(_COMPETITORS_MERGE_SQL, params_list)
        logger.info(
            "Silver competence_competitors: %s upserted, %s errors", len(params_list), errors
        )
        return len(rows), len(params_list), errors

    # ── Param builders (USING + UPDATE + INSERT order) ────────

    def _list_params(self, r: dict) -> tuple:
        vals = (
            r["uid"], r["competitor_list_name"], r["country"], r["country_code"],
            r["auto_managed"], r["status"], r["competitor_count"], r["schema_version"],
            r["last_error"], r["created_at"], r["updated_at"], r["last_run_at"],
            r["bronze_id"], r["sync_run_id"],
        )
        return (r["source_id"], *vals, r["source_id"], *vals)

    def _competitor_params(self, r: dict) -> tuple:
        vals = (
            r["list_source_id"], r["place_id"], r["title"], r["category_name"],
            r["address"], r["street"], r["city"], r["postal_code"], r["country_code"],
            r["phone"], r["website"], r["google_maps_url"],
            r["latitude"], r["longitude"],
            r["last_seen_at"], r["last_seen_in_city"],
            r["created_at"], r["updated_at"],
            r["bronze_id"], r["sync_run_id"],
        )
        return (r["source_id"], *vals, r["source_id"], *vals)
