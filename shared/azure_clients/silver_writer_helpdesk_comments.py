"""
shared/azure_clients/silver_writer_helpdesk_comments.py

Reads bronze.nexudus_helpdesk_comments, transforms, and MERGEs into
silver.nexudus_helpdesk_comments.

HelpDeskComment payloads carry no BusinessId, so location_source_id is
resolved here from the parent help desk message's bronze row — the same
approach silver_writer_event_products uses for its parent calendar event.
Bronze always completes before silver runs (both in the 15-minute poll and
in the webhook path, which writes bronze before enqueuing), so the map is
complete by the time this runs.
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.exclusions import is_excluded_location_source_id
from shared.nexudus.transformers.helpdesk_comments import transform_helpdesk_comment

logger = logging.getLogger(__name__)

_MESSAGE_LOCATION_MAP_SQL = """
    SELECT source_id, location_id
    FROM (
        SELECT source_id, location_id,
               ROW_NUMBER() OVER (
                   PARTITION BY source_id
                   ORDER BY synced_at DESC, id DESC
               ) AS rn
        FROM bronze.nexudus_helpdesk_messages
    ) sub
    WHERE rn = 1
"""

_MERGE_SQL = """
    MERGE silver.nexudus_helpdesk_comments AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        helpdesk_message_source_id = ?,
        location_source_id = ?,
        coworker_source_id = ?, coworker_full_name = ?,
        message_text = ?, is_internal = ?,
        image_file_name = ?,
        updated_by = ?,
        created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        helpdesk_message_source_id,
        location_source_id,
        coworker_source_id, coworker_full_name,
        message_text, is_internal,
        image_file_name,
        updated_by,
        created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?,
        ?,
        ?, ?,
        ?, ?,
        ?,
        ?,
        ?, ?
    );
"""


class SilverHelpdeskCommentsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze help desk comment records")

        message_locations = self._load_message_location_map()

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                message_id = raw.get("HelpDeskMessageId")
                location_id = (
                    message_locations.get(int(message_id)) if message_id else None
                )
                # A comment whose parent ticket belongs to an excluded location
                # (the global/demo businesses, or closed Kingsbourne House) is
                # dropped, so comments and messages stay consistent. An
                # unresolved parent yields location_id None, which is NOT
                # excluded — the comment is still worth keeping.
                if is_excluded_location_source_id(location_id):
                    continue
                cm = transform_helpdesk_comment(
                    raw, row["id"], self.sync_run_id,
                    location_source_id=location_id,
                )
                params_list.append(self._make_params(cm))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver help desk comments: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "helpdesk_comments": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_helpdesk_comments",
            source_name="nexudus",
            entity="helpdesk_comments",
        )

    def _load_message_location_map(self) -> dict[int, int]:
        """{helpdesk_message_source_id -> location_source_id} from latest bronze messages."""
        rows = self.sql.execute_query(_MESSAGE_LOCATION_MAP_SQL)
        return {
            int(r["source_id"]): int(r["location_id"])
            for r in rows
            if r.get("source_id") is not None and r.get("location_id") is not None
        }

    def _make_params(self, c: dict) -> tuple:
        vals = (
            c["unique_id"],                 c["bronze_id"],         c["sync_run_id"],
            c["helpdesk_message_source_id"],
            c["location_source_id"],
            c["coworker_source_id"],        c["coworker_full_name"],
            c["message_text"],              c["is_internal"],
            c["image_file_name"],
            c["updated_by"],
            c["created_on"],                c["updated_on"],
        )
        return (c["source_id"], *vals, c["source_id"], *vals)
