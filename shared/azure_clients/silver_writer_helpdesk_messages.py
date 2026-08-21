"""
shared/azure_clients/silver_writer_helpdesk_messages.py

Reads bronze.nexudus_helpdesk_messages, transforms, and MERGEs into
silver.nexudus_helpdesk_messages.
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.exclusions import is_excluded_location_source_id
from shared.nexudus.transformers.helpdesk_messages import transform_helpdesk_message

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.nexudus_helpdesk_messages AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        location_source_id = ?,
        coworker_source_id = ?, coworker_full_name = ?,
        department_source_id = ?, department_name = ?,
        subject = ?, message_text = ?,
        priority = ?,
        is_closed = ?, closed_on = ?,
        owner_source_id = ?, owner_full_name = ?,
        first_response_minutes = ?, minutes_to_close = ?,
        ai_processing_result = ?, ai_channel_session_id = ?, support_issue_category = ?,
        image_file_name = ?,
        updated_by = ?,
        created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        location_source_id,
        coworker_source_id, coworker_full_name,
        department_source_id, department_name,
        subject, message_text,
        priority,
        is_closed, closed_on,
        owner_source_id, owner_full_name,
        first_response_minutes, minutes_to_close,
        ai_processing_result, ai_channel_session_id, support_issue_category,
        image_file_name,
        updated_by,
        created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?,
        ?,
        ?,
        ?, ?
    );
"""


class SilverHelpdeskMessagesWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze help desk message records")

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                msg = transform_helpdesk_message(raw, row["id"], self.sync_run_id)
                if is_excluded_location_source_id(msg["location_source_id"]):
                    continue
                params_list.append(self._make_params(msg))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver help desk messages: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "helpdesk_messages": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_helpdesk_messages",
            source_name="nexudus",
            entity="helpdesk_messages",
        )

    def _make_params(self, m: dict) -> tuple:
        vals = (
            m["unique_id"],              m["bronze_id"],             m["sync_run_id"],
            m["location_source_id"],
            m["coworker_source_id"],     m["coworker_full_name"],
            m["department_source_id"],   m["department_name"],
            m["subject"],                m["message_text"],
            m["priority"],
            m["is_closed"],              m["closed_on"],
            m["owner_source_id"],        m["owner_full_name"],
            m["first_response_minutes"], m["minutes_to_close"],
            m["ai_processing_result"],   m["ai_channel_session_id"], m["support_issue_category"],
            m["image_file_name"],
            m["updated_by"],
            m["created_on"],             m["updated_on"],
        )
        return (m["source_id"], *vals, m["source_id"], *vals)
