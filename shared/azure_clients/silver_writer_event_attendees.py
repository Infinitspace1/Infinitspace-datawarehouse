"""
shared/azure_clients/silver_writer_event_attendees.py

Reads bronze.nexudus_event_attendees, transforms, and MERGEs into
silver.nexudus_event_attendees (single table, all columns).
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.transformers.event_attendees import transform_event_attendee

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.nexudus_event_attendees AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        calendar_event_source_id = ?, calendar_event_name = ?,
        location_source_id = ?,
        coworker_source_id = ?, coworker_full_name = ?,
        full_name = ?, email = ?, attendee_code = ?,
        checked_in = ?, checked_in_date = ?,
        event_product_source_id = ?, event_product_name = ?,
        event_product_price = ?, event_product_currency_code = ?,
        invoiced = ?, coworker_invoice_source_id = ?,
        coworker_invoice_number = ?, coworker_invoice_paid = ?,
        due_date = ?, purchase_order = ?,
        updated_by = ?,
        created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        calendar_event_source_id, calendar_event_name,
        location_source_id,
        coworker_source_id, coworker_full_name,
        full_name, email, attendee_code,
        checked_in, checked_in_date,
        event_product_source_id, event_product_name,
        event_product_price, event_product_currency_code,
        invoiced, coworker_invoice_source_id,
        coworker_invoice_number, coworker_invoice_paid,
        due_date, purchase_order,
        updated_by,
        created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?, ?,
        ?,
        ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?,
        ?, ?
    );
"""


class SilverEventAttendeesWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze event attendee records")

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                at = transform_event_attendee(raw, row["id"], self.sync_run_id)
                params_list.append(self._make_params(at))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver event attendees: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "event_attendees": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_event_attendees",
            source_name="nexudus",
            entity="event_attendees",
        )

    def _make_params(self, at: dict) -> tuple:
        vals = (
            at["unique_id"],                at["bronze_id"],            at["sync_run_id"],
            at["calendar_event_source_id"], at["calendar_event_name"],
            at["location_source_id"],
            at["coworker_source_id"],       at["coworker_full_name"],
            at["full_name"],                at["email"],                at["attendee_code"],
            at["checked_in"],               at["checked_in_date"],
            at["event_product_source_id"],  at["event_product_name"],
            at["event_product_price"],      at["event_product_currency_code"],
            at["invoiced"],                 at["coworker_invoice_source_id"],
            at["coworker_invoice_number"],  at["coworker_invoice_paid"],
            at["due_date"],                 at["purchase_order"],
            at["updated_by"],
            at["created_on"],               at["updated_on"],
        )
        return (at["source_id"], *vals, at["source_id"], *vals)
