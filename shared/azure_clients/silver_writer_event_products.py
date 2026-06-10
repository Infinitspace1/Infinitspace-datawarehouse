"""
shared/azure_clients/silver_writer_event_products.py

Reads bronze.nexudus_event_products, transforms, and MERGEs into
silver.nexudus_event_products (single table, all columns).

EventProduct payloads carry no BusinessId, so the writer resolves each
product's location from the latest bronze calendar events
(CalendarEventId -> calendar event's BusinessId) and passes it into the
transformer. Bronze runs before the silver fanout, so the event map is
complete by the time this writer executes.
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.exclusions import is_excluded_location_source_id
from shared.nexudus.transformers.event_products import transform_event_product

logger = logging.getLogger(__name__)

_EVENT_LOCATION_MAP_SQL = """
    SELECT source_id, location_id
    FROM (
        SELECT source_id, location_id,
               ROW_NUMBER() OVER (
                   PARTITION BY source_id
                   ORDER BY synced_at DESC, id DESC
               ) AS rn
        FROM bronze.nexudus_calendar_events
    ) sub
    WHERE rn = 1
"""

_MERGE_SQL = """
    MERGE silver.nexudus_event_products AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        calendar_event_source_id = ?,
        location_source_id = ?,
        name = ?, description = ?,
        price = ?, currency_code = ?,
        allocation = ?, sales = ?, max_tickets_per_attendee = ?,
        start_date = ?, end_date = ?,
        only_for_contacts = ?, only_for_members = ?,
        visible = ?, display_order = ?,
        ticket_notes = ?,
        tax_rate_id = ?, financial_account_id = ?,
        updated_by = ?,
        created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        calendar_event_source_id,
        location_source_id,
        name, description,
        price, currency_code,
        allocation, sales, max_tickets_per_attendee,
        start_date, end_date,
        only_for_contacts, only_for_members,
        visible, display_order,
        ticket_notes,
        tax_rate_id, financial_account_id,
        updated_by,
        created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?,
        ?,
        ?, ?,
        ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?,
        ?, ?,
        ?,
        ?, ?
    );
"""


class SilverEventProductsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze event product records")

        event_locations = self._load_event_location_map()

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                event_id = raw.get("CalendarEventId")
                location_id = event_locations.get(int(event_id)) if event_id else None
                if is_excluded_location_source_id(location_id):
                    continue
                ep = transform_event_product(
                    raw, row["id"], self.sync_run_id,
                    location_source_id=location_id,
                )
                params_list.append(self._make_params(ep))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver event products: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "event_products": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_event_products",
            source_name="nexudus",
            entity="event_products",
        )

    def _load_event_location_map(self) -> dict[int, int]:
        """{calendar_event_source_id -> location_source_id} from latest bronze events."""
        rows = self.sql.execute_query(_EVENT_LOCATION_MAP_SQL)
        return {
            int(r["source_id"]): int(r["location_id"])
            for r in rows
            if r.get("source_id") and r.get("location_id")
        }

    def _make_params(self, ep: dict) -> tuple:
        vals = (
            ep["unique_id"],            ep["bronze_id"],    ep["sync_run_id"],
            ep["calendar_event_source_id"],
            ep["location_source_id"],
            ep["name"],                 ep["description"],
            ep["price"],                ep["currency_code"],
            ep["allocation"],           ep["sales"],        ep["max_tickets_per_attendee"],
            ep["start_date"],           ep["end_date"],
            ep["only_for_contacts"],    ep["only_for_members"],
            ep["visible"],              ep["display_order"],
            ep["ticket_notes"],
            ep["tax_rate_id"],          ep["financial_account_id"],
            ep["updated_by"],
            ep["created_on"],           ep["updated_on"],
        )
        return (ep["source_id"], *vals, ep["source_id"], *vals)
