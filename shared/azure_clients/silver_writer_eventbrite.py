"""
shared/azure_clients/silver_writer_eventbrite.py

Reads bronze.eventbrite_events, transforms, and MERGEs into
silver.eventbrite_events (single table, all columns).
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.eventbrite.transformers.events import transform_event

logger = logging.getLogger(__name__)

_COLUMNS = (
    "bronze_id", "sync_run_id",
    "organization_id", "organizer_id", "organizer_name",
    "name", "summary",
    "description_text", "description_html",
    "url", "status", "currency",
    "start_utc", "start_local", "end_utc", "end_local", "timezone",
    "created", "changed", "published",
    "online_event", "listed", "shareable", "is_free",
    "is_series", "is_series_parent",
    "hide_start_date", "hide_end_date",
    "capacity", "capacity_is_custom",
    "series_id",
    "format_id", "format_name",
    "category_id", "category_name", "subcategory_id",
    "venue_id", "venue_resource_uri", "venue_name", "venue_address",
    "venue_address_1", "venue_address_2",
    "venue_city", "venue_region", "venue_postal_code", "venue_country",
    "venue_address_latitude", "venue_address_longitude",
    "venue_localized_area", "venue_multi_line_address",
    "venue_latitude", "venue_longitude",
    "venue_capacity", "venue_age_restriction",
    "has_available_tickets", "is_sold_out", "waitlist_available",
    "minimum_ticket_price", "minimum_ticket_price_display",
    "minimum_ticket_price_currency", "minimum_ticket_price_minor",
    "maximum_ticket_price", "maximum_ticket_price_display",
    "maximum_ticket_price_currency", "maximum_ticket_price_minor",
    "ticket_currency", "sales_start_utc", "sales_start_local", "sales_start_timezone",
    "logo_url",
)

_UPDATE_SET = ",\n        ".join(f"{column} = ?" for column in _COLUMNS)
_INSERT_COLUMNS = ", ".join(("source_id", *_COLUMNS))
_INSERT_PLACEHOLDERS = ", ".join("?" for _ in ("source_id", *_COLUMNS))

_MERGE_SQL = f"""
    MERGE silver.eventbrite_events AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        {_UPDATE_SET},
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT ({_INSERT_COLUMNS})
    VALUES ({_INSERT_PLACEHOLDERS});
"""


class SilverEventbriteEventsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze Eventbrite event records")

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                ev = transform_event(raw, row["id"], self.sync_run_id)
                if not ev["source_id"]:
                    raise ValueError("missing event id")
                params_list.append(self._make_params(ev))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver Eventbrite events: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "events": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.eventbrite_events",
            source_name="eventbrite",
            entity="events",
        )

    def _make_params(self, ev: dict) -> tuple:
        vals = tuple(ev[column] for column in _COLUMNS)
        return (ev["source_id"], *vals, ev["source_id"], *vals)
