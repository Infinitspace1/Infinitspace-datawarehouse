"""
shared/azure_clients/silver_writer_calendar_events.py

Reads bronze.nexudus_calendar_events, transforms, and MERGEs into
silver.nexudus_calendar_events (single table, all columns).
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.exclusions import is_excluded_location_source_id
from shared.nexudus.transformers.calendar_events import transform_calendar_event

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.nexudus_calendar_events AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        location_source_id = ?,
        name = ?, slug = ?,
        short_description = ?, long_description = ?,
        venue_name = ?, venue_address = ?,
        web_address = ?, tickets_page = ?, facebook_page = ?,
        host_full_name = ?,
        resource_source_id = ?,
        start_date = ?, end_date = ?, publish_date = ?,
        only_for_contacts = ?, only_for_members = ?,
        allow_comments = ?, enable_wait_list = ?,
        show_event_attendees = ?, show_in_home_page = ?, show_in_home_banner = ?,
        repeat_event = ?, repeats = ?, repeat_every = ?,
        repeat_until = ?, repeat_series_unique_id = ?,
        has_event_form = ?, form_page_id = ?, form_page_name = ?,
        ticket_notes = ?,
        large_logo_file_name = ?, small_logo_file_name = ?,
        updated_by = ?,
        created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        location_source_id,
        name, slug,
        short_description, long_description,
        venue_name, venue_address,
        web_address, tickets_page, facebook_page,
        host_full_name,
        resource_source_id,
        start_date, end_date, publish_date,
        only_for_contacts, only_for_members,
        allow_comments, enable_wait_list,
        show_event_attendees, show_in_home_page, show_in_home_banner,
        repeat_event, repeats, repeat_every,
        repeat_until, repeat_series_unique_id,
        has_event_form, form_page_id, form_page_name,
        ticket_notes,
        large_logo_file_name, small_logo_file_name,
        updated_by,
        created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?,
        ?,
        ?,
        ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?, ?,
        ?,
        ?, ?,
        ?,
        ?, ?
    );
"""


class SilverCalendarEventsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze calendar event records")

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                ev = transform_calendar_event(raw, row["id"], self.sync_run_id)
                if is_excluded_location_source_id(ev["location_source_id"]):
                    continue
                params_list.append(self._make_params(ev))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver calendar events: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "calendar_events": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_calendar_events",
            source_name="nexudus",
            entity="calendar_events",
        )

    def _make_params(self, ev: dict) -> tuple:
        vals = (
            ev["unique_id"],            ev["bronze_id"],            ev["sync_run_id"],
            ev["location_source_id"],
            ev["name"],                 ev["slug"],
            ev["short_description"],    ev["long_description"],
            ev["venue_name"],           ev["venue_address"],
            ev["web_address"],          ev["tickets_page"],         ev["facebook_page"],
            ev["host_full_name"],
            ev["resource_source_id"],
            ev["start_date"],           ev["end_date"],             ev["publish_date"],
            ev["only_for_contacts"],    ev["only_for_members"],
            ev["allow_comments"],       ev["enable_wait_list"],
            ev["show_event_attendees"], ev["show_in_home_page"],    ev["show_in_home_banner"],
            ev["repeat_event"],         ev["repeats"],              ev["repeat_every"],
            ev["repeat_until"],         ev["repeat_series_unique_id"],
            ev["has_event_form"],       ev["form_page_id"],         ev["form_page_name"],
            ev["ticket_notes"],
            ev["large_logo_file_name"], ev["small_logo_file_name"],
            ev["updated_by"],
            ev["created_on"],           ev["updated_on"],
        )
        return (ev["source_id"], *vals, ev["source_id"], *vals)
