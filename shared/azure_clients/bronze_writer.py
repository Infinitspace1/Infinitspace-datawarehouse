"""
shared/azure_clients/bronze_writer.py

Writes raw records to the bronze layer.
Accepts full dicts from the Nexudus API and stores them as JSON.

Design:
  - Each write operation takes a sync_run_id so all rows from one
    run are grouped together.
  - A small number of denormalised columns (source_id, location_id, etc.)
    are extracted for indexing — everything else lives in raw_json.
  - Uses batch inserts for performance.
  - SHA-256 payload hashing: each record's raw_json is hashed and compared
    against the stored hash for the same source_id. Unchanged records are
    skipped to avoid unnecessary writes and downstream processing.
"""
import hashlib
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

BATCH_SIZE = 100  # rows per upsert batch
HASH_LOOKUP_BATCH = 500  # source_ids per hash lookup query


class BronzeWriter:
    """
    Writes raw Nexudus records to bronze.* tables.

    Usage:
        run_id = uuid.uuid4()
        writer = BronzeWriter(run_id)
        changed, written = writer.write_locations(records)
        ...
    """

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    # ── Helpers ──────────────────────────────────────────────

    def _to_json(self, record: dict) -> str:
        return json.dumps(record, default=str, ensure_ascii=False)

    def _compute_hash(self, raw_json: str) -> str:
        return hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

    def _load_existing_hashes(self, table: str, source_ids: list[int]) -> dict[int, str]:
        """Load the latest payload_hash per source_id from a bronze table."""
        if not source_ids:
            return {}

        result: dict[int, str] = {}
        for i in range(0, len(source_ids), HASH_LOOKUP_BATCH):
            batch_ids = source_ids[i : i + HASH_LOOKUP_BATCH]
            placeholders = ",".join("?" * len(batch_ids))
            rows = self.sql.execute_query(
                f"""
                SELECT source_id, payload_hash
                FROM (
                    SELECT source_id, payload_hash,
                           ROW_NUMBER() OVER (
                               PARTITION BY source_id
                               ORDER BY synced_at DESC, id DESC
                           ) AS rn
                    FROM {table}
                    WHERE source_id IN ({placeholders})
                ) sub
                WHERE rn = 1
                """,
                tuple(batch_ids),
            )
            for row in rows:
                if row.get("payload_hash"):
                    result[int(row["source_id"])] = row["payload_hash"]

        return result

    def _build_merge_sql(self, table: str, columns: list[str], update_columns: list[str]) -> str:
        source_projection = ", ".join([f"? AS {c}" for c in columns])
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{c}" for c in columns])
        update_set = ", ".join([f"target.{c} = source.{c}" for c in update_columns])

        return f"""
            MERGE {table} AS target
            USING (SELECT {source_projection}) AS source
                ON target.id = (
                    SELECT TOP 1 t.id
                    FROM {table} t
                    WHERE t.source_id = source.source_id
                    ORDER BY t.synced_at DESC, t.id DESC
                )
            WHEN MATCHED THEN UPDATE SET
                {update_set},
                target.synced_at = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
        """

    def _batch_upsert(
        self,
        table: str,
        columns: list[str],
        update_columns: list[str],
        rows: list[tuple],
    ) -> int:
        """Upsert rows in batches. Returns total rows processed."""
        if not rows:
            return 0

        sql = self._build_merge_sql(table, columns, update_columns)

        processed = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            self.sql.execute_many(sql, batch)
            processed += len(batch)
            logger.debug(f"{table}: upserted batch of {len(batch)}")

        return processed

    # ── Entity writers ───────────────────────────────────────
    # Each returns (changed_records, rows_written) where changed_records
    # is the list of original API dicts that were new or modified.

    def write_locations(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_locations"""
        table = "bronze.nexudus_locations"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "raw_json", "payload_hash"],
            ["sync_run_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_products(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_products"""
        table = "bronze.nexudus_products"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("FloorPlanBusinessId"),
                r.get("ItemType"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "location_id", "item_type", "raw_json", "payload_hash"],
            ["sync_run_id", "location_id", "item_type", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_contracts(self, records: list[dict], product_id: int = None, location_id: int = None) -> tuple[list[dict], int]:
        """bronze.nexudus_contracts"""
        table = "bronze.nexudus_contracts"
        source_ids = [int(r.get("id") or r.get("Id")) for r in records if r.get("id") or r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("id") or r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                product_id,
                location_id,
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "product_id", "location_id", "raw_json", "payload_hash"],
            ["sync_run_id", "product_id", "location_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_resources(self, records: list[dict], location_id: int = None) -> tuple[list[dict], int]:
        """bronze.nexudus_resources"""
        table = "bronze.nexudus_resources"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                location_id,
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "location_id", "raw_json", "payload_hash"],
            ["sync_run_id", "location_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_extra_services(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_extra_services"""
        table = "bronze.nexudus_extra_services"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("BusinessId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "location_id", "raw_json", "payload_hash"],
            ["sync_run_id", "location_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_coworker_invoices(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_coworker_invoices"""
        table = "bronze.nexudus_coworker_invoices"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("BusinessId"),
                r.get("CoworkerId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "location_id", "coworker_id", "raw_json", "payload_hash"],
            ["sync_run_id", "location_id", "coworker_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_coworker_invoice_lines(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_coworker_invoice_lines"""
        table = "bronze.nexudus_coworker_invoice_lines"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("CoworkerInvoiceId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "invoice_source_id", "raw_json", "payload_hash"],
            ["sync_run_id", "invoice_source_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_coworker_invoice_histories(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_coworker_invoice_histories"""
        table = "bronze.nexudus_coworker_invoice_histories"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("CoworkerInvoiceId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "invoice_source_id", "raw_json", "payload_hash"],
            ["sync_run_id", "invoice_source_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_coworkers(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_coworkers"""
        table = "bronze.nexudus_coworkers"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("InvoicingBusinessId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "location_id", "raw_json", "payload_hash"],
            ["sync_run_id", "location_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_calendar_events(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_calendar_events"""
        table = "bronze.nexudus_calendar_events"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("BusinessId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "location_id", "raw_json", "payload_hash"],
            ["sync_run_id", "location_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_event_attendees(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_event_attendees"""
        table = "bronze.nexudus_event_attendees"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("BusinessId"),
                r.get("CalendarEventId"),
                r.get("CoworkerId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "location_id", "calendar_event_id", "coworker_id", "raw_json", "payload_hash"],
            ["sync_run_id", "location_id", "calendar_event_id", "coworker_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_event_products(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_event_products

        EventProduct payloads carry no BusinessId — location is resolved
        downstream via CalendarEventId -> calendar_events.BusinessId.
        """
        table = "bronze.nexudus_event_products"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("CalendarEventId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "calendar_event_id", "raw_json", "payload_hash"],
            ["sync_run_id", "calendar_event_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_tariffs(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_tariffs — Phase 2 (2026-05-28).

        Small reference table (one row per price plan). No incremental
        watermark used on the sync side because the table is tiny; we
        re-fetch all every run and skip unchanged rows via the SHA-256
        hash comparison (same pattern as locations).
        """
        table = "bronze.nexudus_tariffs"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "raw_json", "payload_hash"],
            ["sync_run_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_financial_accounts(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_financial_accounts — Phase 2 (2026-05-28).

        Small reference table (accounting categories). Same approach as
        write_tariffs above.
        """
        table = "bronze.nexudus_financial_accounts"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "raw_json", "payload_hash"],
            ["sync_run_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    # ── Help desk (2026-08-20) ───────────────────────────────
    # Customer requests. Written by both the 15-minute reconciling poll
    # (functions/nexudus_helpdesk_sync.py) and the Nexudus webhook
    # (functions/nexudus_helpdesk_webhook.py). The webhook re-fetches the
    # canonical record by ID before calling these, so both paths store the
    # identical payload shape and the SHA-256 hash check stays meaningful.

    def write_helpdesk_messages(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_helpdesk_messages"""
        table = "bronze.nexudus_helpdesk_messages"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("BusinessId"),
                r.get("CoworkerId"),
                r.get("HelpDeskDepartmentId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "location_id", "coworker_id", "department_id", "raw_json", "payload_hash"],
            ["sync_run_id", "location_id", "coworker_id", "department_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_helpdesk_comments(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_helpdesk_comments

        HelpDeskComment payloads carry no BusinessId — location is resolved
        from the parent message by the silver writer.
        """
        table = "bronze.nexudus_helpdesk_comments"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("HelpDeskMessageId"),
                r.get("CoworkerId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "helpdesk_message_id", "coworker_id", "raw_json", "payload_hash"],
            ["sync_run_id", "helpdesk_message_id", "coworker_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written

    def write_helpdesk_departments(self, records: list[dict]) -> tuple[list[dict], int]:
        """bronze.nexudus_helpdesk_departments"""
        table = "bronze.nexudus_helpdesk_departments"
        source_ids = [int(r["Id"]) for r in records if r.get("Id")]
        existing = self._load_existing_hashes(table, source_ids)

        changed = []
        rows = []
        for r in records:
            raw = self._to_json(r)
            h = self._compute_hash(raw)
            sid = r.get("Id")
            if sid and existing.get(int(sid)) == h:
                continue
            changed.append(r)
            rows.append((
                self.sync_run_id,
                sid,
                r.get("BusinessId"),
                raw,
                h,
            ))
        written = self._batch_upsert(
            table,
            ["sync_run_id", "source_id", "location_id", "raw_json", "payload_hash"],
            ["sync_run_id", "location_id", "raw_json", "payload_hash"],
            rows,
        )
        return changed, written
