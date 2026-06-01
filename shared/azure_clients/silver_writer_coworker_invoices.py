"""
Reads bronze.nexudus_coworker_invoices, transforms, and MERGEs into
silver.nexudus_coworker_invoices.
"""
from __future__ import annotations

import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import get_last_successful_run_started_at
from shared.nexudus.transformers.coworker_invoices import transform_coworker_invoice

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.nexudus_coworker_invoices AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        coworker_id = ?, coworker_name = ?, coworker_billing_email = ?,
        coworker_company_name = ?, coworker_team_names = ?,
        location_source_id = ?, location_name = ?,
        invoice_number = ?, payment_reference = ?,
        bill_to_name = ?, bill_to_address = ?, bill_to_city = ?, bill_to_post_code = ?,
        bill_to_state = ?, bill_to_country_name = ?, bill_to_tax_id_number = ?,
        description = ?, currency_code = ?,
        invoice_status = ?, processing = ?, payment_failure_count = ?,
        due_date = ?, invoice_from_date = ?, invoice_to_date = ?,
        sent_on = ?, paid_on = ?, refunded_on = ?,
        total_amount = ?, paid_amount = ?, due_amount = ?,
        received_amount = ?, credited_amount = ?, refunded_amount = ?, tax_amount = ?,
        draft = ?, void = ?, paid = ?, sent = ?, refunded = ?, credit_note = ?,
        is_due = ?, xero_invoice_transferred = ?, xero_payment_transferred = ?,
        created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        coworker_id, coworker_name, coworker_billing_email,
        coworker_company_name, coworker_team_names,
        location_source_id, location_name,
        invoice_number, payment_reference,
        bill_to_name, bill_to_address, bill_to_city, bill_to_post_code,
        bill_to_state, bill_to_country_name, bill_to_tax_id_number,
        description, currency_code,
        invoice_status, processing, payment_failure_count,
        due_date, invoice_from_date, invoice_to_date,
        sent_on, paid_on, refunded_on,
        total_amount, paid_amount, due_amount,
        received_amount, credited_amount, refunded_amount, tax_amount,
        draft, void, paid, sent, refunded, credit_note,
        is_due, xero_invoice_transferred, xero_payment_transferred,
        created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?
    );
"""


class SilverCoworkerInvoicesWriter:
    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info("Loaded %s bronze coworker invoice records", len(rows))
        histories_by_invoice = self._load_histories_by_invoice(rows)

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                invoice = transform_coworker_invoice(
                    raw,
                    row["id"],
                    self.sync_run_id,
                    histories=histories_by_invoice.get(int(row["source_id"]), []),
                )
                params_list.append(self._make_params(invoice))
            except Exception as exc:
                logger.warning("Failed source_id=%s: %s", raw.get("Id"), exc)
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info("Silver coworker invoices: %s upserted, %s errors", ok, errors)
        return {"rows_read": len(rows), "coworker_invoices": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        watermark = get_last_successful_run_started_at(
            "nexudus", "coworker_invoices", "silver"
        )

        query = """
            WITH latest_invoice AS (
                SELECT source_id, MAX(synced_at) AS latest
                FROM bronze.nexudus_coworker_invoices
                GROUP BY source_id
            ),
            changed_history AS (
                SELECT DISTINCT invoice_source_id
                FROM bronze.nexudus_coworker_invoice_histories
                WHERE invoice_source_id IS NOT NULL
        """
        params = None
        if watermark is not None:
            query += "\n                  AND synced_at >= ?"
            params = (watermark, watermark)
        query += """
            )
            SELECT b.id, b.source_id, b.raw_json
            FROM bronze.nexudus_coworker_invoices b
            INNER JOIN latest_invoice latest
                ON b.source_id = latest.source_id
               AND b.synced_at = latest.latest
        """
        if watermark is not None:
            query += """
            WHERE b.synced_at >= ?
               OR EXISTS (
                    SELECT 1
                    FROM changed_history ch
                    WHERE ch.invoice_source_id = b.source_id
               )
            """

        return self.sql.execute_query(query, params)

    def _load_histories_by_invoice(self, rows: list[dict]) -> dict[int, list[dict]]:
        invoice_ids = [int(row["source_id"]) for row in rows if row.get("source_id")]
        if not invoice_ids:
            return {}

        result: dict[int, list[dict]] = {invoice_id: [] for invoice_id in invoice_ids}
        for i in range(0, len(invoice_ids), 500):
            batch = invoice_ids[i : i + 500]
            placeholders = ",".join("?" * len(batch))
            history_rows = self.sql.execute_query(
                f"""
                SELECT h.invoice_source_id, h.raw_json
                FROM bronze.nexudus_coworker_invoice_histories h
                INNER JOIN (
                    SELECT source_id, MAX(synced_at) AS latest
                    FROM bronze.nexudus_coworker_invoice_histories
                    WHERE invoice_source_id IN ({placeholders})
                    GROUP BY source_id
                ) latest
                    ON latest.source_id = h.source_id
                   AND latest.latest = h.synced_at
                WHERE h.invoice_source_id IN ({placeholders})
                """,
                tuple(batch + batch),
            )
            for history_row in history_rows:
                invoice_id = int(history_row["invoice_source_id"])
                result.setdefault(invoice_id, []).append(json.loads(history_row["raw_json"]))

        return result

    def _make_params(self, invoice: dict) -> tuple:
        vals = (
            invoice["unique_id"], invoice["bronze_id"], invoice["sync_run_id"],
            invoice["coworker_id"], invoice["coworker_name"], invoice["coworker_billing_email"],
            invoice["coworker_company_name"], invoice["coworker_team_names"],
            invoice["location_source_id"], invoice["location_name"],
            invoice["invoice_number"], invoice["payment_reference"],
            invoice["bill_to_name"], invoice["bill_to_address"], invoice["bill_to_city"], invoice["bill_to_post_code"],
            invoice["bill_to_state"], invoice["bill_to_country_name"], invoice["bill_to_tax_id_number"],
            invoice["description"], invoice["currency_code"],
            invoice["invoice_status"], invoice["processing"], invoice["payment_failure_count"],
            invoice["due_date"], invoice["invoice_from_date"], invoice["invoice_to_date"],
            invoice["sent_on"], invoice["paid_on"], invoice["refunded_on"],
            invoice["total_amount"], invoice["paid_amount"], invoice["due_amount"],
            invoice["received_amount"], invoice["credited_amount"], invoice["refunded_amount"], invoice["tax_amount"],
            invoice["draft"], invoice["void"], invoice["paid"], invoice["sent"], invoice["refunded"], invoice["credit_note"],
            invoice["is_due"], invoice["xero_invoice_transferred"], invoice["xero_payment_transferred"],
            invoice["created_on"], invoice["updated_on"],
        )
        return (invoice["source_id"], *vals, invoice["source_id"], *vals)
