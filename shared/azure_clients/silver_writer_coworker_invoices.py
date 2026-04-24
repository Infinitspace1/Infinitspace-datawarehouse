"""
Reads bronze.nexudus_coworker_invoices, transforms, and MERGEs into
silver.nexudus_coworker_invoices.
"""
from __future__ import annotations

import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
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

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                invoice = transform_coworker_invoice(raw, row["id"], self.sync_run_id)
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
        return load_latest_bronze_rows(
            "bronze.nexudus_coworker_invoices",
            source_name="nexudus",
            entity="coworker_invoices",
        )

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
            invoice["due_date"], invoice["invoice_from_date"], invoice["invoice_to_date"],
            invoice["sent_on"], invoice["paid_on"], invoice["refunded_on"],
            invoice["total_amount"], invoice["paid_amount"], invoice["due_amount"],
            invoice["received_amount"], invoice["credited_amount"], invoice["refunded_amount"], invoice["tax_amount"],
            invoice["draft"], invoice["void"], invoice["paid"], invoice["sent"], invoice["refunded"], invoice["credit_note"],
            invoice["is_due"], invoice["xero_invoice_transferred"], invoice["xero_payment_transferred"],
            invoice["created_on"], invoice["updated_on"],
        )
        return (invoice["source_id"], *vals, invoice["source_id"], *vals)
