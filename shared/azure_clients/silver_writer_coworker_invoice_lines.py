"""
Reads bronze.nexudus_coworker_invoice_lines, transforms, and MERGEs into
silver.nexudus_coworker_invoice_lines.
"""
from __future__ import annotations

import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.exclusions import is_excluded_location_source_id
from shared.nexudus.transformers.coworker_invoice_lines import transform_coworker_invoice_line

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.nexudus_coworker_invoice_lines AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        invoice_source_id = ?, coworker_id = ?, location_source_id = ?,
        description = ?,
        financial_account_code = ?, financial_account_name = ?,
        quantity = ?, unit_price = ?, sub_total = ?,
        tax_amount = ?, total_amount = ?,
        currency_code = ?, tax_category_name = ?, product_name = ?,
        created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        invoice_source_id, coworker_id, location_source_id,
        description,
        financial_account_code, financial_account_name,
        quantity, unit_price, sub_total,
        tax_amount, total_amount,
        currency_code, tax_category_name, product_name,
        created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?, ?, ?,
        ?,
        ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?, ?,
        ?, ?
    );
"""


class SilverCoworkerInvoiceLinesWriter:
    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info("Loaded %s bronze coworker invoice line records", len(rows))

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                line = transform_coworker_invoice_line(raw, row["id"], self.sync_run_id)
                if is_excluded_location_source_id(line["location_source_id"]):
                    continue
                params_list.append(self._make_params(line))
            except Exception as exc:
                logger.warning("Failed source_id=%s: %s", raw.get("Id"), exc)
                errors += 1

        batch_size = 500
        ok = 0
        for i in range(0, len(params_list), batch_size):
            batch = params_list[i : i + batch_size]
            self.sql.execute_many(_MERGE_SQL, batch)
            ok += len(batch)
            logger.info("Silver coworker invoice lines: batch %s/%s (%s rows)",
                         i // batch_size + 1,
                         (len(params_list) + batch_size - 1) // batch_size,
                         ok)

        logger.info("Silver coworker invoice lines: %s upserted, %s errors", ok, errors)
        return {"rows_read": len(rows), "coworker_invoice_lines": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_coworker_invoice_lines",
            source_name="nexudus",
            entity="coworker_invoice_lines",
        )

    def _make_params(self, line: dict) -> tuple:
        vals = (
            line["unique_id"], line["bronze_id"], line["sync_run_id"],
            line["invoice_source_id"], line["coworker_id"], line["location_source_id"],
            line["description"],
            line["financial_account_code"], line["financial_account_name"],
            line["quantity"], line["unit_price"], line["sub_total"],
            line["tax_amount"], line["total_amount"],
            line["currency_code"], line["tax_category_name"], line["product_name"],
            line["created_on"], line["updated_on"],
        )
        return (line["source_id"], *vals, line["source_id"], *vals)
