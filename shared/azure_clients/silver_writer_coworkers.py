"""
Reads bronze.nexudus_coworkers, transforms, and MERGEs into
silver.nexudus_coworkers.
"""
from __future__ import annotations

import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.transformers.coworkers import transform_coworker

logger = logging.getLogger(__name__)


class SilverCoworkersWriter:
    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info("Loaded %s bronze coworker records", len(rows))

        ok = errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                coworker = transform_coworker(raw, row["id"], self.sync_run_id)
                self._upsert(coworker)
                ok += 1
            except Exception as exc:
                logger.warning("Failed source_id=%s: %s", raw.get("Id"), exc)
                errors += 1

        logger.info("Silver coworkers: %s upserted, %s errors", ok, errors)
        return {"coworkers": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return self.sql.execute_query(
            """
            SELECT b.id, b.raw_json
            FROM bronze.nexudus_coworkers b
            INNER JOIN (
                SELECT source_id, MAX(synced_at) AS latest
                FROM bronze.nexudus_coworkers
                GROUP BY source_id
            ) latest ON b.source_id = latest.source_id
                    AND b.synced_at  = latest.latest
            """
        )

    def _upsert(self, coworker: dict) -> None:
        cols_update = """
            unique_id = ?, bronze_id = ?, sync_run_id = ?,
            coworker_type = ?, full_name = ?, email = ?, billing_email = ?, billing_name = ?,
            company_name = ?, team_name = ?, team_names = ?, team_ids = ?, business_ids = ?,
            location_source_id = ?, location_name = ?,
            mobile_phone = ?, land_line = ?,
            address = ?, post_code = ?, city_name = ?, state = ?,
            billing_address = ?, billing_post_code = ?, billing_city_name = ?, billing_state = ?,
            tax_id_number = ?, billing_day = ?,
            tariff_id = ?, tariff_name = ?, next_tariff_id = ?, next_tariff_name = ?,
            coworker_contract_ids = ?, coworker_contract_tariff_names = ?,
            active = ?, archived = ?, user_active = ?,
            notify_on_new_invoice = ?, notify_on_new_payment = ?, notify_on_failed_payment = ?,
            do_not_process_invoices_automatically = ?,
            user_last_access = ?, registration_date = ?, renewal_date = ?, start_date = ?, cancellation_date = ?,
            created_on = ?, updated_on = ?,
            last_synced_at = GETUTCDATE()
        """

        vals = (
            coworker["unique_id"], coworker["bronze_id"], coworker["sync_run_id"],
            coworker["coworker_type"], coworker["full_name"], coworker["email"], coworker["billing_email"], coworker["billing_name"],
            coworker["company_name"], coworker["team_name"], coworker["team_names"], coworker["team_ids"], coworker["business_ids"],
            coworker["location_source_id"], coworker["location_name"],
            coworker["mobile_phone"], coworker["land_line"],
            coworker["address"], coworker["post_code"], coworker["city_name"], coworker["state"],
            coworker["billing_address"], coworker["billing_post_code"], coworker["billing_city_name"], coworker["billing_state"],
            coworker["tax_id_number"], coworker["billing_day"],
            coworker["tariff_id"], coworker["tariff_name"], coworker["next_tariff_id"], coworker["next_tariff_name"],
            coworker["coworker_contract_ids"], coworker["coworker_contract_tariff_names"],
            coworker["active"], coworker["archived"], coworker["user_active"],
            coworker["notify_on_new_invoice"], coworker["notify_on_new_payment"], coworker["notify_on_failed_payment"],
            coworker["do_not_process_invoices_automatically"],
            coworker["user_last_access"], coworker["registration_date"], coworker["renewal_date"], coworker["start_date"], coworker["cancellation_date"],
            coworker["created_on"], coworker["updated_on"],
        )

        self.sql.execute_non_query(
            f"""
            MERGE silver.nexudus_coworkers AS target
            USING (SELECT ? AS source_id) AS source
                ON target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET {cols_update}
            WHEN NOT MATCHED THEN INSERT (
                source_id, unique_id, bronze_id, sync_run_id,
                coworker_type, full_name, email, billing_email, billing_name,
                company_name, team_name, team_names, team_ids, business_ids,
                location_source_id, location_name,
                mobile_phone, land_line,
                address, post_code, city_name, state,
                billing_address, billing_post_code, billing_city_name, billing_state,
                tax_id_number, billing_day,
                tariff_id, tariff_name, next_tariff_id, next_tariff_name,
                coworker_contract_ids, coworker_contract_tariff_names,
                active, archived, user_active,
                notify_on_new_invoice, notify_on_new_payment, notify_on_failed_payment,
                do_not_process_invoices_automatically,
                user_last_access, registration_date, renewal_date, start_date, cancellation_date,
                created_on, updated_on
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?,
                ?, ?, ?, ?, ?,
                ?, ?
            );
            """,
            (coworker["source_id"], *vals, coworker["source_id"], *vals),
        )
