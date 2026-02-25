"""
shared/azure_clients/silver_writer_extra_services.py

Reads bronze.nexudus_extra_services, transforms, and MERGEs into
silver.nexudus_extra_services (single table, all columns).
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.transformers.extra_services import transform_extra_service

logger = logging.getLogger(__name__)


class SilverExtraServicesWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze extra service records")

        ok = errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                es = transform_extra_service(raw, row["id"], self.sync_run_id)
                self._upsert(es)
                ok += 1
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        logger.info(f"Silver extra services: {ok} upserted, {errors} errors")
        return {"extra_services": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return self.sql.execute_query("""
            SELECT b.id, b.raw_json
            FROM bronze.nexudus_extra_services b
            INNER JOIN (
                SELECT source_id, MAX(synced_at) AS latest
                FROM bronze.nexudus_extra_services
                GROUP BY source_id
            ) latest ON b.source_id = latest.source_id
                    AND b.synced_at  = latest.latest
        """)

    def _upsert(self, es: dict):
        cols_update = """
            unique_id = ?, bronze_id = ?, sync_run_id = ?,
            location_source_id = ?,
            name = ?, description = ?,
            price = ?, currency_code = ?, charge_period = ?,
            credit_price = ?, fixed_cost_price = ?, fixed_cost_length_minutes = ?,
            maximum_price = ?, min_length_minutes = ?, max_length_minutes = ?,
            is_default_price = ?, is_printing_credit = ?,
            only_for_contacts = ?, only_for_members = ?,
            apply_charge_to_visitors = ?, use_per_night_pricing = ?,
            last_minute_adjustment_type = ?,
            apply_from = ?, apply_to = ?,
            resource_type_names = ?,
            tax_rate_id = ?, reduced_tax_rate_id = ?, exempt_tax_rate_id = ?,
            financial_account_id = ?,
            updated_by = ?,
            created_on = ?, updated_on = ?,
            last_synced_at = GETUTCDATE()
        """

        vals = (
            es["unique_id"],                es["bronze_id"],                es["sync_run_id"],
            es["location_source_id"],
            es["name"],                     es["description"],
            es["price"],                    es["currency_code"],            es["charge_period"],
            es["credit_price"],             es["fixed_cost_price"],         es["fixed_cost_length_minutes"],
            es["maximum_price"],            es["min_length_minutes"],       es["max_length_minutes"],
            es["is_default_price"],         es["is_printing_credit"],
            es["only_for_contacts"],        es["only_for_members"],
            es["apply_charge_to_visitors"], es["use_per_night_pricing"],
            es["last_minute_adjustment_type"],
            es["apply_from"],               es["apply_to"],
            es["resource_type_names"],
            es["tax_rate_id"],              es["reduced_tax_rate_id"],      es["exempt_tax_rate_id"],
            es["financial_account_id"],
            es["updated_by"],
            es["created_on"],               es["updated_on"],
        )

        self.sql.execute_non_query(f"""
            MERGE silver.nexudus_extra_services AS target
            USING (SELECT ? AS source_id) AS source
                ON target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET {cols_update}
            WHEN NOT MATCHED THEN INSERT (
                source_id, unique_id, bronze_id, sync_run_id,
                location_source_id,
                name, description,
                price, currency_code, charge_period,
                credit_price, fixed_cost_price, fixed_cost_length_minutes,
                maximum_price, min_length_minutes, max_length_minutes,
                is_default_price, is_printing_credit,
                only_for_contacts, only_for_members,
                apply_charge_to_visitors, use_per_night_pricing,
                last_minute_adjustment_type,
                apply_from, apply_to,
                resource_type_names,
                tax_rate_id, reduced_tax_rate_id, exempt_tax_rate_id,
                financial_account_id,
                updated_by,
                created_on, updated_on
            ) VALUES (
                ?, ?, ?, ?,
                ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?,
                ?, ?,
                ?,
                ?, ?, ?,
                ?,
                ?,
                ?, ?
            );
        """, (es["source_id"], *vals, es["source_id"], *vals))
