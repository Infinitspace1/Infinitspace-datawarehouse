"""
shared/azure_clients/silver_writer_contracts.py

Reads bronze.nexudus_contracts, transforms, and MERGEs into
silver.nexudus_contracts (single table, all columns).
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.transformers.contracts import transform_contract

logger = logging.getLogger(__name__)


class SilverContractsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze contract records")

        ok = errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                c = transform_contract(raw, row["id"], self.sync_run_id)
                self._upsert(c)
                ok += 1
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        logger.info(f"Silver contracts: {ok} upserted, {errors} errors")
        return {"contracts": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return self.sql.execute_query("""
            SELECT b.id, b.raw_json
            FROM bronze.nexudus_contracts b
            INNER JOIN (
                SELECT source_id, MAX(synced_at) AS latest
                FROM bronze.nexudus_contracts
                GROUP BY source_id
            ) latest ON b.source_id = latest.source_id
                    AND b.synced_at  = latest.latest
        """)

    def _upsert(self, c: dict):
        cols_update = """
            unique_id = ?, bronze_id = ?, sync_run_id = ?,
            active = ?, cancelled = ?, main_contract = ?, in_paused_period = ?,
            coworker_id = ?, coworker_name = ?, coworker_email = ?,
            coworker_company = ?, coworker_billing_name = ?,
            coworker_type = ?, coworker_active = ?,
            location_source_id = ?, location_name = ?,
            tariff_id = ?, tariff_name = ?, tariff_price = ?, currency_code = ?,
            next_tariff_id = ?, next_tariff_name = ?,
            floor_plan_desk_ids = ?, floor_plan_desk_names = ?,
            price = ?, price_with_products = ?, unit_price = ?,
            quantity = ?, billing_day = ?,
            apply_pro_rating = ?, pro_rate_cancellation = ?,
            include_signup_fee = ?, cancellation_limit_days = ?,
            start_date = ?, contract_term = ?, renewal_date = ?,
            cancellation_date = ?, invoiced_period = ?,
            term_duration_months = ?,
            notes = ?, updated_by = ?,
            created_on = ?, updated_on = ?,
            last_synced_at = GETUTCDATE()
        """

        vals = (
            c["unique_id"],             c["bronze_id"],         c["sync_run_id"],
            c["active"],                c["cancelled"],         c["main_contract"],     c["in_paused_period"],
            c["coworker_id"],           c["coworker_name"],     c["coworker_email"],
            c["coworker_company"],      c["coworker_billing_name"],
            c["coworker_type"],         c["coworker_active"],
            c["location_source_id"],    c["location_name"],
            c["tariff_id"],             c["tariff_name"],       c["tariff_price"],      c["currency_code"],
            c["next_tariff_id"],        c["next_tariff_name"],
            c["floor_plan_desk_ids"],   c["floor_plan_desk_names"],
            c["price"],                 c["price_with_products"],c["unit_price"],
            c["quantity"],              c["billing_day"],
            c["apply_pro_rating"],      c["pro_rate_cancellation"],
            c["include_signup_fee"],    c["cancellation_limit_days"],
            c["start_date"],            c["contract_term"],     c["renewal_date"],
            c["cancellation_date"],     c["invoiced_period"],
            c["term_duration_months"],
            c["notes"],                 c["updated_by"],
            c["created_on"],            c["updated_on"],
        )

        self.sql.execute_non_query(f"""
            MERGE silver.nexudus_contracts AS target
            USING (SELECT ? AS source_id) AS source
                ON target.source_id = source.source_id
            WHEN MATCHED THEN UPDATE SET {cols_update}
            WHEN NOT MATCHED THEN INSERT (
                source_id, unique_id, bronze_id, sync_run_id,
                active, cancelled, main_contract, in_paused_period,
                coworker_id, coworker_name, coworker_email,
                coworker_company, coworker_billing_name,
                coworker_type, coworker_active,
                location_source_id, location_name,
                tariff_id, tariff_name, tariff_price, currency_code,
                next_tariff_id, next_tariff_name,
                floor_plan_desk_ids, floor_plan_desk_names,
                price, price_with_products, unit_price,
                quantity, billing_day,
                apply_pro_rating, pro_rate_cancellation,
                include_signup_fee, cancellation_limit_days,
                start_date, contract_term, renewal_date,
                cancellation_date, invoiced_period,
                term_duration_months,
                notes, updated_by,
                created_on, updated_on
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?,
                ?,
                ?, ?,
                ?, ?
            );
        """, (c["source_id"], *vals, c["source_id"], *vals))
