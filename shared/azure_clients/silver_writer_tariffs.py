"""
shared/azure_clients/silver_writer_tariffs.py

Reads bronze.nexudus_tariffs, transforms via shared/nexudus/transformers/tariffs.py,
MERGEs into silver.nexudus_tariffs.

Same shape as SilverExtraServicesWriter — tariffs are small reference data,
no per-location fan-out needed.
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.exclusions import is_excluded_location_source_id
from shared.nexudus.transformers.tariffs import transform_tariff

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.nexudus_tariffs AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        name = ?, description = ?, location_source_id = ?,
        price = ?, currency_code = ?,
        signup_fee = ?, deposit = ?, included_credit_amount = ?, time_credit_minutes = ?,
        charge_period = ?, billing_day = ?, term_duration_months = ?, notice_period_days = ?,
        financial_account_id = ?,
        active = ?, visible = ?, is_team_plan = ?, is_default = ?,
        apply_pro_rating = ?, pro_rate_cancellation = ?, is_deleted = ?,
        updated_by = ?, created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        name, description, location_source_id,
        price, currency_code,
        signup_fee, deposit, included_credit_amount, time_credit_minutes,
        charge_period, billing_day, term_duration_months, notice_period_days,
        financial_account_id,
        active, visible, is_team_plan, is_default,
        apply_pro_rating, pro_rate_cancellation, is_deleted,
        updated_by, created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?
    );
"""


class SilverTariffsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze tariff records")

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                t = transform_tariff(raw, row["id"], self.sync_run_id)
                if is_excluded_location_source_id(t["location_source_id"]):
                    continue
                params_list.append(self._make_params(t))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver tariffs: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "tariffs": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_tariffs",
            source_name="nexudus",
            entity="tariffs",
        )

    def _make_params(self, t: dict) -> tuple:
        vals = (
            t["unique_id"],            t["bronze_id"],          t["sync_run_id"],
            t["name"],                 t["description"],        t["location_source_id"],
            t["price"],                t["currency_code"],
            t["signup_fee"],           t["deposit"],            t["included_credit_amount"],  t["time_credit_minutes"],
            t["charge_period"],        t["billing_day"],        t["term_duration_months"],    t["notice_period_days"],
            t["financial_account_id"],
            t["active"],               t["visible"],            t["is_team_plan"],            t["is_default"],
            t["apply_pro_rating"],     t["pro_rate_cancellation"], t["is_deleted"],
            t["updated_by"],           t["created_on"],         t["updated_on"],
        )
        return (t["source_id"], *vals, t["source_id"], *vals)
