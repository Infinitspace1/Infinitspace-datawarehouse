"""
shared/azure_clients/silver_writer_financial_accounts.py

Reads bronze.nexudus_financial_accounts, transforms via
shared/nexudus/transformers/financial_accounts.py, MERGEs into
silver.nexudus_financial_accounts.

Same pattern as SilverTariffsWriter.
"""
import json
import logging
import uuid

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.nexudus.transformers.financial_accounts import transform_financial_account

logger = logging.getLogger(__name__)

_MERGE_SQL = """
    MERGE silver.nexudus_financial_accounts AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        unique_id = ?, bronze_id = ?, sync_run_id = ?,
        name = ?, code = ?, description = ?, location_source_id = ?,
        account_type = ?, currency_code = ?,
        active = ?, is_deleted = ?,
        updated_by = ?, created_on = ?, updated_on = ?,
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        source_id, unique_id, bronze_id, sync_run_id,
        name, code, description, location_source_id,
        account_type, currency_code,
        active, is_deleted,
        updated_by, created_on, updated_on
    ) VALUES (
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?
    );
"""


class SilverFinancialAccountsWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info(f"Loaded {len(rows)} bronze financial-account records")

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                fa = transform_financial_account(raw, row["id"], self.sync_run_id)
                params_list.append(self._make_params(fa))
            except Exception as e:
                logger.warning(f"Failed source_id={raw.get('Id')}: {e}")
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info(f"Silver financial accounts: {ok} upserted, {errors} errors")
        return {"rows_read": len(rows), "financial_accounts": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.nexudus_financial_accounts",
            source_name="nexudus",
            entity="financial_accounts",
        )

    def _make_params(self, fa: dict) -> tuple:
        vals = (
            fa["unique_id"],       fa["bronze_id"],        fa["sync_run_id"],
            fa["name"],            fa["code"],             fa["description"],     fa["location_source_id"],
            fa["account_type"],    fa["currency_code"],
            fa["active"],          fa["is_deleted"],
            fa["updated_by"],      fa["created_on"],       fa["updated_on"],
        )
        return (fa["source_id"], *vals, fa["source_id"], *vals)
