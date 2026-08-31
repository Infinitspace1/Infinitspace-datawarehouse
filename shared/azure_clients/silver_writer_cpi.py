"""
shared/azure_clients/silver_writer_cpi.py

Reads bronze.cpi_series, transforms, and MERGEs into silver.cpi_series.

MERGE on source_id (provider:geo:period) rather than insert-only, so a month a
statistics office later revises updates in place instead of appearing twice.
"""
import json
import logging
import uuid

from shared.azure_clients.silver_sync import load_latest_bronze_rows
from shared.azure_clients.sql_client import get_sql_client
from shared.cpi.transformers.series import transform_observation

logger = logging.getLogger(__name__)

_COLUMNS = (
    "bronze_id", "sync_run_id",
    "provider", "geo", "index_code", "index_name", "base_year",
    "period", "index_level", "annual_rate_pct", "status",
    "source_url", "published_at",
)

_UPDATE_SET = ",\n        ".join(f"{column} = ?" for column in _COLUMNS)
_INSERT_COLUMNS = ", ".join(("source_id", *_COLUMNS))
_INSERT_PLACEHOLDERS = ", ".join("?" for _ in ("source_id", *_COLUMNS))

_MERGE_SQL = f"""
    MERGE silver.cpi_series AS target
    USING (SELECT ? AS source_id) AS source
        ON target.source_id = source.source_id
    WHEN MATCHED THEN UPDATE SET
        {_UPDATE_SET},
        last_synced_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT ({_INSERT_COLUMNS})
    VALUES ({_INSERT_PLACEHOLDERS});
"""


class SilverCpiSeriesWriter:

    def __init__(self, sync_run_id: uuid.UUID):
        self.sync_run_id = str(sync_run_id)
        self.sql = get_sql_client()

    def run(self) -> dict[str, int]:
        rows = self._load_latest_bronze()
        logger.info("Loaded %s bronze CPI observations", len(rows))

        params_list = []
        errors = 0
        for row in rows:
            raw = json.loads(row["raw_json"])
            try:
                obs = transform_observation(raw, row["id"], self.sync_run_id)
                params_list.append(self._make_params(obs))
            except Exception as e:  # noqa: BLE001 - one bad month must not kill the run
                logger.warning("Failed source_id=%s: %s", raw.get("source_id"), e)
                errors += 1

        if params_list:
            self.sql.execute_many(_MERGE_SQL, params_list)

        ok = len(params_list)
        logger.info("Silver CPI series: %s upserted, %s errors", ok, errors)
        return {"rows_read": len(rows), "series": ok, "errors": errors}

    def _load_latest_bronze(self) -> list[dict]:
        return load_latest_bronze_rows(
            "bronze.cpi_series",
            source_name="cpi",
            entity="series",
        )

    def _make_params(self, obs: dict) -> tuple:
        vals = tuple(obs[column] for column in _COLUMNS)
        return (obs["source_id"], *vals, obs["source_id"], *vals)
