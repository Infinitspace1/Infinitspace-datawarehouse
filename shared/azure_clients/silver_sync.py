from __future__ import annotations

from datetime import datetime
from typing import Optional

from shared.azure_clients.sql_client import get_sql_client


def get_last_successful_run_started_at(
    source_name: str,
    entity: str,
    layer: str = "silver",
) -> Optional[datetime]:
    """
    Return the latest successful started_at for a tracked sync entity.

    We use started_at rather than finished_at so a bronze row written while a
    silver run is still in progress is re-read on the next run instead of
    being skipped forever.
    """
    sql = get_sql_client()
    return sql.execute_scalar(
        """
        SELECT MAX(started_at)
        FROM meta.sync_runs
        WHERE source_name = ?
          AND entity = ?
          AND layer = ?
          AND status = 'success'
        """,
        (source_name, entity, layer),
    )


def load_latest_bronze_rows(
    table: str,
    source_name: str,
    entity: str,
    columns: str = "b.id, b.raw_json",
) -> list[dict]:
    """
    Load the latest bronze row per source_id, optionally restricted to rows
    changed since the last successful silver run for the entity.
    """
    sql = get_sql_client()
    watermark = get_last_successful_run_started_at(source_name, entity, "silver")

    query = f"""
        SELECT {columns}
        FROM {table} b
        INNER JOIN (
            SELECT source_id, MAX(synced_at) AS latest
            FROM {table}
            GROUP BY source_id
        ) latest ON b.source_id = latest.source_id
                AND b.synced_at  = latest.latest
    """

    params = None
    if watermark is not None:
        query += "\nWHERE b.synced_at >= ?"
        params = (watermark,)

    return sql.execute_query(query, params)
