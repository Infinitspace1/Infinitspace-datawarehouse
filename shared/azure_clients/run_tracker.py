"""
shared/azure_clients/run_tracker.py

Logs pipeline runs to meta.sync_runs and meta.sync_errors.
Use as a context manager so failures are always recorded.

Usage:
    async with RunTracker("nexudus", "locations", "bronze") as run:
        records = await fetch_locations()
        run.rows_read = len(records)
        writer.write_locations(records)
        run.rows_written = len(records)
"""
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)


class RunTracker:
    def __init__(
        self,
        source_name: str,
        entity: str,
        layer: str,
        triggered_by: str = "cron",
        metadata: Optional[str] = None,
    ):
        self.run_id = uuid.uuid4()
        self.source_name = source_name
        self.entity = entity
        self.layer = layer
        self.triggered_by = triggered_by
        self.metadata = metadata

        self.rows_read: int = 0
        self.rows_written: int = 0
        self.rows_skipped: int = 0

        self._sql = get_sql_client()
        self._started_at = datetime.now(timezone.utc)

    async def __aenter__(self):
        self._sql.execute_non_query("""
            INSERT INTO meta.sync_runs
                (id, source_name, entity, layer, status, started_at, triggered_by, metadata)
            VALUES (?, ?, ?, ?, 'running', ?, ?, ?)
        """, (
            str(self.run_id),
            self.source_name,
            self.entity,
            self.layer,
            self._started_at,
            self.triggered_by,
            self.metadata,
        ))
        logger.info(f"Sync run started: {self.source_name}/{self.entity} [{self.run_id}]")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        finished_at = datetime.now(timezone.utc)
        status = "success" if exc_type is None else "failed"
        error_msg = str(exc_val) if exc_val else None

        self._sql.execute_non_query("""
            UPDATE meta.sync_runs
            SET status       = ?,
                finished_at  = ?,
                rows_read    = ?,
                rows_written = ?,
                rows_skipped = ?,
                error_message = ?
            WHERE id = ?
        """, (
            status,
            finished_at,
            self.rows_read,
            self.rows_written,
            self.rows_skipped,
            error_msg,
            str(self.run_id),
        ))

        duration = (finished_at - self._started_at).total_seconds()
        logger.info(
            f"Sync run {status}: {self.source_name}/{self.entity} "
            f"[read={self.rows_read}, written={self.rows_written}, "
            f"skipped={self.rows_skipped}, duration={duration:.1f}s]"
        )
        return False  # don't suppress exceptions

    def log_error(self, source_id: str, error: Exception, raw_payload: str = None):
        """Record a record-level failure without failing the whole run."""
        try:
            self._sql.execute_non_query("""
                INSERT INTO meta.sync_errors
                    (sync_run_id, source_id, entity, error_message, raw_payload)
                VALUES (?, ?, ?, ?, ?)
            """, (
                str(self.run_id),
                str(source_id),
                self.entity,
                str(error),
                raw_payload,
            ))
        except Exception as e:
            logger.warning(f"Failed to log sync error: {e}")