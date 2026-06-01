"""
phase2_initial_sync.py

One-off local script — pulls Nexudus tariffs + financial_accounts into
bronze, then transforms them into silver. Use this to do the initial full
fetch from your dev machine so the nightly cron only has incremental work
(via UpdatedSince) from then on.

Prerequisites:
  1. silver_nexudus_billing_reference_schema.sql is deployed (creates the
     bronze.nexudus_tariffs, bronze.nexudus_financial_accounts,
     silver.nexudus_tariffs, silver.nexudus_financial_accounts tables).
  2. .env at repo root has the standard Nexudus + Azure SQL settings:
       NEXUDUS_USERNAME / NEXUDUS_PASSWORD          (or NEXUDUS_BEARER_TOKEN)
       AZURE_SQL_CONNECTION_STRING (or AZURE_SQL_SERVER + AZURE_SQL_DATABASE)
  3. Network access to spaces.nexudus.com (no VPN gating).

Usage:
  cd Infinitspace-datawarehouse
  python scripts/python_scripts/phase2_initial_sync.py

What it does:
  Bronze step  → fetches /api/billing/tariffs and /api/billing/financialaccounts
                  with NO UpdatedSince filter (full fetch — first time only),
                  upserts to bronze.nexudus_tariffs / bronze.nexudus_financial_accounts.
                  Records a meta.sync_runs row per entity with finished_at = now.
  Silver step  → reads the bronze rows, transforms via the silver writers,
                  upserts to silver.nexudus_tariffs / silver.nexudus_financial_accounts.

After this completes, the nightly cron in functions/bronze_nexudus.py will
use the meta.sync_runs.finished_at timestamps as the UpdatedSince watermark
and only fetch records changed since the local run.

Blob audit:
  If AZURE_STORAGE_ACCOUNT_NAME isn't set locally the blob snapshot write is
  silently skipped — the sync still completes, just without the audit
  snapshot. The nightly cron in Azure has the storage account configured so
  blob snapshots resume there.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

# Configure logging so we see what's happening
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("phase2_initial_sync")

from shared.nexudus.auth import get_bearer_token  # noqa: E402
from shared.nexudus.client import NexudusClient  # noqa: E402
from shared.azure_clients.bronze_writer import BronzeWriter  # noqa: E402
from shared.azure_clients.run_tracker import RunTracker  # noqa: E402
from shared.azure_clients.silver_writer_tariffs import SilverTariffsWriter  # noqa: E402
from shared.azure_clients.silver_writer_financial_accounts import (  # noqa: E402
    SilverFinancialAccountsWriter,
)


# ── Optional blob snapshot ─────────────────────────────────────────────────
# The blob snapshot is audit-only — bronze + silver SQL are the actual
# source of truth. For local one-off runs we tolerate two failure modes:
#   1. AZURE_STORAGE_ACCOUNT_NAME not set            → use no-op
#   2. AZURE_STORAGE_ACCOUNT_NAME set but local
#      Azure CLI identity lacks blob-write permission → graceful adapter
#      that catches the upload error and continues with bronze/silver.
# The production Function App has the right managed identity permissions so
# blob audit still happens there nightly.

class _NoopBlobWriter:
    def write_snapshot(self, *args, **kwargs) -> str:
        return "<blob-snapshot-skipped-local-run>"


class _GracefulBlobWriter:
    """Calls the real BlobWriter but catches any exception on write_snapshot
    so the sync can keep going without blob audit. Logs once per process."""
    def __init__(self, inner):
        self._inner = inner
        self._warned = False

    def write_snapshot(self, *args, **kwargs) -> str:
        try:
            return self._inner.write_snapshot(*args, **kwargs)
        except Exception as exc:
            if not self._warned:
                logger.warning(
                    "Blob snapshot upload failed (%s: %s) — continuing without "
                    "blob audit. This is fine for a local run; production cron "
                    "uses managed-identity creds with the right permissions.",
                    exc.__class__.__name__, str(exc).split('\n', 1)[0][:200],
                )
                self._warned = True
            return "<blob-snapshot-write-failed>"


def _make_blob_writer():
    if not os.getenv("AZURE_STORAGE_ACCOUNT_NAME"):
        logger.info("AZURE_STORAGE_ACCOUNT_NAME not set — skipping blob snapshot writes")
        return _NoopBlobWriter()
    try:
        from shared.azure_clients.blob_writer import BlobWriter
        return _GracefulBlobWriter(BlobWriter())
    except Exception as exc:
        logger.warning("Could not initialise BlobWriter (%s) — skipping blob snapshots", exc)
        return _NoopBlobWriter()


# ── Bronze sync (one entity at a time, no UpdatedSince) ─────────────────────

async def _bronze_sync(
    entity: str,
    api_path: str,
    writer_method_name: str,
    client: NexudusClient,
    bronze_writer: BronzeWriter,
    blob_writer,
    run_id: uuid.UUID,
) -> int:
    """Returns the number of records changed (written to bronze)."""
    async with RunTracker("nexudus", entity, "bronze", metadata=str(run_id)) as run:
        records = await client.get_all(api_path)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot(entity, records, run_id)
        write_fn = getattr(bronze_writer, writer_method_name)
        changed, run.rows_written = write_fn(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "%s: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            entity, run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
        )
        return len(changed)


# ── Silver upsert ───────────────────────────────────────────────────────────

def _silver_sync(writer_class, entity: str, sync_run_id: uuid.UUID) -> dict:
    """Wraps the silver writer in a meta.sync_runs row so it shows up in the
    monitoring email. Mirrors the schema RunTracker uses (id PK + INSERT
    'running' / UPDATE on completion) — see shared/azure_clients/run_tracker.py."""
    from datetime import datetime, timezone
    from shared.azure_clients.sql_client import get_sql_client

    sql = get_sql_client()
    run_uuid = str(uuid.uuid4())
    started = datetime.now(timezone.utc)

    # INSERT running
    sql.execute_non_query(
        """
        INSERT INTO meta.sync_runs
            (id, source_name, entity, layer, status,
             started_at, triggered_by, metadata)
        VALUES (?, 'nexudus', ?, 'silver', 'running',
                ?, 'manual', ?)
        """,
        (run_uuid, entity, started, str(sync_run_id)),
    )
    logger.info("Sync run started: nexudus/%s silver [%s]", entity, run_uuid)

    try:
        result = writer_class(sync_run_id).run()
        rows_written = next(
            (v for k, v in result.items() if k not in ("rows_read", "errors")),
            0,
        )
        finished = datetime.now(timezone.utc)
        sql.execute_non_query(
            """
            UPDATE meta.sync_runs
            SET status       = 'success',
                finished_at  = ?,
                rows_read    = ?,
                rows_written = ?,
                rows_skipped = 0,
                error_message = NULL
            WHERE id = ?
            """,
            (finished, result.get("rows_read", 0), rows_written, run_uuid),
        )
        duration = (finished - started).total_seconds()
        logger.info(
            "Sync run success: nexudus/%s silver [read=%s, written=%s, duration=%.1fs]",
            entity, result.get("rows_read", 0), rows_written, duration,
        )
        return result
    except Exception as exc:
        finished = datetime.now(timezone.utc)
        try:
            sql.execute_non_query(
                """
                UPDATE meta.sync_runs
                SET status       = 'failed',
                    finished_at  = ?,
                    error_message = ?
                WHERE id = ?
                """,
                (finished, str(exc)[:500], run_uuid),
            )
        except Exception:
            pass
        raise


# ── Driver ──────────────────────────────────────────────────────────────────

async def _main() -> None:
    logger.info("Phase 2 initial sync starting")

    try:
        bearer_token = get_bearer_token()
    except EnvironmentError as exc:
        logger.error("Auth failed: %s — set NEXUDUS_USERNAME/PASSWORD in .env", exc)
        sys.exit(1)

    run_id = uuid.uuid4()
    logger.info("run_id = %s", run_id)

    blob_writer = _make_blob_writer()

    # ── Bronze ──────────────────────────────────────────────────────────
    async with NexudusClient(bearer_token) as client:
        bronze_writer = BronzeWriter(run_id)

        tariff_changed = await _bronze_sync(
            entity="tariffs",
            api_path="billing/tariffs",
            writer_method_name="write_tariffs",
            client=client,
            bronze_writer=bronze_writer,
            blob_writer=blob_writer,
            run_id=run_id,
        )
        fa_changed = await _bronze_sync(
            entity="financial_accounts",
            api_path="billing/financialaccounts",
            writer_method_name="write_financial_accounts",
            client=client,
            bronze_writer=bronze_writer,
            blob_writer=blob_writer,
            run_id=run_id,
        )

    logger.info(
        "Bronze done: tariffs %s changed, financial_accounts %s changed",
        tariff_changed, fa_changed,
    )

    # ── Silver ──────────────────────────────────────────────────────────
    logger.info("Starting silver transforms")
    _silver_sync(SilverTariffsWriter, "tariffs", run_id)
    _silver_sync(SilverFinancialAccountsWriter, "financial_accounts", run_id)

    # ── Quick verification ──────────────────────────────────────────────
    from shared.azure_clients.sql_client import get_sql_client
    sql = get_sql_client()
    counts = sql.execute_query("""
        SELECT
            (SELECT COUNT(*) FROM silver.nexudus_tariffs)            AS tariff_count,
            (SELECT COUNT(*) FROM silver.nexudus_financial_accounts) AS financial_account_count,
            (SELECT COUNT(*) FROM silver.nexudus_financial_accounts
              WHERE LOWER(name) LIKE N'%membership fee%')             AS membership_fee_account_count
    """)
    if counts:
        c = counts[0]
        logger.info(
            "Silver populated: tariffs=%s, financial_accounts=%s, of which %s match '%%membership fee%%'",
            c.get("tariff_count"), c.get("financial_account_count"), c.get("membership_fee_account_count"),
        )

    logger.info("Phase 2 initial sync complete.")


if __name__ == "__main__":
    asyncio.run(_main())
