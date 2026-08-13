"""
scripts/python_scripts/backfill_full_resources.py

One-time backfill: fetch EVERY Nexudus resource (full ID sweep from
GET /spaces/resources, then per-ID detail fetch) into bronze, then rebuild
silver.nexudus_resources from bronze.

Why: until Aug 2026 resources were only fetched per-ID when a referencing
floor-plan product changed, so resources whose own flags changed (e.g. the
Fox Court "Large board/class room" hidden by reception in June 2026) stayed
stale in silver — is_visible never updated, system_resource_type never
populated for new rows. The nightly sync now does the full sweep too
(functions/bronze_nexudus.py::_sync_resources); this script is the immediate
catch-up so the AVA refresh can be re-run without waiting for tonight.

Usage:
    python scripts/python_scripts/backfill_full_resources.py
"""
import asyncio
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("backfill_full_resources")

from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.bronze_writer import BronzeWriter
from shared.azure_clients.silver_writer_resources import SilverResourcesWriter
from shared.azure_clients.sql_client import get_sql_client
from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient


class FullReloadSilverResourcesWriter(SilverResourcesWriter):
    """Reload EVERY bronze row, not just rows changed since the last silver run.

    The nightly worker is incremental on bronze.synced_at, and the bronze
    writer hash-skips unchanged payloads — so a resource whose Nexudus payload
    is byte-identical to its last fetch would never re-transform, and the
    columns this backfill exists to populate (system_resource_type,
    is_archived, allocation) would stay NULL on exactly those rows.
    """

    def _load_latest_bronze(self) -> list[dict]:
        return get_sql_client().execute_query(
            """
            SELECT b.id, b.raw_json
            FROM bronze.nexudus_resources b
            INNER JOIN (
                SELECT source_id, MAX(synced_at) AS latest
                FROM bronze.nexudus_resources
                GROUP BY source_id
            ) latest ON b.source_id = latest.source_id
                    AND b.synced_at  = latest.latest
            """
        )


async def main() -> None:
    run_id = uuid.uuid4()
    token = get_bearer_token()

    async with NexudusClient(token) as client:
        list_records = await client.get_all("spaces/resources")
        resource_ids = [int(r["Id"]) for r in list_records if r.get("Id")]
        logger.info("Resource list sweep: %s ids", len(resource_ids))

        tasks = [client.get_one(f"spaces/resources/{rid}") for rid in resource_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    records = []
    for rid, result in zip(resource_ids, results):
        if isinstance(result, Exception):
            logger.warning("Resource %s failed: %s", rid, result)
            continue
        if result:
            records.append(result)
    logger.info("Fetched %s resource detail records", len(records))

    blob_writer = BlobWriter()
    writer = BronzeWriter(run_id)
    blob_path = blob_writer.write_snapshot("resources", records, run_id)

    total_written = 0
    for record in records:
        _changed, written = writer.write_resources([record], location_id=record.get("BusinessId"))
        total_written += written
    logger.info("Bronze: %s written (unchanged payloads skipped) [blob=%s]", total_written, blob_path)

    silver_stats = FullReloadSilverResourcesWriter(run_id).run()
    logger.info("Silver: %s", silver_stats)
    print(f"Done. bronze written={total_written}, silver={silver_stats}")


if __name__ == "__main__":
    asyncio.run(main())
