"""
functions/bronze_nexudus.py

Blueprint: Timer trigger (daily) that pulls all Nexudus entities
and writes raw JSON to the bronze layer.

Entities pulled (in order):
  1. locations              -- GET /sys/businesses
  2. products               -- GET /sys/floorplandesks (all item types)
  3. contracts              -- GET /billing/coworkercontracts
  4. coworker_invoices      -- GET /billing/coworkerinvoices
  5. coworkers              -- GET /spaces/coworkers/{id}
  6. resources              -- GET /spaces/resources/{id}
  7. extra_services         -- GET /billing/extraservices
  8. coworker_invoice_lines -- GET /billing/coworkerinvoicelines (per invoice)

Incremental sync:
  Paginated entities (locations, products, contracts, extra_services,
  coworker_invoices) use the UpdatedSince watermark from meta.sync_runs.
  First run does a full fetch; subsequent runs only fetch records updated
  since the last successful bronze run for that entity.

  Per-record entities (resources, coworkers) are driven by their parent
  entity and cannot use UpdatedSince directly.

  coworker_invoice_lines fetches lines only for invoices updated in the
  current run (from coworker_invoices).

Each entity gets its own RunTracker entry in meta.sync_runs.
"""
import asyncio
import logging
import os
import uuid

import azure.functions as func

from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient
from shared.azure_clients.blob_writer import BlobWriter
from shared.azure_clients.bronze_writer import BronzeWriter
from shared.azure_clients.run_tracker import RunTracker
from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("NEXUDUS_SYNC_SCHEDULE", "0 0 2 * * *")


# ── Watermark helper ────────────────────────────────────────────

def _get_watermark(entity: str) -> str | None:
    """Return ISO timestamp of the last successful bronze run for entity, or None."""
    try:
        sql = get_sql_client()
        rows = sql.execute_query(
            """
            SELECT TOP 1 finished_at
            FROM meta.sync_runs
            WHERE source_name = 'nexudus'
              AND entity = ?
              AND layer = 'bronze'
              AND status = 'success'
            ORDER BY finished_at DESC
            """,
            (entity,),
        )
        if rows and rows[0].get("finished_at"):
            ts = rows[0]["finished_at"]
            dt = ts if hasattr(ts, "strftime") else __import__("datetime").datetime.fromisoformat(str(ts))
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as exc:
        logger.warning("Could not read %s watermark: %s", entity, exc)
    return None


def _incremental_params(entity: str) -> dict:
    """Build extra_params with UpdatedSince if a watermark exists."""
    watermark = _get_watermark(entity)
    if watermark:
        logger.info("%s: incremental fetch since %s", entity, watermark)
        return {"UpdatedSince": watermark}
    logger.info("%s: no watermark found, doing full fetch", entity)
    return {}


# ── Main trigger ────────────────────────────────────────────────

@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False)
async def nexudus_to_bronze(timer: func.TimerRequest) -> None:
    logger.info("Nexudus -> Bronze sync started")

    try:
        bearer_token = get_bearer_token()
    except EnvironmentError as e:
        logger.error(f"Auth failed: {e}")
        raise

    async with NexudusClient(bearer_token) as client:
        run_id = uuid.uuid4()
        blob_writer = BlobWriter()
        writer = BronzeWriter(run_id)

        locations = await _sync_locations(client, blob_writer, writer, run_id)
        products, resource_ids_by_location = await _sync_products(client, blob_writer, writer, run_id, locations)
        await _sync_contracts(client, blob_writer, writer, run_id, products)
        invoice_records, coworker_ids = await _sync_coworker_invoices(client, blob_writer, writer, run_id)
        await _sync_coworkers(client, blob_writer, writer, run_id, coworker_ids)
        await _sync_resources(client, blob_writer, writer, run_id, resource_ids_by_location)
        await _sync_extra_services(client, blob_writer, writer, run_id)
        await _sync_coworker_invoice_lines(client, blob_writer, writer, run_id, invoice_records)

    logger.info(f"Nexudus -> Bronze sync complete [run_id={run_id}]")


# ── Entity sync functions ───────────────────────────────────────

async def _sync_locations(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
) -> list[dict]:
    extra_params = _incremental_params("locations")
    async with RunTracker("nexudus", "locations", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("sys/businesses", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("locations", records, run_id)
        run.rows_written = writer.write_locations(records)
        logger.info(
            f"Locations: {run.rows_read} fetched, {run.rows_written} written to bronze "
            f"[blob={blob_path}]"
        )
        return records


async def _sync_products(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    locations: list[dict],
) -> tuple[list[dict], dict[int, list[int]]]:
    extra_params = _incremental_params("products")
    async with RunTracker("nexudus", "products", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("sys/floorplandesks", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("products", records, run_id)
        run.rows_written = writer.write_products(records)

        resource_ids_by_location: dict[int, list[int]] = {}
        for r in records:
            resource_id = r.get("ResourceId")
            location_id = r.get("FloorPlanBusinessId")
            if resource_id and location_id:
                resource_ids_by_location.setdefault(location_id, [])
                if resource_id not in resource_ids_by_location[location_id]:
                    resource_ids_by_location[location_id].append(resource_id)

        logger.info(
            f"Products: {run.rows_read} fetched, {run.rows_written} written to bronze. "
            f"ResourceIds found: {sum(len(v) for v in resource_ids_by_location.values())} "
            f"[blob={blob_path}]"
        )
        return records, resource_ids_by_location


async def _sync_contracts(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    products: list[dict],
) -> None:
    extra_params = _incremental_params("contracts")
    async with RunTracker("nexudus", "contracts", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("billing/coworkercontracts", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("contracts", records, run_id)
        run.rows_written = writer.write_contracts(records)
        logger.info(
            f"Contracts: {run.rows_read} fetched, {run.rows_written} written to bronze "
            f"[blob={blob_path}]"
        )


async def _sync_resources(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    resource_ids_by_location: dict[int, list[int]],
) -> None:
    """Resources are fetched per-ID from products — no UpdatedSince support."""
    all_resource_ids = [
        (location_id, resource_id)
        for location_id, ids in resource_ids_by_location.items()
        for resource_id in ids
    ]

    if not all_resource_ids:
        logger.info("Resources: no ResourceIds found in products, skipping")
        return

    async with RunTracker("nexudus", "resources", "bronze", metadata=str(run_id)) as run:
        run.rows_read = len(all_resource_ids)

        tasks = [
            client.get_one(f"spaces/resources/{resource_id}")
            for _, resource_id in all_resource_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        records = []
        for (location_id, resource_id), result in zip(all_resource_ids, results):
            if isinstance(result, Exception):
                logger.warning(f"Resource {resource_id} failed: {result}")
                run.rows_skipped += 1
                continue
            if result:
                records.append((result, location_id))

        blob_records = [
            {"location_id": location_id, "record": record}
            for record, location_id in records
        ]
        blob_path = blob_writer.write_snapshot("resources", blob_records, run_id)

        total_written = 0
        for record, location_id in records:
            total_written += writer.write_resources([record], location_id=location_id)

        run.rows_written = total_written
        logger.info(
            f"Resources: {run.rows_read} attempted, "
            f"{run.rows_written} written, {run.rows_skipped} skipped "
            f"[blob={blob_path}]"
        )


async def _sync_extra_services(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
) -> None:
    extra_params = _incremental_params("extra_services")
    async with RunTracker("nexudus", "extra_services", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("billing/extraservices", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("extra_services", records, run_id)
        run.rows_written = writer.write_extra_services(records)
        logger.info(
            f"Extra services: {run.rows_read} fetched, {run.rows_written} written to bronze "
            f"[blob={blob_path}]"
        )


async def _sync_coworker_invoices(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
) -> tuple[list[dict], list[int]]:
    extra_params = _incremental_params("coworker_invoices")
    async with RunTracker("nexudus", "coworker_invoices", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("billing/coworkerinvoices", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("coworker_invoices", records, run_id)
        run.rows_written = writer.write_coworker_invoices(records)
        coworker_ids = sorted({int(r["CoworkerId"]) for r in records if r.get("CoworkerId")})
        logger.info(
            "Coworker invoices: %s fetched, %s written to bronze. Distinct coworker ids: %s [blob=%s]",
            run.rows_read,
            run.rows_written,
            len(coworker_ids),
            blob_path,
        )
        return records, coworker_ids


async def _sync_coworkers(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    coworker_ids: list[int],
) -> None:
    """Coworkers are fetched per-ID from invoices — no UpdatedSince support."""
    if not coworker_ids:
        logger.info("Coworkers: no CoworkerIds found in coworker invoices, skipping")
        return

    async with RunTracker("nexudus", "coworkers", "bronze", metadata=str(run_id)) as run:
        run.rows_read = len(coworker_ids)
        tasks = [client.get_coworker(coworker_id) for coworker_id in coworker_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        records = []
        for coworker_id, result in zip(coworker_ids, results):
            if isinstance(result, Exception):
                logger.warning("Coworker %s failed: %s", coworker_id, result)
                run.rows_skipped += 1
                continue
            if result:
                records.append(result)

        blob_path = blob_writer.write_snapshot("coworkers", records, run_id)
        run.rows_written = writer.write_coworkers(records)
        logger.info(
            "Coworkers: %s attempted, %s written, %s skipped [blob=%s]",
            run.rows_read,
            run.rows_written,
            run.rows_skipped,
            blob_path,
        )


async def _sync_coworker_invoice_lines(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    invoice_records: list[dict],
) -> None:
    """Fetch line items only for invoices updated in the current run.

    On the first run (no invoice_records because coworker_invoices did a full
    fetch with UpdatedSince and returned everything), this processes all of them.
    On incremental runs, only invoices that changed are re-fetched.
    """
    if not invoice_records:
        logger.info("Coworker invoice lines: no updated invoices, skipping")
        return

    invoice_source_ids = [
        int(r["Id"]) for r in invoice_records if r.get("Id")
    ]

    async with RunTracker("nexudus", "coworker_invoice_lines", "bronze", metadata=str(run_id)) as run:
        run.rows_read = len(invoice_source_ids)
        all_lines: list[dict] = []
        errors = 0

        for invoice_id in invoice_source_ids:
            try:
                lines = await client.get_coworker_invoice_lines(invoice_id)
                all_lines.extend(lines)
            except Exception as exc:
                logger.warning("Invoice lines for %s failed: %s", invoice_id, exc)
                errors += 1

        run.rows_skipped = errors
        if all_lines:
            blob_path = blob_writer.write_snapshot("coworker_invoice_lines", all_lines, run_id)
            run.rows_written = writer.write_coworker_invoice_lines(all_lines)
        else:
            blob_path = "none"
            run.rows_written = 0

        logger.info(
            "Coworker invoice lines: %s invoices queried, %s lines fetched, "
            "%s written to bronze, %s errors [blob=%s]",
            len(invoice_source_ids),
            len(all_lines),
            run.rows_written,
            errors,
            blob_path,
        )
