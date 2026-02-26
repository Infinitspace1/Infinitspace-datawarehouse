"""
functions/bronze_nexudus.py

Blueprint: Timer trigger (daily) that pulls all Nexudus entities
and writes raw JSON to the bronze layer.

Entities pulled (in order):
  1. locations        -- GET /sys/businesses
  2. products         -- GET /sys/floorplandesks (all item types)
  3. contracts        -- GET /billing/coworkercontracts
  4. resources        -- GET /spaces/resources/{id}
  5. extra_services   -- GET /billing/extraservices

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

logger = logging.getLogger(__name__)

bp = func.Blueprint()

SCHEDULE = os.getenv("NEXUDUS_SYNC_SCHEDULE", "0 0 2 * * *")


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
        await _sync_resources(client, blob_writer, writer, run_id, resource_ids_by_location)
        await _sync_extra_services(client, blob_writer, writer, run_id)

    logger.info(f"Nexudus -> Bronze sync complete [run_id={run_id}]")


async def _sync_locations(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
) -> list[dict]:
    async with RunTracker("nexudus", "locations", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("sys/businesses")
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
    async with RunTracker("nexudus", "products", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("sys/floorplandesks")
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
    async with RunTracker("nexudus", "contracts", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("billing/coworkercontracts")
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
    async with RunTracker("nexudus", "extra_services", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("billing/extraservices")
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("extra_services", records, run_id)
        run.rows_written = writer.write_extra_services(records)
        logger.info(
            f"Extra services: {run.rows_read} fetched, {run.rows_written} written to bronze "
            f"[blob={blob_path}]"
        )
