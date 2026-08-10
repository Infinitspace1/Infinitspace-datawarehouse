"""
functions/bronze_nexudus.py

Blueprint: Timer trigger (daily) that pulls all Nexudus entities
and writes raw JSON to the bronze layer.

Entities pulled (in order):
  1. locations              -- GET /sys/businesses
  2. products               -- GET /sys/floorplandesks (all item types)
  3. contracts              -- GET /billing/coworkercontracts
  4. coworker_invoices      -- GET /billing/coworkerinvoices
  5. coworkers              -- GET /spaces/coworkers
  6. resources              -- GET /spaces/resources/{id}
  7. extra_services         -- GET /billing/extraservices
  8. coworker_invoice_lines -- GET /billing/coworkerinvoicelines
  9. coworker_invoice_histories -- GET /billing/coworkerinvoicehistories
 10. tariffs                -- GET /billing/tariffs                  (Phase 2, 2026-05-28)
 11. financial_accounts     -- GET /billing/financialaccounts        (Phase 2, 2026-05-28)
 12. calendar_events        -- GET /content/calendarevents           (Events, 2026-06-10)
 13. event_attendees        -- GET /content/eventattendees           (Events, 2026-06-10)
 14. event_products         -- GET /content/eventproducts            (Events, 2026-06-10)

Incremental sync:
  Paginated entities (locations, products, contracts, coworkers,
  extra_services) use the UpdatedSince watermark from meta.sync_runs.
  First run does a full fetch; subsequent runs only fetch records
  updated since the last successful bronze run for that entity.

  coworker_invoices uses from_CoworkerInvoice_UpdatedOn with a 3-day
  lookback window — no watermark dependency.

  open-invoice resync (2026-07-16): Nexudus does NOT bump an invoice's
  UpdatedOn when a payment/credit is applied, so the UpdatedOn window above
  silently freezes a settled invoice's DueAmount/PaidAmount at its last
  in-window value — the finance dashboard then shows the full gross amount
  for an invoice that is really almost paid. _resync_open_invoices re-fetches
  every currently-open unpaid invoice OBJECT by ID (independent of UpdatedOn)
  so its balance stays fresh. Small set (~tens of rows); the SHA-256 hash
  check only writes genuinely-changed payloads downstream.

  coworker_invoice_lines fetches lines only for invoices returned by
  the coworker_invoices step (using CoworkerInvoiceLine_CoworkerInvoice
  filter per invoice).

  coworker_invoice_histories fetches histories only for unpaid direct-debit
  invoices with due dates from the last month onward. These histories drive
  finance-dashboard suppression while payments are still processing.

  Per-record entities (resources) are driven by their parent entity and
  cannot use UpdatedSince directly.

Change detection:
  All entity writers compare SHA-256 hashes of incoming payloads against
  stored hashes. Only records with changed payloads are written to bronze
  and passed downstream. This avoids unnecessary DB writes and expensive
  per-record API calls for unchanged data.

Each entity gets its own RunTracker entry in meta.sync_runs.
"""
import asyncio
from datetime import datetime, timedelta, timezone
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

    async with RunTracker("nexudus", "bronze_sync", "bronze") as _top:
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
            changed_invoices = await _sync_coworker_invoices(client, blob_writer, writer, run_id)
            # Re-fetch every open invoice by ID so a payment/credit that Nexudus
            # applied WITHOUT bumping UpdatedOn (invisible to the incremental
            # window above) still refreshes its DueAmount downstream.
            await _resync_open_invoices(client, blob_writer, writer, run_id)
            await _sync_coworkers(client, blob_writer, writer, run_id)
            await _sync_resources(client, blob_writer, writer, run_id, resource_ids_by_location)
            await _sync_extra_services(client, blob_writer, writer, run_id)
            await _sync_coworker_invoice_lines(client, blob_writer, writer, run_id, changed_invoices)
            await _sync_coworker_invoice_histories(client, blob_writer, writer, run_id)
            # Phase 2 reference data — small tables, full re-fetch each run.
            await _sync_tariffs(client, blob_writer, writer, run_id)
            await _sync_financial_accounts(client, blob_writer, writer, run_id)
            # Events (2026-06-10): calendar events + attendees + ticket products.
            await _sync_calendar_events(client, blob_writer, writer, run_id)
            await _sync_event_attendees(client, blob_writer, writer, run_id)
            await _sync_event_products(client, blob_writer, writer, run_id)

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
        changed, run.rows_written = writer.write_locations(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Locations: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
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
        changed, run.rows_written = writer.write_products(records)
        run.rows_skipped = len(records) - len(changed)

        # Build resource IDs only from changed products
        resource_ids_by_location: dict[int, list[int]] = {}
        for r in changed:
            resource_id = r.get("ResourceId")
            location_id = r.get("FloorPlanBusinessId")
            if resource_id and location_id:
                resource_ids_by_location.setdefault(location_id, [])
                if resource_id not in resource_ids_by_location[location_id]:
                    resource_ids_by_location[location_id].append(resource_id)

        logger.info(
            "Products: %s fetched, %s changed, %s skipped, %s written to bronze. "
            "ResourceIds from changed: %s [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written,
            sum(len(v) for v in resource_ids_by_location.values()), blob_path,
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
        changed, run.rows_written = writer.write_contracts(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Contracts: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
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
        logger.info("Resources: no ResourceIds found in changed products, skipping")
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
            _changed, written = writer.write_resources([record], location_id=location_id)
            total_written += written

        run.rows_written = total_written
        logger.info(
            "Resources: %s attempted, %s written, %s skipped [blob=%s]",
            run.rows_read, run.rows_written, run.rows_skipped, blob_path,
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
        changed, run.rows_written = writer.write_extra_services(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Extra services: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
        )


async def _sync_tariffs(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
) -> None:
    """Phase 2 (2026-05-28): pull Nexudus tariffs.

    Uses the UpdatedSince watermark like the other paginated entities so each
    nightly run only fetches what's changed. The SHA-256 hash check in
    BronzeWriter.write_tariffs is the second line of defence — it catches
    updates where Nexudus's UpdatedOn timestamp was bumped but the actual
    payload didn't change.
    """
    extra_params = _incremental_params("tariffs")
    async with RunTracker("nexudus", "tariffs", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("billing/tariffs", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("tariffs", records, run_id)
        changed, run.rows_written = writer.write_tariffs(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Tariffs: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
        )


async def _sync_financial_accounts(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
) -> None:
    """Phase 2 (2026-05-28): pull Nexudus financial accounts.

    Same incremental approach as _sync_tariffs above.
    """
    extra_params = _incremental_params("financial_accounts")
    async with RunTracker("nexudus", "financial_accounts", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("billing/financialaccounts", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("financial_accounts", records, run_id)
        changed, run.rows_written = writer.write_financial_accounts(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Financial accounts: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
        )


async def _sync_calendar_events(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
) -> None:
    """Events (2026-06-10): pull Nexudus calendar events.

    Small table (~750 records). Uses the UpdatedSince watermark like the
    other paginated entities; the SHA-256 hash check in
    BronzeWriter.write_calendar_events skips unchanged payloads.
    """
    extra_params = _incremental_params("calendar_events")
    async with RunTracker("nexudus", "calendar_events", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("content/calendarevents", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("calendar_events", records, run_id)
        changed, run.rows_written = writer.write_calendar_events(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Calendar events: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
        )


async def _sync_event_attendees(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
) -> None:
    """Events (2026-06-10): pull Nexudus event attendees (ticket registrations)."""
    extra_params = _incremental_params("event_attendees")
    async with RunTracker("nexudus", "event_attendees", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("content/eventattendees", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("event_attendees", records, run_id)
        changed, run.rows_written = writer.write_event_attendees(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Event attendees: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
        )


async def _sync_event_products(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
) -> None:
    """Events (2026-06-10): pull Nexudus event products (ticket types per event)."""
    extra_params = _incremental_params("event_products")
    async with RunTracker("nexudus", "event_products", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("content/eventproducts", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("event_products", records, run_id)
        changed, run.rows_written = writer.write_event_products(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Event products: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
        )


async def _sync_coworker_invoices(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    lookback_days: int | None = None,
) -> list[dict]:
    days = lookback_days if lookback_days is not None else int(
        os.getenv("NEXUDUS_INVOICE_LOOKBACK_DAYS", "2")
    )
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    extra_params = {"from_CoworkerInvoice_UpdatedOn": since}
    logger.info("Coworker invoices: fetching updated since %s (%s-day window)", since, days)

    async with RunTracker("nexudus", "coworker_invoices", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("billing/coworkerinvoices", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("coworker_invoices", records, run_id)
        changed, run.rows_written = writer.write_coworker_invoices(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Coworker invoices: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
        )
        return changed


async def _sync_coworkers(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    force_full: bool = False,
) -> None:
    """Full coworker list via GET /spaces/coworkers (UpdatedSince watermark).

    Until 2026-06-10 coworkers were fetched per-ID from invoice CoworkerIds,
    which silently dropped anyone never invoiced (leads, team members billed
    through their company, event guests). The paginated list endpoint covers
    every coworker record; `force_full` skips the watermark for backfills.
    """
    extra_params = {} if force_full else _incremental_params("coworkers")
    if force_full:
        logger.info("Coworkers: force_full — fetching the complete list")
    async with RunTracker("nexudus", "coworkers", "bronze", metadata=str(run_id)) as run:
        records = await client.get_all("spaces/coworkers", extra_params=extra_params)
        run.rows_read = len(records)
        blob_path = blob_writer.write_snapshot("coworkers", records, run_id)
        changed, run.rows_written = writer.write_coworkers(records)
        run.rows_skipped = len(records) - len(changed)
        logger.info(
            "Coworkers: %s fetched, %s changed, %s skipped, %s written to bronze [blob=%s]",
            run.rows_read, len(changed), run.rows_skipped, run.rows_written, blob_path,
        )


async def _sync_coworker_invoice_lines(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    changed_invoices: list[dict],
) -> None:
    """
    Fetch line items for invoices that changed in the current run.

    Uses Nexudus filter CoworkerInvoiceLine_CoworkerInvoice={id} per invoice.
    Since changed_invoices is already scoped to the last N days by
    _sync_coworker_invoices, this only touches recent data.
    """
    if not changed_invoices:
        logger.info("Coworker invoice lines: no changed invoices, skipping")
        return

    invoice_ids = [int(r["Id"]) for r in changed_invoices if r.get("Id")]
    logger.info(
        "Coworker invoice lines: fetching lines for %s changed invoices",
        len(invoice_ids),
    )

    async with RunTracker("nexudus", "coworker_invoice_lines", "bronze", metadata=str(run_id)) as run:
        all_lines: list[dict] = []
        errors = 0

        for invoice_id in invoice_ids:
            try:
                lines = await client.get_coworker_invoice_lines(invoice_id)
                all_lines.extend(lines)
            except Exception as exc:
                logger.warning("Invoice lines for %s failed: %s", invoice_id, exc)
                errors += 1

        run.rows_read = len(invoice_ids)
        run.rows_skipped = errors
        if all_lines:
            blob_path = blob_writer.write_snapshot(
                "coworker_invoice_lines", all_lines, run_id
            )
            _changed, run.rows_written = writer.write_coworker_invoice_lines(
                all_lines
            )
        else:
            blob_path = "none"
            run.rows_written = 0

        logger.info(
            "Coworker invoice lines: %s invoices queried, %s lines fetched, "
            "%s written to bronze, %s errors [blob=%s]",
            len(invoice_ids),
            len(all_lines),
            run.rows_written,
            errors,
            blob_path,
        )


def _load_open_invoice_ids(lookback_months: int) -> list[int]:
    """Return open, unpaid invoices due from the recent window onward.

    Drives the by-ID re-fetch in _resync_open_invoices. Sourced from SILVER
    (not the raw bronze payload) so it inherits `is_deleted` — invoices removed
    in Nexudus are soft-deleted by nexudus_invoice_reconcile, so filtering them
    out here avoids a nightly flood of 404s re-fetching invoices that no longer
    exist. Unlike _load_invoice_history_candidate_ids there is NO
    direct-debit-only filter: a credit/payment on ANY invoice can be applied by
    Nexudus without bumping its UpdatedOn, so every open invoice needs
    refreshing. The lookback bounds the set so we never re-fetch ancient
    invoices; comfortably covers the finance worklist's 2026-03-01 floor.
    """
    sql = get_sql_client()
    rows = sql.execute_query(
        """
        SELECT source_id
        FROM silver.nexudus_coworker_invoices
        WHERE due_amount > 0
          AND ISNULL(paid, 0) = 0
          AND ISNULL(void, 0) = 0
          AND ISNULL(draft, 0) = 0
          AND ISNULL(is_deleted, 0) = 0
          AND due_date >= DATEADD(MONTH, -?, GETUTCDATE())
        ORDER BY source_id
        """,
        (lookback_months,),
    )
    return [int(row["source_id"]) for row in rows if row.get("source_id")]


async def _resync_open_invoices(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    entity_suffix: str = "",
) -> None:
    """Re-fetch every open unpaid invoice OBJECT by ID, independent of the 2-day
    UpdatedOn incremental window used by _sync_coworker_invoices.

    Why this exists: Nexudus applies a payment/credit to an invoice WITHOUT
    bumping its UpdatedOn (observed 2026-07-16 with UpdatedBy='[System]'). The
    incremental fetch is keyed on from_CoworkerInvoice_UpdatedOn, so once an
    invoice ages out of that window its DueAmount/PaidAmount freeze at the last
    in-window value and the finance dashboard keeps showing the full gross
    amount for an invoice that is really almost settled. Re-fetching the small
    set of open invoices by ID keeps their balances honest. The SHA-256 hash
    check in BronzeWriter.write_coworker_invoices means unchanged invoices cost
    one GET and no write.
    """
    lookback_months = int(os.getenv("NEXUDUS_INVOICE_RESYNC_LOOKBACK_MONTHS", "12"))
    invoice_ids = _load_open_invoice_ids(lookback_months)
    if not invoice_ids:
        logger.info("Open-invoice resync: no open invoices to refresh, skipping")
        return

    logger.info(
        "Open-invoice resync: re-fetching %s open invoices by ID (due within last %s months)",
        len(invoice_ids),
        lookback_months,
    )

    # `entity_suffix` keeps an off-cycle caller (the 07:00 pre-send refresh) from
    # masking the nightly run in the sync-health report, which keeps only the
    # latest run per (source_name, entity, layer).
    async with RunTracker(
        "nexudus", f"coworker_invoices_resync{entity_suffix}", "bronze", metadata=str(run_id)
    ) as run:
        tasks = [
            client.get_one(f"billing/coworkerinvoices/{invoice_id}")
            for invoice_id in invoice_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        records: list[dict] = []
        errors = 0
        for invoice_id, result in zip(invoice_ids, results):
            if isinstance(result, Exception):
                logger.warning("Open-invoice resync for %s failed: %s", invoice_id, result)
                errors += 1
                continue
            if result:
                records.append(result)

        run.rows_read = len(invoice_ids)
        run.rows_skipped = errors
        if records:
            blob_path = blob_writer.write_snapshot("coworker_invoices_resync", records, run_id)
            _changed, run.rows_written = writer.write_coworker_invoices(records)
        else:
            blob_path = "none"
            run.rows_written = 0

        logger.info(
            "Open-invoice resync: %s queried, %s fetched, %s written to bronze, %s errors [blob=%s]",
            len(invoice_ids),
            len(records),
            run.rows_written,
            errors,
            blob_path,
        )


def _load_invoice_history_candidate_ids(lookback_months: int) -> list[int]:
    """Return unpaid direct-debit invoices due from the recent window onward."""
    sql = get_sql_client()
    rows = sql.execute_query(
        """
        WITH latest_invoice AS (
            SELECT source_id, MAX(synced_at) AS latest
            FROM bronze.nexudus_coworker_invoices
            GROUP BY source_id
        ),
        latest_payload AS (
            SELECT b.source_id, b.raw_json
            FROM bronze.nexudus_coworker_invoices b
            INNER JOIN latest_invoice latest
                ON latest.source_id = b.source_id
               AND latest.latest = b.synced_at
        )
        SELECT source_id
        FROM latest_payload
        WHERE TRY_CONVERT(
                  DATETIME2,
                  REPLACE(JSON_VALUE(raw_json, '$.DueDate'), 'Z', ''),
                  126
              ) >= DATEADD(MONTH, -?, GETUTCDATE())
          AND ISNULL(TRY_CONVERT(FLOAT, JSON_VALUE(raw_json, '$.DueAmount')), 0) > 0
          AND ISNULL(JSON_VALUE(raw_json, '$.Paid'), 'false') <> 'true'
          AND ISNULL(JSON_VALUE(raw_json, '$.Void'), 'false') <> 'true'
          AND ISNULL(JSON_VALUE(raw_json, '$.Draft'), 'false') <> 'true'
          -- Nexudus's CreditNote flag is unreliable (it gets set on normal
          -- DD invoices like QH-INV-2026.05-0711). We mirror the worklist
          -- procs and skip the credit_note filter here so flagged-but-real
          -- DD invoices still get their payment-result history fetched.
          AND (
              JSON_VALUE(raw_json, '$.CoworkerEnableGoCardlessPayments') = 'true'
              OR JSON_VALUE(raw_json, '$.GoCardlessReference') IS NOT NULL
              OR JSON_VALUE(raw_json, '$.CoworkerRegularPaymentProvider') IS NOT NULL
          )
        ORDER BY source_id
        """,
        (lookback_months,),
    )
    return [int(row["source_id"]) for row in rows if row.get("source_id")]


async def _sync_coworker_invoice_histories(
    client: NexudusClient,
    blob_writer: BlobWriter,
    writer: BronzeWriter,
    run_id: uuid.UUID,
    entity_suffix: str = "",
    lookback_months: int | None = None,
) -> None:
    if lookback_months is None:
        lookback_months = int(os.getenv("NEXUDUS_INVOICE_HISTORY_LOOKBACK_MONTHS", "1"))
    invoice_ids = _load_invoice_history_candidate_ids(lookback_months)
    if not invoice_ids:
        logger.info("Coworker invoice histories: no candidate invoices, skipping")
        return

    # NB "direct-debit invoices" is a misnomer inherited from the candidate query:
    # its `CoworkerRegularPaymentProvider IS NOT NULL` clause is true for
    # 31,451/31,452 coworkers (it is the location's default gateway, not a member
    # mandate), so this is effectively every unpaid non-void invoice in the window.
    logger.info(
        "Coworker invoice histories: fetching histories for %s unpaid invoices due from last %s month(s)",
        len(invoice_ids),
        lookback_months,
    )

    # `entity_suffix` keeps an off-cycle caller (the 07:00 pre-send refresh) from
    # masking the nightly run in the sync-health report, which keeps only the
    # latest run per (source_name, entity, layer).
    async with RunTracker(
        "nexudus", f"coworker_invoice_histories{entity_suffix}", "bronze", metadata=str(run_id)
    ) as run:
        all_histories: list[dict] = []
        errors = 0
        tasks = [client.get_coworker_invoice_histories(invoice_id) for invoice_id in invoice_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for invoice_id, result in zip(invoice_ids, results):
            if isinstance(result, Exception):
                logger.warning("Invoice histories for %s failed: %s", invoice_id, result)
                errors += 1
                continue
            all_histories.extend(result)

        run.rows_read = len(invoice_ids)
        run.rows_skipped = errors
        if all_histories:
            blob_path = blob_writer.write_snapshot(
                "coworker_invoice_histories", all_histories, run_id
            )
            _changed, run.rows_written = writer.write_coworker_invoice_histories(
                all_histories
            )
        else:
            blob_path = "none"
            run.rows_written = 0

        logger.info(
            "Coworker invoice histories: %s invoices queried, %s history rows fetched, "
            "%s written to bronze, %s errors [blob=%s]",
            len(invoice_ids),
            len(all_histories),
            run.rows_written,
            errors,
            blob_path,
        )
