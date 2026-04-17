"""
scripts/test_local.py

Run and test each bronze sync step locally, one at a time.
Connects to your real Azure SQL and real Nexudus API.

Usage:
    python scripts/test_local.py --step auth
    python scripts/test_local.py --step locations
    python scripts/test_local.py --step products
    python scripts/test_local.py --step contracts
    python scripts/test_local.py --step resources
    python scripts/test_local.py --step extra_services
    python scripts/test_local.py --step all

Options:
    --dry-run       Fetch from Nexudus but do NOT write to SQL
    --limit N       Only process first N records (useful for contracts/resources)
    --step STEP     Which step to run (default: all)
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from pathlib import Path

# ── make shared/ importable regardless of where you run from ──
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_local")


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _print_section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def _print_record_sample(records: list[dict], n: int = 2):
    """Print first n records prettily."""
    for i, r in enumerate(records[:n]):
        print(f"\n  Sample record [{i+1}]:")
        for line in json.dumps(r, default=str, indent=4).splitlines():
            print(f"    {line}")


def _print_result(fetched: int, written: int, dry_run: bool):
    status = "✅" if fetched > 0 else "⚠️ "
    write_msg = f"written to bronze: {written}" if not dry_run else "dry-run (no writes)"
    print(f"\n  {status}  fetched: {fetched}  |  {write_msg}")


# ──────────────────────────────────────────────────────────────
# Step 1 — Auth
# ──────────────────────────────────────────────────────────────

def test_auth():
    _print_section("STEP: auth")
    from shared.nexudus.auth import get_bearer_token

    try:
        token = get_bearer_token()
        masked = token[:12] + "..." + token[-6:] if len(token) > 20 else token
        print(f"\n  ✅  Token obtained: {masked}")
        return token
    except Exception as e:
        print(f"\n  ❌  Auth failed: {e}")
        raise


# ──────────────────────────────────────────────────────────────
# Step 2 — Locations
# ──────────────────────────────────────────────────────────────

async def test_locations(token: str, dry_run: bool, limit: int, run_id: uuid.UUID):
    _print_section("STEP: locations")
    from shared.nexudus.client import NexudusClient
    from shared.azure_clients.bronze_writer import BronzeWriter

    async with NexudusClient(token) as client:
        records = await client.get_all("sys/businesses")

    if limit:
        records = records[:limit]

    print(f"\n  Fetched: {len(records)} locations")
    _print_record_sample(records)

    written = 0
    if not dry_run and records:
        writer = BronzeWriter(run_id)
        _changed, written = writer.write_locations(records)

    _print_result(len(records), written, dry_run)
    return records


# ──────────────────────────────────────────────────────────────
# Step 3 — Products (FloorPlanDesks)
# ──────────────────────────────────────────────────────────────

async def test_products(token: str, dry_run: bool, limit: int, run_id: uuid.UUID):
    _print_section("STEP: products (floorplandesks)")
    from shared.nexudus.client import NexudusClient
    from shared.azure_clients.bronze_writer import BronzeWriter

    async with NexudusClient(token) as client:
        records = await client.get_all("sys/floorplandesks")

    if limit:
        records = records[:limit]

    # Show ItemType distribution
    type_counts: dict = {}
    for r in records:
        t = r.get("ItemType", "?")
        type_counts[t] = type_counts.get(t, 0) + 1

    print(f"\n  Fetched: {len(records)} products")
    print(f"  ItemType distribution: {type_counts}")

    # Show how many have ResourceId (needed for extra services)
    with_resource = sum(1 for r in records if r.get("ResourceId"))
    print(f"  Records with ResourceId: {with_resource}")

    _print_record_sample(records)

    written = 0
    if not dry_run and records:
        writer = BronzeWriter(run_id)
        _changed, written = writer.write_products(records)

    _print_result(len(records), written, dry_run)
    return records


# ──────────────────────────────────────────────────────────────
# Step 4 — Contracts
# ──────────────────────────────────────────────────────────────

async def test_contracts(token: str, dry_run: bool, limit: int, run_id: uuid.UUID):
    _print_section("STEP: contracts (coworkercontracts)")
    from shared.nexudus.client import NexudusClient
    from shared.azure_clients.bronze_writer import BronzeWriter

    async with NexudusClient(token) as client:
        records = await client.get_all("billing/coworkercontracts")

    if limit:
        records = records[:limit]

    # Show active vs inactive
    active = sum(1 for r in records if r.get("Active") or r.get("active"))
    print(f"\n  Fetched: {len(records)} contracts")
    print(f"  Active: {active}  |  Inactive: {len(records) - active}")

    _print_record_sample(records)

    written = 0
    if not dry_run and records:
        writer = BronzeWriter(run_id)
        _changed, written = writer.write_contracts(records)

    _print_result(len(records), written, dry_run)
    return records


# ──────────────────────────────────────────────────────────────
# Step 5 — Resources
# Needs products to be fetched first to know which ResourceIds exist
# ──────────────────────────────────────────────────────────────

async def test_resources(token: str, dry_run: bool, limit: int, run_id: uuid.UUID):
    _print_section("STEP: resources")
    from shared.nexudus.client import NexudusClient
    from shared.azure_clients.bronze_writer import BronzeWriter

    # First fetch products to get ResourceIds
    print("  Fetching products to collect ResourceIds...")
    async with NexudusClient(token) as client:
        products = await client.get_all("sys/floorplandesks")

    resource_ids = list({
        r["ResourceId"]
        for r in products
        if r.get("ResourceId")
    })

    print(f"  Unique ResourceIds found in products: {len(resource_ids)}")

    if not resource_ids:
        print("  ⚠️  No ResourceIds found. Nothing to fetch.")
        return []

    if limit:
        resource_ids = resource_ids[:limit]
        print(f"  Limited to first {limit} resource IDs")

    # Fetch each resource
    print(f"  Fetching {len(resource_ids)} resources...")
    async with NexudusClient(token) as client:
        tasks = [client.get_one(f"spaces/resources/{rid}") for rid in resource_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    records = []
    errors = 0
    for rid, result in zip(resource_ids, results):
        if isinstance(result, Exception):
            print(f"  ⚠️  Resource {rid}: {result}")
            errors += 1
        elif result:
            records.append(result)

    # Show GroupName/ResourceTypeId distribution
    group_counts: dict = {}
    for r in records:
        g = r.get("GroupName") or r.get("ResourceTypeName") or "?"
        group_counts[g] = group_counts.get(g, 0) + 1

    print(f"\n  Fetched: {len(records)} resources  |  Errors: {errors}")
    print(f"  GroupName distribution: {group_counts}")
    _print_record_sample(records)

    written = 0
    if not dry_run and records:
        writer = BronzeWriter(run_id)
        for record in records:
            _changed, w = writer.write_resources([record])
            written += w

    _print_result(len(records), written, dry_run)
    return records


# ──────────────────────────────────────────────────────────────
# Step 6 — Extra Services
# ──────────────────────────────────────────────────────────────

async def test_extra_services(token: str, dry_run: bool, limit: int, run_id: uuid.UUID):
    _print_section("STEP: extra_services")
    from shared.nexudus.client import NexudusClient
    from shared.azure_clients.bronze_writer import BronzeWriter

    async with NexudusClient(token) as client:
        records = await client.get_all("billing/extraservices")

    if limit:
        records = records[:limit]

    # Show which fields carry pricing info
    with_fixed_cost = sum(1 for r in records if r.get("FixedCostPrice"))
    with_resource   = sum(1 for r in records if r.get("ResourceId") or r.get("ResourceTypeId"))
    print(f"\n  Fetched: {len(records)} extra services")
    print(f"  With FixedCostPrice: {with_fixed_cost}")
    print(f"  With ResourceId/ResourceTypeId: {with_resource}")

    _print_record_sample(records)

    written = 0
    if not dry_run and records:
        writer = BronzeWriter(run_id)
        _changed, written = writer.write_extra_services(records)

    _print_result(len(records), written, dry_run)
    return records


# ──────────────────────────────────────────────────────────────
# Step 7 — Coworker Invoice Lines (bulk UpdatedSince)
# ──────────────────────────────────────────────────────────────

async def test_invoice_lines(token: str, dry_run: bool, limit: int, run_id: uuid.UUID):
    _print_section("STEP: invoice_lines (via changed invoices)")
    import time
    from datetime import datetime, timedelta, timezone
    from shared.nexudus.client import NexudusClient
    from shared.azure_clients.bronze_writer import BronzeWriter

    lookback_days = 2
    since = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    # Step 1: fetch invoices updated in last 3 days
    print(f"\n  Step 1: Fetching invoices updated since {since}...")
    t0 = time.perf_counter()
    async with NexudusClient(token) as client:
        invoices = await client.get_all(
            "billing/coworkerinvoices",
            extra_params={"from_CoworkerInvoice_UpdatedOn": since},
        )
        t1 = time.perf_counter()
        print(f"  Invoices fetched: {len(invoices)} in {t1 - t0:.1f}s")

        # Step 2: fetch lines for those invoices
        invoice_ids = [int(r["Id"]) for r in invoices if r.get("Id")]
        if limit:
            invoice_ids = invoice_ids[:limit]

        print(f"\n  Step 2: Fetching lines for {len(invoice_ids)} invoices...")
        all_lines = []
        errors = 0
        for inv_id in invoice_ids:
            try:
                lines = await client.get_coworker_invoice_lines(inv_id)
                all_lines.extend(lines)
            except Exception as exc:
                print(f"  ⚠️  Invoice {inv_id}: {exc}")
                errors += 1

    elapsed = time.perf_counter() - t0
    print(f"\n  Fetched: {len(all_lines)} invoice lines from {len(invoice_ids)} invoices in {elapsed:.1f}s")
    if errors:
        print(f"  Errors: {errors}")
    _print_record_sample(all_lines)

    written = 0
    if not dry_run and all_lines:
        writer = BronzeWriter(run_id)
        _changed, written = writer.write_coworker_invoice_lines(all_lines)

    _print_result(len(all_lines), written, dry_run)
    return all_lines


# ──────────────────────────────────────────────────────────────
# Step 8 — PDF Cache Query (check how many invoices match)
# ──────────────────────────────────────────────────────────────

def test_pdf_cache_query():
    _print_section("STEP: pdf_cache_query (check scope)")
    from shared.azure_clients.sql_client import get_sql_client

    sql = get_sql_client()

    # Old query (all missing)
    old_count = sql.execute_scalar(
        """
        SELECT COUNT(*)
        FROM silver.nexudus_coworker_invoices
        WHERE pdf_blob_path IS NULL
        """
    )

    # New query (updated_on last 2 days only)
    new_count = sql.execute_scalar(
        """
        SELECT COUNT(*)
        FROM silver.nexudus_coworker_invoices
        WHERE pdf_blob_path IS NULL
          AND updated_on >= DATEADD(DAY, -2, GETUTCDATE())
        """
    )

    # Unavailable sentinel count
    unavailable = sql.execute_scalar(
        """
        SELECT COUNT(*)
        FROM silver.nexudus_coworker_invoices
        WHERE pdf_blob_path = '__unavailable__'
        """
    )

    print(f"\n  Old query (all NULL pdf_blob_path):        {old_count}")
    print(f"  New query (NULL + last 2 days):             {new_count}")
    print(f"  Already marked __unavailable__:             {unavailable}")
    print(f"  Reduction: {old_count} -> {new_count} ({100 * (1 - (new_count or 0) / max(old_count or 1, 1)):.0f}% fewer)")


# ──────────────────────────────────────────────────────────────
# SQL connectivity check
# ──────────────────────────────────────────────────────────────

def test_sql():
    _print_section("STEP: sql connection")
    from shared.azure_clients.sql_client import get_sql_client

    try:
        sql = get_sql_client()
        result = sql.execute_scalar("SELECT @@VERSION")
        print(f"\n  ✅  Connected to SQL Server")
        print(f"  Version: {str(result)[:80]}...")

        # Check bronze tables exist
        tables = sql.execute_query("""
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS full_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'bronze'
            ORDER BY TABLE_NAME
        """)
        if tables:
            print(f"\n  Bronze tables found:")
            for t in tables:
                print(f"    • {t['full_name']}")
        else:
            print("\n  ⚠️  No bronze tables found — run sql/02_bronze.sql first")

    except Exception as e:
        print(f"\n  ❌  SQL connection failed: {e}")
        raise


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────

STEPS = ["auth", "sql", "locations", "products", "contracts", "resources", "extra_services", "invoice_lines", "pdf_cache_query"]

async def main():
    parser = argparse.ArgumentParser(description="Test Nexudus → Bronze pipeline locally")
    parser.add_argument(
        "--step",
        choices=STEPS + ["all"],
        default="all",
        help="Which step to test (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from Nexudus but skip writing to SQL",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max records to process per step (0 = no limit)",
    )
    args = parser.parse_args()

    dry_run = args.dry_run
    limit   = args.limit
    step    = args.step
    run_id  = uuid.uuid4()

    if dry_run:
        print("\n  🔵  DRY RUN — no data will be written to SQL")
    print(f"\n  Run ID: {run_id}")

    steps_to_run = STEPS if step == "all" else [step]

    # Auth always runs first (needed for API steps)
    token = None
    if any(s not in ("auth", "sql") for s in steps_to_run):
        token = test_auth()
    elif "auth" in steps_to_run:
        token = test_auth()

    for s in steps_to_run:
        if s == "auth":
            if token is None:
                token = test_auth()
        elif s == "sql":
            test_sql()
        elif s == "locations":
            await test_locations(token, dry_run, limit, run_id)
        elif s == "products":
            await test_products(token, dry_run, limit, run_id)
        elif s == "contracts":
            await test_contracts(token, dry_run, limit, run_id)
        elif s == "resources":
            await test_resources(token, dry_run, limit, run_id)
        elif s == "extra_services":
            await test_extra_services(token, dry_run, limit, run_id)
        elif s == "invoice_lines":
            await test_invoice_lines(token, dry_run, limit, run_id)
        elif s == "pdf_cache_query":
            test_pdf_cache_query()

    print(f"\n{'─'*60}")
    print(f"  Done. Run ID: {run_id}")
    if not dry_run:
        print(f"  Check meta.sync_runs or bronze.* tables for results")
    print(f"{'─'*60}\n")


if __name__ == "__main__":
    asyncio.run(main())