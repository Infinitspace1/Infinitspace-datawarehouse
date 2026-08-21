"""
scripts/python_scripts/test_helpdesk_sync.py

Local runner for the Nexudus help desk ("customer requests") sync.

    # Dry run — fetch + transform samples, print counts, NO SQL writes
    .\venv\Scripts\python.exe scripts\python_scripts\test_helpdesk_sync.py

    # Full local run — bronze -> silver, incremental (24h lookback)
    .\venv\Scripts\python.exe scripts\python_scripts\test_helpdesk_sync.py --write

    # One-off backfill — full history, ignores the incremental window
    .\venv\Scripts\python.exe scripts\python_scripts\test_helpdesk_sync.py --write --full

Requires Nexudus creds; --write additionally needs SQL and the schema from
scripts/sql_scripts/nexudus_helpdesk_schema.sql to have been applied.
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# Ticket subjects contain emoji; force UTF-8 on a cp1252 console.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")

from shared.nexudus.auth import get_bearer_token                       # noqa: E402
from shared.nexudus.client import NexudusClient                        # noqa: E402
from shared.nexudus.helpdesk import COMMENTS, ENTITIES, MESSAGES       # noqa: E402
from shared.nexudus.transformers.helpdesk_comments import transform_helpdesk_comment       # noqa: E402
from shared.nexudus.transformers.helpdesk_departments import transform_helpdesk_department  # noqa: E402
from shared.nexudus.transformers.helpdesk_messages import transform_helpdesk_message        # noqa: E402

_TRANSFORMERS = {
    "helpdesk_messages": transform_helpdesk_message,
    "helpdesk_comments": transform_helpdesk_comment,
    "helpdesk_departments": transform_helpdesk_department,
}


def _preview(value, width: int = 70) -> str:
    s = json.dumps(value, default=str, ensure_ascii=False)
    return s if len(s) <= width else s[: width - 3] + "..."


async def dry_run() -> None:
    token = get_bearer_token()
    async with NexudusClient(token) as client:
        for key, entity in ENTITIES.items():
            print(f"\n{'=' * 72}\n{key}  (GET /api/{entity.endpoint})\n{'=' * 72}")
            data = await client.get(entity.endpoint, {"page": 1, "size": 3})
            records = data.get("Records", [])
            print(f"  TotalItems: {data.get('TotalItems')}   sample: {len(records)}")
            if not records:
                continue
            fn = _TRANSFORMERS[key]
            kwargs = {"location_source_id": 1376491118} if key == COMMENTS else {}
            row = fn(records[0], bronze_id=1, sync_run_id=str(uuid.uuid4()), **kwargs)
            print("  transformed sample:")
            for k in sorted(row):
                print(f"    {k:30s} = {_preview(row[k])}")

        # The incremental filter is the whole basis of the poll — prove it
        # still filters, because `UpdatedSince` silently does not.
        print(f"\n{'=' * 72}\nincremental filter sanity check\n{'=' * 72}")
        entity = ENTITIES[MESSAGES]
        full = await client.get(entity.endpoint, {"page": 1, "size": 1})
        since = await client.get(
            entity.endpoint,
            {"page": 1, "size": 1, entity.updated_filter: "2026-08-01T00:00:00Z"},
        )
        total, filtered = full.get("TotalItems"), since.get("TotalItems")
        verdict = "OK — filter works" if filtered != total else "BROKEN — filter ignored!"
        print(f"  unfiltered={total}  since 2026-08-01={filtered}  -> {verdict}")


async def write_run(full: bool) -> None:
    from shared.azure_clients.bronze_writer import BronzeWriter
    from functions.nexudus_helpdesk_sync import sync_helpdesk

    token = get_bearer_token()
    run_id = uuid.uuid4()
    print(f"{'FULL BACKFILL' if full else 'INCREMENTAL RUN'}  [run_id={run_id}]\n")
    async with NexudusClient(token) as client:
        totals = await sync_helpdesk(
            client, BronzeWriter(run_id), run_id,
            force_full=full, triggered_by="manual",
        )
    print("\nRESULTS")
    for entity_key, result in totals.items():
        if "error" in result:
            print(f"  {entity_key:22s} FAILED: {result['error']}")
        else:
            print(
                f"  {entity_key:22s} fetched={result['fetched']:6} "
                f"changed={result['changed']:6} "
                f"bronze={result['bronze_written']:6} "
                f"silver={result['silver_written']:6}"
            )

    from shared.azure_clients.sql_client import get_sql_client
    sql = get_sql_client()
    print("\nSILVER ROW COUNTS (is_deleted = 0)")
    for table in ("nexudus_helpdesk_messages",
                  "nexudus_helpdesk_comments",
                  "nexudus_helpdesk_departments"):
        n = sql.execute_scalar(f"SELECT COUNT(*) FROM silver.{table} WHERE is_deleted = 0")
        print(f"  silver.{table:32s} {n}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true",
                        help="actually write to bronze + silver (default: dry run)")
    parser.add_argument("--full", action="store_true",
                        help="with --write: ignore the incremental window and backfill everything")
    args = parser.parse_args()

    if args.write:
        asyncio.run(write_run(full=args.full))
    else:
        if args.full:
            print("--full only applies with --write; doing a dry run.\n")
        asyncio.run(dry_run())


if __name__ == "__main__":
    main()
