"""
scripts/python_scripts/nexudus_sync_colleague_access.py

Run Nexudus colleague -> location access sync manually.

Usage:
  python scripts/python_scripts/nexudus_sync_colleague_access.py --coworker-ids 123,456
  python scripts/python_scripts/nexudus_sync_colleague_access.py --env-coworker-ids
"""
import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from shared.nexudus.colleague_sync import sync_coworker_access_blocking

load_dotenv(ROOT / ".env")


def _parse_ids(raw: str) -> list[int]:
    ids = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        ids.append(int(item))
    return ids


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Nexudus colleague location access")
    parser.add_argument(
        "--coworker-ids",
        help="Comma-separated Nexudus coworker ids, e.g. 123,456",
    )
    parser.add_argument(
        "--env-coworker-ids",
        action="store_true",
        help="Read ids from NEXUDUS_COWORKER_IDS env var",
    )
    args = parser.parse_args()

    raw_ids = None
    if args.coworker_ids:
        raw_ids = args.coworker_ids
    elif args.env_coworker_ids:
        raw_ids = os.getenv("NEXUDUS_COWORKER_IDS")

    if not raw_ids:
        parser.error("Provide --coworker-ids or --env-coworker-ids with NEXUDUS_COWORKER_IDS set")

    coworker_ids = _parse_ids(raw_ids)
    stats = sync_coworker_access_blocking(coworker_ids=coworker_ids)
    print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
