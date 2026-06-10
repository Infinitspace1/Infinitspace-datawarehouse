"""
scripts/python_scripts/inspect_nexudus_events.py

Discovery utility: fetch a few records from the Nexudus event endpoints
(calendarevents, eventattendees, eventproducts) and print their field
names + sample values so transformer mappings can be verified against
real payloads.

Usage:
    .\\venv\\Scripts\\python.exe scripts\\python_scripts\\inspect_nexudus_events.py
    .\\venv\\Scripts\\python.exe scripts\\python_scripts\\inspect_nexudus_events.py --limit 5 --raw
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient

ENDPOINTS = {
    "calendar_events": "content/calendarevents",
    "event_attendees": "content/eventattendees",
    "event_products": "content/eventproducts",
}


def _truncate(value, width: int = 80) -> str:
    s = json.dumps(value, default=str, ensure_ascii=False)
    return s if len(s) <= width else s[: width - 3] + "..."


async def main(limit: int, raw: bool) -> None:
    token = get_bearer_token()
    async with NexudusClient(token) as client:
        for name, path in ENDPOINTS.items():
            print(f"\n{'=' * 70}\n{name}  (GET /api/{path})\n{'=' * 70}")
            try:
                data = await client.get(path, {"page": 1, "size": limit})
            except Exception as exc:
                print(f"  FAILED: {exc}")
                continue

            records = data.get("Records", []) if isinstance(data, dict) else data
            total = data.get("TotalItems") if isinstance(data, dict) else None
            print(f"  records on page: {len(records)}  total: {total}")

            if not records:
                continue

            if raw:
                for r in records:
                    print(json.dumps(r, indent=2, default=str, ensure_ascii=False))
                continue

            # Field inventory across the sample: name -> example value
            fields: dict = {}
            for r in records:
                for k, v in r.items():
                    if k not in fields or fields[k] is None:
                        fields[k] = v
            for k in sorted(fields):
                print(f"  {k:45s} {_truncate(fields[k])}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--raw", action="store_true", help="dump full JSON records")
    args = parser.parse_args()
    asyncio.run(main(args.limit, args.raw))
