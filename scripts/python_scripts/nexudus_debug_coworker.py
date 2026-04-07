"""
scripts/python_scripts/nexudus_debug_coworker.py

Fetch and print a single Nexudus coworker payload plus parsed location access.
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient
from shared.nexudus.colleague_sync import parse_coworker_payload

load_dotenv(ROOT / ".env")


async def _run(coworker_id: int) -> None:
    token = get_bearer_token()
    async with NexudusClient(token) as client:
        payload = await client.get_coworker(coworker_id)

    if payload is None:
        raise SystemExit(f"Coworker {coworker_id} not found")

    parsed = parse_coworker_payload(payload)
    print(json.dumps(
        {
            "parsed": {
                "nexudus_coworker_id": parsed.nexudus_coworker_id,
                "team_business_id": parsed.team_business_id,
                "full_name": parsed.full_name,
                "first_name": parsed.first_name,
                "last_name": parsed.last_name,
                "email": parsed.email,
                "status": parsed.status,
                "accessible_businesses": [
                    {
                        "business_id": business.business_id,
                        "name": business.name,
                        "slug": business.slug,
                    }
                    for business in parsed.accessible_businesses
                ],
            },
            "raw_payload": payload,
        },
        indent=2,
        default=str,
    ))


def main() -> None:
    parser = argparse.ArgumentParser(description="Debug one Nexudus coworker payload")
    parser.add_argument("--coworker-id", type=int, required=True)
    args = parser.parse_args()
    asyncio.run(_run(args.coworker_id))


if __name__ == "__main__":
    main()
