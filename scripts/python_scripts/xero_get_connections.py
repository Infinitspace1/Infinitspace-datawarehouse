"""
scripts/python_scripts/xero_get_connections.py

Fetch live Xero /connections output using the stored connection.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from shared.xero.client import XeroApiClient

load_dotenv(ROOT / ".env")


def main() -> None:
    parser = argparse.ArgumentParser(description="Get live Xero connections")
    parser.add_argument("--owner-type", default="workspace")
    parser.add_argument("--owner-id", default="default")
    args = parser.parse_args()

    client = XeroApiClient(
        owner_type=args.owner_type,
        owner_id=args.owner_id,
    )
    connections = client.get_connections()
    print(json.dumps(
        {"connection_count": len(connections), "connections": connections},
        indent=2,
        default=str,
    ))


if __name__ == "__main__":
    main()
