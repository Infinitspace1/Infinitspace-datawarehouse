"""
scripts/python_scripts/xero_test_contacts.py

Fetch contacts using stored Xero connection + tenant.
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
    parser = argparse.ArgumentParser(description="Call Xero Contacts API")
    parser.add_argument("--owner-type", default="workspace")
    parser.add_argument("--owner-id", default="default")
    parser.add_argument("--tenant-id")
    parser.add_argument("--summary-only", action="store_true")
    args = parser.parse_args()

    client = XeroApiClient(
        owner_type=args.owner_type,
        owner_id=args.owner_id,
        tenant_id=args.tenant_id,
    )
    contacts = client.get_contacts(summary_only=args.summary_only)
    print(json.dumps(contacts, indent=2, default=str))


if __name__ == "__main__":
    main()
