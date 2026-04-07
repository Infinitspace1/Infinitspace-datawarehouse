"""
scripts/python_scripts/xero_list_tenants.py

List stored Xero tenants for an owner or a connection id.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from shared.xero.store import XeroStore

load_dotenv(ROOT / ".env")


def main() -> None:
    parser = argparse.ArgumentParser(description="List Xero tenants")
    parser.add_argument("--owner-type", default="workspace")
    parser.add_argument("--owner-id", default="default")
    parser.add_argument("--connection-id", type=int)
    args = parser.parse_args()

    store = XeroStore()
    if args.connection_id is not None:
        tenants = store.list_tenants(connection_id=args.connection_id)
    else:
        tenants = store.list_tenants(owner_type=args.owner_type, owner_id=args.owner_id)

    print(json.dumps({"tenant_count": len(tenants), "tenants": tenants}, indent=2, default=str))


if __name__ == "__main__":
    main()
