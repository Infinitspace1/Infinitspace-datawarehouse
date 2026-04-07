"""
scripts/python_scripts/xero_start_oauth.py

Creates a Xero OAuth state record and prints authorization URL.
Use this for manual debug flow.
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from shared.xero.flow import XeroAuthFlowService

load_dotenv(ROOT / ".env")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Xero OAuth authorization URL")
    parser.add_argument("--owner-type", default="workspace")
    parser.add_argument("--owner-id", default="default")
    args = parser.parse_args()

    flow = XeroAuthFlowService()
    result = flow.start_auth(owner_type=args.owner_type, owner_id=args.owner_id)
    print("Open this URL in a browser and complete login:")
    print(result["authorization_url"])


if __name__ == "__main__":
    main()
