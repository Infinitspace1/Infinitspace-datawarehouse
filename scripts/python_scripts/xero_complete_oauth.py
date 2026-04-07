"""
Complete the Xero OAuth flow and persist the connection in SQL.

Usage:
  python scripts/python_scripts/xero_complete_oauth.py --redirect-url "https://xero.com/?code=...&state=..."
  python scripts/python_scripts/xero_complete_oauth.py --code <code> --state <state>
  python scripts/python_scripts/xero_complete_oauth.py

This is the supported non-HTTP replacement for the old callback endpoint.
It stores encrypted tokens in meta.xero_connections and tenants in meta.xero_tenants.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

from shared.xero.flow import XeroAuthFlowService


def _extract_from_redirect_url(redirect_url: str) -> tuple[str | None, str | None]:
    parsed = urlparse(redirect_url.strip())
    query = parse_qs(parsed.query)
    code = query.get("code", [None])[0]
    state = query.get("state", [None])[0]
    return code, state


def _resolve_callback_inputs(args: argparse.Namespace) -> tuple[str, str]:
    if args.redirect_url:
        code, state = _extract_from_redirect_url(args.redirect_url)
    else:
        code = args.code
        state = args.state

    if code and state:
        return code, state

    print("Paste the full redirect URL from Xero, including both code and state:")
    redirect_url = input("> ").strip()
    code, state = _extract_from_redirect_url(redirect_url)
    if not code or not state:
        raise SystemExit("Redirect URL must include both code and state query parameters")
    return code, state


def main() -> None:
    parser = argparse.ArgumentParser(description="Complete Xero OAuth and persist connection in SQL")
    parser.add_argument("--redirect-url", help="Full redirect URL returned by Xero after consent")
    parser.add_argument("--code", help="OAuth authorization code")
    parser.add_argument("--state", help="OAuth state value created by xero_start_oauth.py")
    args = parser.parse_args()

    code, state = _resolve_callback_inputs(args)
    flow = XeroAuthFlowService()
    result = flow.handle_callback(code=code, state=state)

    print(json.dumps(result, indent=2, default=str))
    print()
    print("Connection stored successfully.")
    print("Next step: run xero_sync_invoices.py to verify the DB-backed refresh path.")


if __name__ == "__main__":
    main()
