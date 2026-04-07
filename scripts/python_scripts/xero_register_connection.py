"""
scripts/python_scripts/xero_register_connection.py

Bootstrap: registers the Xero OAuth tokens from .env into the database
(meta.xero_connections + meta.xero_tenants) so the pipeline can use them.

Run this ONCE after completing the OAuth flow with xero_exchange_code.py.
After this, the Azure Functions pipeline manages tokens automatically via DB.

Usage:
    python scripts/python_scripts/xero_register_connection.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import os
from shared.xero.oauth import TokenSet
from shared.xero.store import XeroStore

XERO_TOKEN_URL = "https://identity.xero.com/connect/token"
XERO_CONNECTIONS_URL = "https://api.xero.com/connections"

client_id = os.getenv("XERO_CLIENT_ID")
client_secret = os.getenv("XERO_CLIENT_SECRET")
refresh_token = os.getenv("XERO_REFRESH_TOKEN")

if not all([client_id, client_secret, refresh_token]):
    raise EnvironmentError("XERO_CLIENT_ID, XERO_CLIENT_SECRET and XERO_REFRESH_TOKEN must be set in .env")

# --- Step 1: Refresh to get a valid access token ---
print("Refreshing access token...")
resp = requests.post(
    XERO_TOKEN_URL,
    data={"grant_type": "refresh_token", "refresh_token": refresh_token},
    auth=HTTPBasicAuth(client_id, client_secret),
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    timeout=30,
)
if not resp.ok:
    print(f"ERROR {resp.status_code}: {resp.text}")
    sys.exit(1)

raw_tokens = resp.json()
access_token = raw_tokens["access_token"]
new_refresh_token = raw_tokens["refresh_token"]
expires_in = int(raw_tokens.get("expires_in", 1800))

token_set = TokenSet(
    access_token=access_token,
    refresh_token=new_refresh_token,
    id_token=raw_tokens.get("id_token"),
    token_type=raw_tokens.get("token_type", "Bearer"),
    scope=raw_tokens.get("scope"),
    expires_at=datetime.now(timezone.utc) + timedelta(seconds=expires_in),
    raw_payload=raw_tokens,
    xero_user_id=raw_tokens.get("xero_userid"),
)
print(f"  access_token : {access_token[:20]}...")
print(f"  refresh_token: {new_refresh_token[:20]}...")

# --- Step 2: Fetch connected tenants from Xero ---
print("\nFetching tenants from Xero API...")
tenant_resp = requests.get(
    XERO_CONNECTIONS_URL,
    headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
    timeout=30,
)
tenant_resp.raise_for_status()
tenants = tenant_resp.json()
print(f"  {len(tenants)} tenant(s) found:")
for t in tenants:
    print(f"    - [{t.get('tenantType')}] {t.get('tenantName')} | {t.get('tenantId')}")

# --- Step 3: Upsert connection + tenants into DB ---
print("\nRegistering connection in database...")
store = XeroStore()
connection_id = store.upsert_connection(
    owner_type="workspace",
    owner_id="default",
    token_set=token_set,
    selected_tenant_id=tenants[0]["tenantId"] if tenants else None,
)
print(f"  connection_id = {connection_id}")

store.save_tenants(connection_id=connection_id, tenants=tenants)
print(f"  Saved {len(tenants)} tenant(s) to meta.xero_tenants")

# --- Step 4: Rotate refresh token in .env ---
env_path = ROOT / ".env"
env_text = env_path.read_text(encoding="utf-8")
env_path.write_text(
    "\n".join(
        f"XERO_REFRESH_TOKEN={new_refresh_token}" if l.startswith("XERO_REFRESH_TOKEN=") else l
        for l in env_text.splitlines()
    ) + "\n",
    encoding="utf-8",
)
print("  Updated XERO_REFRESH_TOKEN in .env")

print("\n" + "=" * 60)
print("SUCCESS — Connection registered")
print("=" * 60)
print(f"  DB connection_id : {connection_id}")
print(f"  Tenants stored   : {len(tenants)}")
print()
print("Next step — run the first full migration:")
print("  python scripts/python_scripts/xero_sync_invoices.py --force-full")
