"""
scripts/python_scripts/xero_refresh_token.py

Refresh the Xero access token using the stored refresh token.
Updates XERO_REFRESH_TOKEN in .env with the new rotated token.

Xero refresh tokens rotate on every use and expire after 60 days of non-use.
The Azure Functions pipeline handles refresh automatically via shared/xero/oauth.py.
Use this script only for manual testing or to verify the token is still valid.

Usage:
    python scripts/python_scripts/xero_refresh_token.py
"""
import os
import sys
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

XERO_TOKEN_URL = "https://identity.xero.com/connect/token"
XERO_CONNECTIONS_URL = "https://api.xero.com/connections"

client_id = os.getenv("XERO_CLIENT_ID")
client_secret = os.getenv("XERO_CLIENT_SECRET")
refresh_token = os.getenv("XERO_REFRESH_TOKEN")

if not client_id:
    raise EnvironmentError("XERO_CLIENT_ID not set in .env")
if not client_secret:
    raise EnvironmentError("XERO_CLIENT_SECRET not set in .env")
if not refresh_token:
    raise EnvironmentError(
        "XERO_REFRESH_TOKEN not set in .env — run xero_exchange_code.py first"
    )

print(f"Using refresh token: {refresh_token[:20]}...")

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

tokens = resp.json()
new_access_token = tokens["access_token"]
new_refresh_token = tokens["refresh_token"]

print(f"New access_token : {new_access_token[:20]}...")
print(f"New refresh_token: {new_refresh_token[:20]}...")
print(f"Expires in       : {tokens.get('expires_in')}s")

# Verify by fetching tenants
conn_resp = requests.get(
    XERO_CONNECTIONS_URL,
    headers={"Authorization": f"Bearer {new_access_token}", "Accept": "application/json"},
    timeout=30,
)
if conn_resp.ok:
    tenants = conn_resp.json()
    print(f"\nConnected tenants ({len(tenants)}):")
    for t in tenants:
        print(f"  - [{t.get('tenantType')}] {t.get('tenantName')} | {t.get('tenantId')}")
else:
    print(f"Warning: could not fetch tenants ({conn_resp.status_code})")

# Update .env with rotated refresh token
env_path = ROOT / ".env"
env_text = env_path.read_text(encoding="utf-8")
lines = env_text.splitlines()
lines = [
    f"XERO_REFRESH_TOKEN={new_refresh_token}" if l.startswith("XERO_REFRESH_TOKEN=") else l
    for l in lines
]
env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"\nUpdated XERO_REFRESH_TOKEN in .env")
