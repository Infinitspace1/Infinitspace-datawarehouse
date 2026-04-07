"""
scripts/python_scripts/xero_exchange_code.py

Step 2 of Xero OAuth: exchange the authorization code for tokens, fetch all
connected tenants, and save the refresh token to .env for future use.

Usage:
    python scripts/python_scripts/xero_exchange_code.py "<full redirect URL>"
    python scripts/python_scripts/xero_exchange_code.py  # prompts for input
"""
import json
import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

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
redirect_uri = os.getenv("XERO_REDIRECT_URI", "https://xero.com")

if not client_id:
    raise EnvironmentError("XERO_CLIENT_ID not set in .env")
if not client_secret:
    raise EnvironmentError("XERO_CLIENT_SECRET not set in .env")

# --- Accept full redirect URL or just the code ---
if len(sys.argv) >= 2:
    raw_input = " ".join(sys.argv[1:]).strip()
else:
    print("Paste the full redirect URL from the browser (https://xero.com/?code=...):")
    raw_input = input("> ").strip()

# Extract code from URL if a full URL was pasted
if raw_input.startswith("http"):
    parsed = urlparse(raw_input)
    qs = parse_qs(parsed.query)
    code = qs.get("code", [None])[0]
    if not code:
        print(f"ERROR: No 'code' parameter found in URL: {raw_input}")
        sys.exit(1)
else:
    code = raw_input  # assume raw code was pasted

print(f"\nCode     : {code[:12]}...")
print(f"Client ID: {client_id}")
print(f"Redirect : {redirect_uri}")

# --- Step 1: Exchange code for tokens ---
print("\n[1/3] Exchanging code for tokens...")
resp = requests.post(
    XERO_TOKEN_URL,
    data={
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    },
    auth=HTTPBasicAuth(client_id, client_secret),
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    timeout=30,
)

if not resp.ok:
    print(f"ERROR {resp.status_code}: {resp.text}")
    sys.exit(1)

tokens = resp.json()
access_token = tokens["access_token"]
refresh_token = tokens["refresh_token"]
print(f"  access_token : {access_token[:20]}...")
print(f"  refresh_token: {refresh_token[:20]}...")
print(f"  expires_in   : {tokens.get('expires_in')}s")
print(f"  scope        : {tokens.get('scope')}")

# --- Step 2: Fetch all connected tenants ---
print("\n[2/3] Fetching connected tenants...")
conn_resp = requests.get(
    XERO_CONNECTIONS_URL,
    headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
    timeout=30,
)

if not conn_resp.ok:
    print(f"ERROR {conn_resp.status_code}: {conn_resp.text}")
    sys.exit(1)

tenants = conn_resp.json()
print(f"  {len(tenants)} tenant(s) connected:")
for t in tenants:
    print(f"    - [{t.get('tenantType')}] {t.get('tenantName')} | id={t.get('tenantId')}")

# --- Step 3: Save refresh token to .env ---
print("\n[3/3] Saving refresh token to .env...")
env_path = ROOT / ".env"
env_text = env_path.read_text(encoding="utf-8")

refresh_line = f"XERO_REFRESH_TOKEN={refresh_token}"

if "XERO_REFRESH_TOKEN=" in env_text:
    # Replace existing line
    lines = env_text.splitlines()
    lines = [refresh_line if l.startswith("XERO_REFRESH_TOKEN=") else l for l in lines]
    env_text = "\n".join(lines) + "\n"
else:
    # Append after XERO_CLIENT_SECRET line
    env_text += f"\n# Xero OAuth refresh token (rotates on each refresh — update after every use)\n{refresh_line}\n"

env_path.write_text(env_text, encoding="utf-8")
print(f"  Saved XERO_REFRESH_TOKEN to .env")

# --- Summary ---
print("\n" + "=" * 70)
print("SUCCESS — Xero OAuth complete")
print("=" * 70)
print(f"Tenants connected : {len(tenants)}")
print(f"Refresh token     : {refresh_token[:20]}... (saved to .env)")
print()
print("Next steps:")
print("  - Run xero_sync_invoices.py to pull invoices from all tenants")
print("  - The refresh token rotates every time it is used — .env is updated automatically")
print("  - Refresh tokens expire after 60 days of non-use")
print("=" * 70)
