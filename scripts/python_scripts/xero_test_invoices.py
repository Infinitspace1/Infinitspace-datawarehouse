"""
scripts/python_scripts/xero_test_invoices.py

Fetch 10 invoices directly from the Xero API for each connected tenant.
Uses the refresh token from .env — no database required.

Usage:
    python scripts/python_scripts/xero_test_invoices.py
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

XERO_TOKEN_URL = "https://identity.xero.com/connect/token"
XERO_CONNECTIONS_URL = "https://api.xero.com/connections"
XERO_INVOICES_URL = "https://api.xero.com/api.xro/2.0/Invoices"

client_id = os.getenv("XERO_CLIENT_ID")
client_secret = os.getenv("XERO_CLIENT_SECRET")
refresh_token = os.getenv("XERO_REFRESH_TOKEN")

if not all([client_id, client_secret, refresh_token]):
    raise EnvironmentError("XERO_CLIENT_ID, XERO_CLIENT_SECRET and XERO_REFRESH_TOKEN must be set in .env")

# --- Refresh access token ---
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

tokens = resp.json()
access_token = tokens["access_token"]
new_refresh_token = tokens["refresh_token"]

# Rotate refresh token in .env
env_path = ROOT / ".env"
env_text = env_path.read_text(encoding="utf-8")
env_path.write_text(
    "\n".join(
        f"XERO_REFRESH_TOKEN={new_refresh_token}" if l.startswith("XERO_REFRESH_TOKEN=") else l
        for l in env_text.splitlines()
    ) + "\n",
    encoding="utf-8",
)
print("Token refreshed OK\n")

# --- Get connected tenants ---
tenants = requests.get(
    XERO_CONNECTIONS_URL,
    headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
    timeout=30,
).json()

print(f"Connected tenants: {len(tenants)}")
for t in tenants:
    print(f"  - {t['tenantName']} ({t['tenantId']})")
print()


def parse_xero_date(raw: str) -> str:
    """Convert /Date(1234567890000+0000)/ to YYYY-MM-DD."""
    digits = raw.replace("/Date(", "").replace(")/", "").split("+")[0].split("-")[0]
    if digits.lstrip("-").isdigit():
        return datetime.fromtimestamp(int(digits) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return raw[:10]


# --- Fetch 10 invoices per tenant ---
for tenant in tenants:
    tenant_id = tenant["tenantId"]
    tenant_name = tenant["tenantName"]

    print("=" * 70)
    print(f"  TENANT : {tenant_name}")
    print(f"  ID     : {tenant_id}")
    print("=" * 70)

    inv_resp = requests.get(
        XERO_INVOICES_URL,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Xero-Tenant-Id": tenant_id,
            "Accept": "application/json",
        },
        params={"page": 1, "pageSize": 10, "order": "Date DESC"},
        timeout=30,
    )

    if not inv_resp.ok:
        print(f"  ERROR {inv_resp.status_code}: {inv_resp.text}\n")
        continue

    invoices = inv_resp.json().get("Invoices", [])
    if not invoices:
        print("  No invoices found.\n")
        continue

    print(f"  {'#':<4} {'Date':<12} {'Invoice No':<18} {'Contact':<28} {'Status':<12} {'Total':>10} {'Cur'}")
    print(f"  {'-'*4} {'-'*12} {'-'*18} {'-'*28} {'-'*12} {'-'*10} {'-'*3}")
    for i, inv in enumerate(invoices, 1):
        date = parse_xero_date(inv.get("Date", ""))
        number = (inv.get("InvoiceNumber") or "—")[:17]
        contact = (inv.get("Contact", {}).get("Name") or "—")[:27]
        status = inv.get("Status", "—")
        total = inv.get("Total", 0)
        currency = inv.get("CurrencyCode", "")
        print(f"  {i:<4} {date:<12} {number:<18} {contact:<28} {status:<12} {total:>10.2f} {currency}")
    print()
