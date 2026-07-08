"""
scripts/python_scripts/xero_open_auth.py

Step 1 of Xero OAuth: build the authorization URL and open it in the browser.
No database required — uses a simple random state token.

The boss must open this URL, select ALL tenants/organizations on the Xero consent
screen, then copy the full redirect URL (https://xero.com/?code=...) and send it
back so the code can be exchanged for tokens.

Usage:
    python scripts/python_scripts/xero_open_auth.py
"""
import os
import secrets
import webbrowser
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parent.parent.parent

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

XERO_AUTHORIZE_URL = "https://login.xero.com/identity/connect/authorize"

# Read from .env — must match what is registered in the Xero Developer Portal
client_id = os.getenv("XERO_CLIENT_ID")
redirect_uri = os.getenv("XERO_REDIRECT_URI", "https://xero.com")

# New granular scopes required for apps created after 2 March 2026
scopes = os.getenv(
    "XERO_SCOPES",
    (
        "openid offline_access "
        "accounting.contacts accounting.contacts.read "
        "accounting.payments accounting.payments.read "
        "accounting.transactions accounting.transactions.read "
        "accounting.settings.read accounting.reports.read"
    ),
)

if not client_id:
    raise EnvironmentError("XERO_CLIENT_ID not set in .env")
if not redirect_uri:
    raise EnvironmentError("XERO_REDIRECT_URI not set in .env")

state = secrets.token_urlsafe(32)

params = {
    "response_type": "code",
    "client_id": client_id,
    "redirect_uri": redirect_uri,
    "scope": scopes,
    "state": state,
}

auth_url = f"{XERO_AUTHORIZE_URL}?{urlencode(params)}"

print("=" * 70)
print("XERO OAUTH — STEP 1: AUTHORIZATION")
print("=" * 70)
print(f"Client ID   : {client_id}")
print(f"Redirect URI: {redirect_uri}")
print(f"State       : {state}")
print()
print("IMPORTANT — tell the boss to:")
print("  1. Log in with his Xero account")
print("  2. SELECT ALL organizations/tenants on the consent screen")
print("  3. Click Allow access")
print("  4. Copy the FULL URL from the browser after redirect")
print("     (it starts with: https://xero.com/?code=...)")
print("  5. Send you that full URL")
print()
print("Opening browser now...")
print(auth_url)
print("=" * 70)

webbrowser.open(auth_url)
