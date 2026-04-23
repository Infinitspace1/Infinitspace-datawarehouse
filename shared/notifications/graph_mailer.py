"""
shared/notifications/graph_mailer.py

Send email via Microsoft Graph using app-only (client credentials) auth.

Requires an Azure AD App Registration with the `Mail.Send` application
permission granted (admin consent) on the sender mailbox.

Env vars:
    GRAPH_TENANT_ID
    GRAPH_CLIENT_ID
    GRAPH_CLIENT_SECRET
    GRAPH_SENDER_UPN  -- the mailbox to send from, e.g. info@infinitspace.com
"""
from __future__ import annotations

import logging
import os
from typing import Iterable

import msal
import requests

logger = logging.getLogger(__name__)

GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH_SEND_URL = "https://graph.microsoft.com/v1.0/users/{sender}/sendMail"


class GraphMailerError(RuntimeError):
    pass


def _acquire_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )
    result = app.acquire_token_for_client(scopes=GRAPH_SCOPE)
    if "access_token" not in result:
        raise GraphMailerError(
            f"Graph token acquisition failed: "
            f"{result.get('error')}: {result.get('error_description')}"
        )
    return result["access_token"]


def send_mail(
    subject: str,
    html_body: str,
    to_recipients: Iterable[str],
    sender_upn: str | None = None,
    save_to_sent_items: bool = False,
) -> None:
    """Send an HTML email via Microsoft Graph.

    Raises GraphMailerError on any failure.
    """
    tenant_id = os.getenv("GRAPH_TENANT_ID")
    client_id = os.getenv("GRAPH_CLIENT_ID")
    client_secret = os.getenv("GRAPH_CLIENT_SECRET")
    sender = sender_upn or os.getenv("GRAPH_SENDER_UPN")

    if not all([tenant_id, client_id, client_secret, sender]):
        raise GraphMailerError(
            "Missing Graph config: GRAPH_TENANT_ID, GRAPH_CLIENT_ID, "
            "GRAPH_CLIENT_SECRET, GRAPH_SENDER_UPN must all be set"
        )

    recipients = [r.strip() for r in to_recipients if r and r.strip()]
    if not recipients:
        raise GraphMailerError("send_mail called with no recipients")

    token = _acquire_token(tenant_id, client_id, client_secret)

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [
                {"emailAddress": {"address": addr}} for addr in recipients
            ],
        },
        "saveToSentItems": save_to_sent_items,
    }

    resp = requests.post(
        GRAPH_SEND_URL.format(sender=sender),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )

    if resp.status_code != 202:
        raise GraphMailerError(
            f"Graph sendMail returned {resp.status_code}: {resp.text[:500]}"
        )

    logger.info(
        "Graph sendMail OK: from=%s, to=%s, subject=%r",
        sender, recipients, subject,
    )
