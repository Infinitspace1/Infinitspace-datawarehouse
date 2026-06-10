"""
shared/hubspot/client.py

HubSpot Marketing Email API client (API v3).

Auth: private app access token in HUBSPOT_ACCESS_TOKEN.
  Create one under Settings -> Integrations -> Private Apps with the
  `content` scope (read access to marketing emails).

API docs:
  - List emails: GET https://api.hubapi.com/marketing/v3/emails
      ?limit=100&after=<cursor>&includeStats=true
    Response: {"results": [...], "paging": {"next": {"after": "..."}}}
  - Each email carries `stats` (when includeStats=true) with `counters`
    (sent, open, delivered, bounce, click, unsubscribed, ...) and `ratios`
    (openratio, clickratio, ...).
"""
from __future__ import annotations

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.hubapi.com"
PAGE_LIMIT = 100
MAX_RETRIES = 5


def get_access_token() -> str:
    token = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not token:
        raise EnvironmentError(
            "HUBSPOT_ACCESS_TOKEN is not set — create a HubSpot private app "
            "with the `content` scope and set its access token"
        )
    return token


def _headers() -> dict:
    return {"Authorization": f"Bearer {get_access_token()}"}


def _get_with_retry(url: str, params: dict) -> dict:
    """GET with retry on rate limits (429) and transient server errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, headers=_headers(), params=params, timeout=60)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 10))
            logger.warning("HubSpot rate limited — waiting %ss (attempt %s)", wait, attempt)
            time.sleep(wait)
            continue
        if resp.status_code >= 500 and attempt < MAX_RETRIES:
            wait = 2 ** attempt
            logger.warning(
                "HubSpot server error %s — retrying in %ss (attempt %s)",
                resp.status_code, wait, attempt,
            )
            time.sleep(wait)
            continue
        if not resp.ok:
            # HubSpot error bodies name the problem (e.g. MISSING_SCOPES with
            # the exact required scope) — surface them instead of a bare 4xx.
            raise requests.HTTPError(
                f"{resp.status_code} from HubSpot {url}: {resp.text[:500]}",
                response=resp,
            )
        return resp.json()
    raise RuntimeError(f"HubSpot request failed after {MAX_RETRIES} retries: {url}")


def fetch_marketing_emails(include_stats: bool = True) -> list[dict]:
    """Return ALL marketing emails (every page), each with embedded stats.

    Full fetch by design — the volume is small (hundreds of emails) and
    stats for already-sent emails keep changing, so an incremental
    watermark would miss KPI updates. Bronze hash-dedup keeps writes
    cheap when nothing changed.
    """
    results: list[dict] = []
    after: str | None = None

    while True:
        params: dict = {"limit": PAGE_LIMIT}
        if include_stats:
            params["includeStats"] = "true"
        if after:
            params["after"] = after

        data = _get_with_retry(f"{BASE_URL}/marketing/v3/emails", params)
        page = data.get("results", [])
        results.extend(page)
        logger.debug("HubSpot marketing emails — page of %s (total %s)", len(page), len(results))

        after = (data.get("paging") or {}).get("next", {}).get("after")
        if not after or not page:
            break

    return results
