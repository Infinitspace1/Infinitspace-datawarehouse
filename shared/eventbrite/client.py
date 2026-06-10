"""
shared/eventbrite/client.py

Eventbrite API client (API v3).

Auth setup (one-time):
  1. Log in to Eventbrite with the organization account.
  2. Go to https://www.eventbrite.com/platform/api-keys
     (Account Settings -> Developer Links -> API Keys).
  3. Copy the "Private token" (a personal OAuth token — no OAuth flow
     needed for first-party access to your own organization).
  4. Set it as EVENTBRITE_PRIVATE_TOKEN.

Optionally set EVENTBRITE_ORGANIZATION_ID to pin one organization;
otherwise every organization the token can access is synced.

API docs: https://www.eventbrite.com/platform/api
  - GET /v3/users/me/organizations/   -> {"organizations": [...], "pagination": {...}}
  - GET /v3/organizations/{id}/events/?status=all&expand=venue,ticket_availability,organizer
      -> {"events": [...], "pagination": {"has_more_items": bool, "continuation": "..."}}
Pagination is continuation-token based.
"""
from __future__ import annotations

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://www.eventbriteapi.com/v3"
MAX_RETRIES = 5

# Expansions pulled with each event so silver gets venue + ticket details
# in a single fetch.
EVENT_EXPANSIONS = "venue,ticket_availability,organizer,format,category"


def get_private_token() -> str:
    token = os.getenv("EVENTBRITE_PRIVATE_TOKEN")
    if not token:
        raise EnvironmentError(
            "EVENTBRITE_PRIVATE_TOKEN is not set — copy the private token from "
            "https://www.eventbrite.com/platform/api-keys"
        )
    return token


def _headers() -> dict:
    return {"Authorization": f"Bearer {get_private_token()}"}


def _get_with_retry(url: str, params: dict) -> dict:
    """GET with retry on rate limits (429) and transient server errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, headers=_headers(), params=params, timeout=60)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 30))
            logger.warning("Eventbrite rate limited — waiting %ss (attempt %s)", wait, attempt)
            time.sleep(wait)
            continue
        if resp.status_code >= 500 and attempt < MAX_RETRIES:
            wait = 2 ** attempt
            logger.warning(
                "Eventbrite server error %s — retrying in %ss (attempt %s)",
                resp.status_code, wait, attempt,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Eventbrite request failed after {MAX_RETRIES} retries: {url}")


def _paginate(url: str, list_key: str, params: dict | None = None) -> list[dict]:
    """Collect all pages of an Eventbrite continuation-paginated endpoint."""
    results: list[dict] = []
    params = dict(params or {})

    while True:
        data = _get_with_retry(url, params)
        page = data.get(list_key, [])
        results.extend(page)

        pagination = data.get("pagination") or {}
        if not pagination.get("has_more_items") or not pagination.get("continuation"):
            break
        params["continuation"] = pagination["continuation"]

    return results


def fetch_organizations() -> list[dict]:
    """Return all organizations the token has access to.

    Honours EVENTBRITE_ORGANIZATION_ID when set (filters to that org, or
    pins it directly if the /users/me/organizations call is not desired).
    """
    org_id = os.getenv("EVENTBRITE_ORGANIZATION_ID")
    orgs = _paginate(f"{BASE_URL}/users/me/organizations/", "organizations")
    if org_id:
        pinned = [o for o in orgs if str(o.get("id")) == str(org_id)]
        if pinned:
            return pinned
        logger.warning(
            "EVENTBRITE_ORGANIZATION_ID=%s not in token's organizations — using it directly",
            org_id,
        )
        return [{"id": str(org_id), "name": None}]
    return orgs


def fetch_events(organization_id: str, status: str = "all") -> list[dict]:
    """Return ALL events for an organization, with venue/ticket/organizer
    expansions embedded.

    status="all" includes draft, live, started, ended, completed and
    canceled events.
    """
    return _paginate(
        f"{BASE_URL}/organizations/{organization_id}/events/",
        "events",
        params={"status": status, "expand": EVENT_EXPANSIONS},
    )
