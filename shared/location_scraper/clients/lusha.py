"""
Lusha REST API client.

Endpoints used:
  - POST https://api.lusha.com/v2/contacts/search
      Search contacts by company domain and job titles.
      Docs: https://lusha.readme.io/reference/contacts-search

  - POST https://api.lusha.com/v2/person/search
      Search an individual by first name, last name, and company.
      Docs: https://lusha.readme.io/reference/person-search

  - POST https://api.lusha.com/v2/person/enrich
      Enrich a person record from a search result to obtain email addresses.
      Docs: https://lusha.readme.io/reference/person-enrich

Auth: API key via the `api_key` query parameter on every request.

Retries: tenacity — max 3 attempts, exponential backoff, retry on 429/5xx.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.lusha.com"
_API_KEY = os.environ.get("LUSHA_API_KEY", "")


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        return exc.response is not None and exc.response.status_code in (429, 500, 502, 503, 504)
    return False


def _retry_decorator():
    return retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )


def _get(path: str, params: dict | None = None) -> dict:
    url = f"{_BASE_URL}{path}"
    p = {"api_key": _API_KEY, **(params or {})}
    resp = requests.get(url, params=p, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, body: dict) -> dict:
    url = f"{_BASE_URL}{path}"
    resp = requests.post(
        url,
        json=body,
        headers={"api_key": _API_KEY, "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _seniority_rank(title: str) -> int:
    """Port of the seniorityRank() function from n8n 'Pick Best Contact by Seniority' node."""
    if not title:
        return 0
    t = title.lower()
    if any(kw in t for kw in ("partner", "head", "director")):
        return 3
    if "manager" in t:
        return 2
    if "consultant" in t:
        return 1
    return 0


@_retry_decorator()
def search_contacts_by_domain(
    domain: str,
    country: str,
    job_titles: list[str],
) -> list[dict[str, Any]]:
    """
    Search for contacts at a company domain with matching job titles.
    Returns a list of raw Lusha contact records.

    Maps to n8n "Extract Contact" node (operation: searchContacts).
    """
    body = {
        "companyDomain": domain,
        "jobTitles": job_titles,
        "countries": [country.capitalize()],
    }
    try:
        data = _post("/v2/contacts/search", body)
        return data.get("data", [])
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return []
        raise


@_retry_decorator()
def enrich_contact(contact_id: str) -> Optional[dict[str, Any]]:
    """
    Enrich a contact from a search result to obtain email addresses.
    Maps to n8n "Enrich Emails" node (operation: enrichFromSearch).
    """
    try:
        data = _post("/v2/person/enrich", {"contactId": contact_id})
        return data.get("data")
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise


@_retry_decorator()
def search_individual(
    first_name: str,
    last_name: str,
    company_name: str,
) -> Optional[dict[str, Any]]:
    """
    Search for an individual person by name and company.
    Maps to n8n "Lusha Search Individuals" node.
    Returns the enriched person record or None if not found.
    """
    body = {
        "firstName": first_name,
        "lastName": last_name,
        "companyName": company_name,
    }
    try:
        data = _post("/v2/person/search", body)
        person = data.get("data")
        if isinstance(person, list):
            return person[0] if person else None
        return person
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code in (404, 422):
            return None
        raise


def extract_best_email(person: dict[str, Any]) -> tuple[str, str]:
    """
    Return best available email and confidence from a Lusha person payload.

    Priority:
      1) primaryEmail / primaryEmailConfidence
      2) first entry in emailAddresses[]
    """
    primary = (person.get("primaryEmail") or "").strip()
    if primary:
        return primary, str(person.get("primaryEmailConfidence", ""))

    email_addresses = person.get("emailAddresses") or []
    if email_addresses:
        first = email_addresses[0] or {}
        email = (first.get("email") or "").strip()
        if email:
            return email, str(first.get("emailConfidence", ""))

    return "", ""


def pick_best_contacts(
    raw_contacts: list[dict],
    domain_rank: int,
) -> list[dict]:
    """
    Port of 'Pick Best Contact by Seniority' n8n node logic.
    Filters to contacts with emails, sorts by seniority desc / domain_rank asc,
    returns enriched contact dicts ready for consolidation.
    """
    results = []
    for raw in raw_contacts:
        if not raw:
            continue
        email_addresses = raw.get("emailAddresses") or []
        if not email_addresses:
            continue
        title = raw.get("jobTitle", "")
        results.append({
            "full_name": raw.get("fullName", ""),
            "job_title": title,
            "email": email_addresses[0].get("email", ""),
            "email_confidence": str(email_addresses[0].get("emailConfidence", "")),
            "linkedin_url": raw.get("linkedinUrl") or raw.get("linkedInUrl") or "",
            "seniority_rank": _seniority_rank(title),
            "domain_rank": domain_rank,
        })
    results.sort(key=lambda c: (-c["seniority_rank"], c["domain_rank"]))
    return results
