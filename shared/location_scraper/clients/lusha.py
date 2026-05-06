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

from shared.location_scraper.config import LUSHA_MAX_REVEALS_PER_AGENCY

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.lusha.com"
_API_KEY = os.environ.get("LUSHA_API_KEY", "")
_PROSPECTING_PAGE_SIZE = 20


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


def _full_name(contact: dict[str, Any]) -> str:
    return (
        contact.get("fullName")
        or contact.get("name")
        or " ".join(
            p for p in (contact.get("firstName"), contact.get("lastName")) if p
        )
        or ""
    )


def _contact_id(contact: dict[str, Any]) -> str:
    return str(contact.get("contactId") or contact.get("id") or "").strip()


def _company_id(contact: dict[str, Any]) -> str:
    return str(contact.get("companyId") or contact.get("organizationId") or "").strip()


def _contact_domain(contact: dict[str, Any], fallback: str) -> str:
    return (
        contact.get("fqdn")
        or contact.get("domain")
        or contact.get("companyDomain")
        or fallback
        or ""
    )


def _email_addresses(contact: dict[str, Any]) -> list[dict[str, Any]]:
    emails = contact.get("emailAddresses") or []
    if emails:
        return emails
    email = contact.get("email") or contact.get("workEmail")
    if email:
        return [{"email": email, "emailConfidence": contact.get("emailConfidence", "")}]
    data = contact.get("data") or {}
    emails = data.get("emailAddresses") or []
    if emails:
        return emails
    email = data.get("email") or data.get("workEmail")
    if email:
        return [{"email": email, "emailConfidence": data.get("emailConfidence", "")}]
    return []


def _normalize_prospect_contact(contact: dict[str, Any], fallback_domain: str) -> dict[str, Any]:
    data = contact.get("data") if isinstance(contact.get("data"), dict) else contact
    social_links = data.get("socialLinks") or contact.get("socialLinks") or {}
    linkedin = (
        data.get("linkedinUrl")
        or data.get("linkedinProfile")
        or contact.get("linkedinUrl")
        or contact.get("linkedinProfile")
        or social_links.get("linkedin")
        or ""
    )
    return {
        "contactId": _contact_id(contact) or _contact_id(data),
        "fullName": _full_name(data) or _full_name(contact),
        "jobTitle": data.get("jobTitle") or contact.get("jobTitle") or "",
        "emailAddresses": _email_addresses(data) or _email_addresses(contact),
        "linkedinUrl": linkedin,
        "companyName": data.get("companyName") or contact.get("companyName") or "",
        "domain": _contact_domain(data, fallback_domain) or _contact_domain(contact, fallback_domain),
        "_already_enriched": True,
    }


def _prospecting_search_contacts(
    *,
    company_name: str,
    domain: str,
    country: str | None,
    job_titles: list[str] | None,
) -> tuple[str, list[dict[str, Any]]]:
    filters: dict[str, Any] = {
        "contacts": {
            "include": {"existing_data_points": ["work_email"]},
            "exclude": {},
        },
        "companies": {
            "include": {"names": [company_name]},
            "exclude": {},
        },
    }
    if job_titles:
        filters["contacts"]["include"]["jobTitles"] = job_titles
    if country:
        filters["contacts"]["include"]["locations"] = [{"country": country.capitalize()}]

    data = _post(
        "/prospecting/contact/search",
        {
            "pages": {"page": 0, "size": _PROSPECTING_PAGE_SIZE},
            "filters": filters,
        },
    )
    request_id = data.get("requestId") or ""
    contacts = data.get("contacts")
    if contacts is None:
        contacts = data.get("data") or []
    normalized = []
    for contact in contacts:
        contact_id = _contact_id(contact)
        if not contact_id:
            continue
        normalized.append(
            {
                "contactId": contact_id,
                "fullName": _full_name(contact),
                "companyName": contact.get("companyName") or company_name,
                "companyId": _company_id(contact),
                "jobTitle": contact.get("jobTitle") or "",
                "domain": _contact_domain(contact, domain),
                "hasWorkEmail": bool(contact.get("hasWorkEmail", True)),
            }
        )
    return request_id, normalized


def _prospecting_enrich_contacts(
    *,
    request_id: str,
    contacts: list[dict[str, Any]],
    domain: str,
    limit: int,
) -> list[dict[str, Any]]:
    contact_ids = [
        contact["contactId"]
        for contact in contacts
        if contact.get("contactId") and contact.get("hasWorkEmail", True)
    ]
    if not request_id or not contact_ids:
        return []

    by_id = {str(contact.get("contactId")): contact for contact in contacts}
    results = []
    for start in range(0, len(contact_ids), limit):
        batch_ids = contact_ids[start : start + limit]
        data = _post(
            "/prospecting/contact/enrich",
            {
                "requestId": request_id,
                "contactIds": batch_ids,
                "revealEmails": True,
                "revealPhones": False,
            },
        )
        enriched_items = data.get("contacts") or []
        if isinstance(enriched_items, dict):
            iterable = enriched_items.items()
        else:
            iterable = ((_contact_id(item), item) for item in enriched_items)
        for contact_id, enriched in iterable:
            base = by_id.get(str(contact_id), {})
            merged = {**base, **(enriched or {})}
            normalized = _normalize_prospect_contact(merged, domain)
            if normalized["emailAddresses"]:
                results.append(normalized)
                if len(results) >= limit:
                    return results
    return results


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
    country: str | None = None,
    job_titles: list[str] | None = None,
    company_name: str | None = None,
    limit: int = LUSHA_MAX_REVEALS_PER_AGENCY,
) -> list[dict[str, Any]]:
    """
    Search for contacts at a company domain with matching job titles.
    Returns a list of raw Lusha contact records.

    Maps to n8n "Extract Contact" node (operation: searchContacts).
    """
    company = (company_name or "").strip() or domain.split(".", 1)[0].replace("-", " ").strip()
    request_id, prospects = _prospecting_search_contacts(
        company_name=company,
        domain=domain,
        country=country,
        job_titles=job_titles,
    )
    enriched = _prospecting_enrich_contacts(
        request_id=request_id,
        contacts=prospects,
        domain=domain,
        limit=limit,
    )
    if enriched:
        return enriched

    # Backward-compatible fallback for older Lusha accounts/endpoints.
    body = {"companyDomain": domain}
    if country:
        body["countries"] = [country.capitalize()]
    if job_titles:
        body["jobTitles"] = job_titles
    try:
        data = _post("/v2/contacts/search", body)
        return (data.get("data", []) or [])[:limit]
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
