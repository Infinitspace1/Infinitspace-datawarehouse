"""
Activities: dedupe_agencies, filter_new_agencies, enrich_agency, consolidate_contacts

Ports these n8n nodes:
  - Distinct Agency Name
  - Filter agencies without Lusha
  - Build Google Search Queries + Get companies domain + Extract Domain from Results
  - Extract Contact + Filter + Enrich Emails + Pick Best Contact by Seniority  (company path)
  - Filter1 + Lusha Search Individuals + Normalize Individual Result             (individual path)
  - Consolidate by Agency
"""
from __future__ import annotations

import json
import logging
from typing import Any

from shared.location_scraper.clients import apify as apify_client
from shared.location_scraper.clients import lusha as lusha_client
from shared.location_scraper.config import (
    GOOGLE_SEARCH_ACTOR_ID,
    LUSHA_JOB_TITLES,
    POLISH_COMPANY_KEYWORDS,
)
from shared.location_scraper.models import Agency, ContactBundle, Listing

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dedupe agencies (port: "Distinct Agency Name" node)
# ---------------------------------------------------------------------------

def dedupe_agencies(listings: list[dict]) -> list[dict]:
    """
    Extract unique agencies from normalized listings.
    For Otodom: agency = company_name; for others: agency = contact_name.
    Attempts to split Otodom individual contact names into first/last name
    unless the name contains company keywords.

    Returns list of Agency.to_dict().
    """
    seen: set[str] = set()
    results: list[dict] = []

    for raw in listings:
        listing = Listing.from_dict(raw)
        source = listing.source

        company_name = (
            listing.company_name if source == "otodom" else listing.contact_name
        )
        if not company_name or not company_name.strip():
            continue
        if company_name in seen:
            continue
        seen.add(company_name)

        first_name = ""
        last_name = ""

        if source == "otodom" and listing.contact_name:
            contact = listing.contact_name.strip()
            contact_lower = contact.lower()
            is_company = any(kw in contact_lower for kw in POLISH_COMPANY_KEYWORDS)
            is_same_as_company = contact.lower() == company_name.lower()
            if not is_company and not is_same_as_company:
                parts = contact.split()
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = " ".join(parts[1:])

        agency = Agency(
            company_name=company_name,
            first_name=first_name,
            last_name=last_name,
            source=source,
        )
        results.append(agency.to_dict())

    return results


# ---------------------------------------------------------------------------
# Filter agencies already in SQL (port: "Filter agencies without Lusha" node)
# ---------------------------------------------------------------------------

def filter_new_agencies(agencies: list[dict]) -> list[dict]:
    """
    Remove agencies that already have Lusha contacts in SQL.
    Queries bronze.n8n_location_scraper_contacts for existing lusha names.
    """
    from shared.azure_clients.sql_client import get_sql_client

    sql = get_sql_client()
    try:
        rows = sql.execute_query(
            "SELECT DISTINCT name FROM bronze.n8n_location_scraper_contacts "
            "WHERE source = 'lusha' AND name IS NOT NULL"
        )
        existing: set[str] = {r["name"] for r in rows}
    except Exception:
        logger.exception("Could not query existing lusha agencies; proceeding with all")
        existing = set()

    return [a for a in agencies if a["company_name"] not in existing]


# ---------------------------------------------------------------------------
# Enrich one agency (fan-out target — called per agency)
# ---------------------------------------------------------------------------

def enrich_agency(payload: dict) -> dict:
    """
    Enrich a single agency with Lusha contacts.
    payload = {"agency": Agency.to_dict(), "country": str, "country_code": str}

    Routes to:
      - Individual path (has first_name + last_name): Lusha Search Individual
      - Company path: Google Search → domain extraction → Lusha searchContacts → enrich

    Returns Agency.to_dict() with contacts populated.
    """
    agency = Agency.from_dict(payload["agency"])
    country = payload["country"]
    country_code = payload["country_code"]

    if agency.is_individual():
        _enrich_individual(agency)
    else:
        _enrich_company(agency, country, country_code)

    return agency.to_dict()


def _enrich_individual(agency: Agency) -> None:
    """Port: Filter1 → Lusha Search Individuals → Normalize Individual Result."""
    try:
        person = lusha_client.search_individual(
            agency.first_name, agency.last_name, agency.company_name
        )
        if not person:
            return
        primary_email = person.get("primaryEmail", "")
        if not primary_email:
            return
        agency.contacts = [
            {
                "full_name": person.get("fullName", ""),
                "job_title": person.get("jobTitle", ""),
                "email": primary_email,
                "email_confidence": str(person.get("primaryEmailConfidence", "")),
                "linkedin_url": person.get("linkedinProfile", ""),
                "seniority_rank": 3,  # individuals get highest priority per n8n logic
                "domain_rank": 1,
            }
        ]
    except Exception:
        logger.exception("Lusha individual search failed for %s %s", agency.first_name, agency.last_name)


def _enrich_company(agency: Agency, country: str, country_code: str) -> None:
    """
    Port: Build Google Search Queries → Get companies domain →
          Extract Domain from Results → Extract Contact → Filter →
          Enrich Emails → Pick Best Contact by Seniority.
    """
    company = agency.company_name
    if not company:
        return

    # 1. Google Search via Apify to find company domain
    query = f"{company.replace('&', 'and')} real estate official website"
    apify_input = json.dumps({
        "queries": query,
        "countryCode": country_code,
        "maxPagesPerQuery": 1,
        "resultsPerPage": 3,
    })
    try:
        search_results = apify_client.run_sync(
            GOOGLE_SEARCH_ACTOR_ID,
            {"queries": query, "countryCode": country_code, "maxPagesPerQuery": 1, "resultsPerPage": 3},
            limit=10,
        )
    except Exception:
        logger.exception("Google Search Apify run failed for company=%s", company)
        return

    # 2. Extract up to 3 domains from organic results
    domains_with_rank: list[tuple[str, int]] = []
    for result in search_results:
        organic = result.get("organicResults") or []
        for rank, hit in enumerate(organic[:3], start=1):
            url = hit.get("url", "")
            import re
            match = re.match(r"^https?://(?:www\.)?([^/]+)", url)
            if match:
                domains_with_rank.append((match.group(1), rank))

    if not domains_with_rank:
        return

    # 3. Lusha contact search per domain, collect best contacts
    all_contacts: list[dict] = []
    for domain, domain_rank in domains_with_rank:
        try:
            raw_contacts = lusha_client.search_contacts_by_domain(
                domain=domain,
                country=country,
                job_titles=LUSHA_JOB_TITLES,
            )
            if not raw_contacts:
                continue

            # 4. Enrich each contact to get email addresses
            enriched = []
            for rc in raw_contacts:
                contact_id = rc.get("contactId") or rc.get("id")
                if not contact_id:
                    # Some search endpoints return full data directly
                    enriched.append(rc)
                    continue
                try:
                    full = lusha_client.enrich_contact(contact_id)
                    if full:
                        enriched.append(full)
                except Exception:
                    logger.debug("Enrich failed for contact_id=%s", contact_id)

            best = lusha_client.pick_best_contacts(enriched, domain_rank)
            all_contacts.extend(best)
        except Exception:
            logger.exception("Lusha search failed for domain=%s", domain)

    agency.contacts = all_contacts


# ---------------------------------------------------------------------------
# Consolidate contacts across domains per agency (port: "Consolidate by Agency")
# ---------------------------------------------------------------------------

def consolidate_contacts(agencies: list[dict]) -> list[dict]:
    """
    Group all contacts by agency_name, dedupe by email, sort by seniority,
    and return top-3 contacts per agency as list of ContactBundle.to_dict().

    Ports the n8n "Consolidate by Agency" code node.
    """
    # Aggregate all contacts keyed by agency name
    agency_contacts: dict[str, list[dict]] = {}
    for raw in agencies:
        agency = Agency.from_dict(raw)
        if not agency.company_name:
            continue
        bucket = agency_contacts.setdefault(agency.company_name, [])
        for c in agency.contacts:
            contact_dict = c if isinstance(c, dict) else c.to_dict()
            bucket.append(contact_dict)

    results: list[dict] = []
    for agency_name, contacts in agency_contacts.items():
        # Deduplicate by email
        seen_emails: set[str] = set()
        unique: list[dict] = []
        for c in contacts:
            email = c.get("email", "")
            if not email or email in seen_emails:
                continue
            seen_emails.add(email)
            unique.append(c)

        # Sort: highest seniority first, most trusted domain source first
        unique.sort(key=lambda c: (-c.get("seniority_rank", 0), c.get("domain_rank", 99)))
        top3 = unique[:3]

        def _slot(i: int) -> dict:
            return top3[i] if i < len(top3) else {}

        bundle = ContactBundle(
            agency_name=agency_name,
            email_1=_slot(0).get("email", ""),
            email_1_contact=_slot(0).get("full_name", ""),
            email_1_title=_slot(0).get("job_title", ""),
            email_1_confidence=str(_slot(0).get("email_confidence", "")),
            email_1_linkedin=_slot(0).get("linkedin_url", ""),
            email_2=_slot(1).get("email", ""),
            email_2_contact=_slot(1).get("full_name", ""),
            email_2_title=_slot(1).get("job_title", ""),
            email_2_confidence=str(_slot(1).get("email_confidence", "")),
            email_2_linkedin=_slot(1).get("linkedin_url", ""),
            email_3=_slot(2).get("email", ""),
            email_3_contact=_slot(2).get("full_name", ""),
            email_3_title=_slot(2).get("job_title", ""),
            email_3_confidence=str(_slot(2).get("email_confidence", "")),
            email_3_linkedin=_slot(2).get("linkedin_url", ""),
        )
        results.append(bundle.to_dict())

    return results
