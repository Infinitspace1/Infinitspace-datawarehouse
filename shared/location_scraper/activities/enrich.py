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

import logging
import re
import unicodedata
from typing import Any

from shared.location_scraper.clients import apify as apify_client
from shared.location_scraper.clients import lusha as lusha_client
from shared.location_scraper.clients import website_scraper
from shared.location_scraper.config import (
    GOOGLE_SEARCH_ACTOR_ID,
    LUSHA_MAX_REVEALS_PER_AGENCY,
    LUSHA_JOB_TITLES,
    POLISH_COMPANY_KEYWORDS,
)
from shared.location_scraper.models import Agency, ContactBundle, Listing

logger = logging.getLogger(__name__)

_BLOCKED_DOMAIN_SUFFIXES = (
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "youtube.com",
    "otodom.pl",
    "realtor.com",
    "immobilienscout24.de",
    "yelp.com",
    "yelp.de",
)


def _split_contact_name(raw_name: str) -> tuple[str, str]:
    """
    Parse a person-like full name into first/last name.
    Returns empty strings when parsing is not reliable.
    """
    name = (raw_name or "").strip()
    if not name:
        return "", ""

    # Remove common separators from broker payload variants.
    for sep in ("|", "-", ","):
        if sep in name:
            name = name.split(sep, 1)[0].strip()

    parts = [p for p in name.split() if p]
    if len(parts) < 2:
        return "", ""
    return parts[0], " ".join(parts[1:])


def _otodom_individual_name_parts(company_name: str, contact_name: str | None) -> tuple[str, str]:
    contact = (contact_name or "").strip()
    if not contact:
        return "", ""
    contact_lower = contact.lower()
    is_company = any(kw in contact_lower for kw in POLISH_COMPANY_KEYWORDS)
    is_same_as_company = contact_lower == company_name.lower()
    if is_company or is_same_as_company:
        return "", ""
    return _split_contact_name(contact)


def _clean_company_name_for_lusha(company_name: str) -> str:
    """Remove legal suffixes/noise that can make Lusha person search too strict."""
    cleaned = f" {company_name or ''} "
    replacements = [
        r"\bsp\.?\s*z\.?\s*o\.?\s*o\.?\b",
        r"\bspółka\s*z\s*ograniczoną\s*odpowiedzialnością\b",
        r"\bsp\.?\s*j\.?\b",
        r"\bsp\.?\s*k\.?\b",
        r"\bs\.?\s*a\.?\b",
        r"\bsa\b",
        r"\bllc\b",
        r"\bltd\b",
        r"\blimited\b",
        r"\bgmbh\b",
        r"\bdeweloper\b",
        r"\bproperty\b",
        r"\bgroup\b",
        r"\bnieruchomości\b",
    ]
    for pattern in replacements:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[\"'“”‘’,;:.()]+", " ", cleaned)
    cleaned = re.sub(r"\s*[-|]\s*", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or company_name


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _clean_idealista_company_name_for_lusha(company_name: str) -> str:
    """Remove Idealista agency-label noise while keeping the brand name."""
    cleaned = _clean_company_name_for_lusha(company_name)
    cleaned = re.split(r"\s+[-|]\s+", cleaned, maxsplit=1)[0].strip()

    normalized = _strip_accents(cleaned).lower()
    # Strip city names that commonly appear as trailing qualifiers on Idealista listings
    _SPANISH_CITIES = [
        "barcelona", "madrid", "valencia", "sevilla", "bilbao", "zaragoza",
        "malaga", "granada", "alicante", "murcia", "pamplona", "san sebastian",
    ]
    _ITALIAN_CITIES = [
        "milano", "milan", "roma", "rome", "torino", "napoli", "firenze",
        "bologna", "venezia", "genova", "palermo", "bari",
    ]
    _TRAILING_CITIES = _SPANISH_CITIES + _ITALIAN_CITIES

    # First: strip trailing city name so "Engel & Völkers Commercial Barcelona"
    # becomes "Engel & Völkers Commercial" before suffix stripping below.
    for city in _TRAILING_CITIES:
        if normalized.endswith(f" {city}"):
            cleaned = cleaned[: len(cleaned) - len(city)].strip(" -|,.;")
            normalized = _strip_accents(cleaned).lower()
            break

    suffixes = [
        "oficinas y locales",
        "especialistas en oficinas",
        "consultores inmobiliarios",
        "consultores inmobiliarias",
        "consultoria inmobiliaria",
        "commercial",
        "consultores",
        "inmobiliarios",
        "inmobiliaria",
        "inmobiliario",
        "real estate",
        "properties",
        "property",
    ]
    for suffix in suffixes:
        if normalized.endswith(f" {suffix}"):
            cleaned = cleaned[: len(cleaned) - len(suffix)].strip(" -|,.;")
            normalized = _strip_accents(cleaned).lower()

    return cleaned or company_name


def _clean_immobilienscout_company_name_for_lusha(company_name: str) -> str:
    """Remove German city/descriptor noise while keeping the brand name."""
    cleaned = _clean_company_name_for_lusha(company_name)
    cleaned = re.split(r"\s+[-|]\s+", cleaned, maxsplit=1)[0].strip()

    normalized = _strip_accents(cleaned).lower()
    _GERMAN_CITIES = [
        "berlin", "munich", "munchen", "münchen", "hamburg", "cologne", "koln", "köln",
        "frankfurt", "dusseldorf", "dusseldorf", "düsseldorf", "stuttgart", "dortmund",
        "essen", "leipzig", "bremen", "dresden", "hannover", "nuremberg", "nurnberg",
        "duisburg", "bochum", "wuppertal", "bielefeld", "mannheim", "augsburg",
    ]
    for city in _GERMAN_CITIES:
        if normalized.endswith(f" {city}"):
            cleaned = cleaned[: len(cleaned) - len(city)].strip(" -|,.;")
            normalized = _strip_accents(cleaned).lower()
            break

    suffixes = [
        "gewerbeimmobilien",
        "immobiliengesellschaft",
        "immobilienmakler",
        "immobilien",
        "transactions",
        "transaction",
        "vermietung",
        "verwaltung",
        "beratung",
        "real estate",
        "property",
    ]
    for suffix in suffixes:
        if normalized.endswith(f" {suffix}"):
            cleaned = cleaned[: len(cleaned) - len(suffix)].strip(" -|,.;")
            normalized = _strip_accents(cleaned).lower()

    # Remove consecutive duplicate tokens (e.g. "HSC HSC GmbH" → "HSC")
    tokens = cleaned.split()
    deduped = []
    for tok in tokens:
        if not deduped or tok.lower() != deduped[-1].lower():
            deduped.append(tok)
    cleaned = " ".join(deduped).strip()

    return cleaned or company_name


def _company_name_variants_for_lusha(company_name: str, source: str) -> list[str]:
    """Return ordered company-name variants to try against Lusha."""
    variants: list[str] = []
    base_values = (
        (company_name, _clean_company_name_for_lusha(company_name))
        if source == "idealista"
        else (_clean_company_name_for_lusha(company_name), company_name)
    )
    for value in base_values:
        cleaned = (value or "").strip()
        if cleaned and cleaned.casefold() not in {v.casefold() for v in variants}:
            variants.append(cleaned)

    if source == "idealista":
        cleaned = _clean_idealista_company_name_for_lusha(company_name)
        if cleaned and cleaned.casefold() not in {v.casefold() for v in variants}:
            variants.append(cleaned)
    elif source == "immobilienscout":
        cleaned = _clean_immobilienscout_company_name_for_lusha(company_name)
        if cleaned and cleaned.casefold() not in {v.casefold() for v in variants}:
            variants.append(cleaned)

    return variants or [company_name]


def _is_low_value_otodom_agency_name(company_name: str) -> bool:
    cleaned = (company_name or "").strip()
    if not cleaned:
        return True
    lower = cleaned.lower()
    if any(kw in lower for kw in POLISH_COMPANY_KEYWORDS):
        return False
    if any(ch in cleaned for ch in (".", "-", "&")):
        return False
    words = [w for w in cleaned.split() if w]
    return len(words) <= 2


def _is_blocked_domain(domain: str) -> bool:
    value = (domain or "").lower().strip()
    return any(value == suffix or value.endswith(f".{suffix}") for suffix in _BLOCKED_DOMAIN_SUFFIXES)


def _building_key(lat: float | None, lon: float | None, floor: str | None) -> str | None:
    if lat is None or lon is None:
        return None
    floor_key = floor if floor is not None else "null"
    return f"{lat:.4f}_{lon:.4f}_{floor_key}"


def _agency_key(value: str | None) -> str:
    return (value or "").strip().casefold()


# ---------------------------------------------------------------------------
# Dedupe agencies (port: "Distinct Agency Name" node)
# ---------------------------------------------------------------------------

def dedupe_agencies(listings: list[dict]) -> list[dict]:
    """
    Extract unique agencies from normalized listings.
    For Otodom: agency = company_name; for others: agency = contact_name.
    Otodom enrichment is company-first because Warsaw run diagnostics showed
    individual person searches consistently return no contacts.

    Returns list of Agency.to_dict().
    """
    seen: set[str] = set()
    otodom_candidates: dict[str, dict] = {}
    results: list[dict] = []

    for raw in listings:
        listing = Listing.from_dict(raw)
        source = listing.source
        if source == "otodom" and (listing.contact_type or "").lower() == "private":
            continue

        company_name = (
            listing.company_name
            if source in ("otodom", "immobilienscout")
            else listing.contact_name
        )
        if not company_name or not company_name.strip():
            continue
        company_name = company_name.strip()
        if source == "otodom" and _is_low_value_otodom_agency_name(company_name):
            continue

        if source == "otodom":
            first_name, last_name = _otodom_individual_name_parts(company_name, listing.contact_name)
            bucket = otodom_candidates.setdefault(
                company_name,
                {"individuals": [], "seen": set()},
            )
            if first_name and last_name:
                key = (first_name.lower(), last_name.lower())
                if key not in bucket["seen"]:
                    bucket["seen"].add(key)
                    bucket["individuals"].append((first_name, last_name))
            continue

        if company_name in seen:
            continue
        seen.add(company_name)

        agency = Agency(
            company_name=company_name,
            first_name="",
            last_name="",
            source=source,
        )
        results.append(agency.to_dict())

    for company_name, bucket in otodom_candidates.items():
        results.append(Agency(company_name=company_name, source="otodom").to_dict())
    return results


# ---------------------------------------------------------------------------
# Filter agencies already in SQL (port: "Filter agencies without Lusha" node)
# ---------------------------------------------------------------------------

def filter_new_agencies(payload: list[dict] | dict) -> list[dict]:
    """
    Remove agencies that should not trigger Lusha again.

    Skips an agency when:
      - the agency already produced Lusha contacts in prior diagnostics
      - the exact person/contact name already exists in Lusha contacts
      - every listing for that agency is on a building that already has a
        Lusha contact linked
    """
    from shared.azure_clients.sql_client import get_sql_client

    if isinstance(payload, dict):
        agencies = payload.get("agencies") or []
        listings = [Listing.from_dict(d) for d in payload.get("listings") or []]
    else:
        agencies = payload
        listings = []

    sql = get_sql_client()
    try:
        contact_rows = sql.execute_query(
            "SELECT DISTINCT name FROM bronze.n8n_location_scraper_contacts "
            "WHERE source = 'lusha' AND name IS NOT NULL"
        )
        existing_contacts: set[str] = {
            _agency_key(r["name"]) for r in contact_rows if r.get("name")
        }
    except Exception:
        logger.exception("Could not query existing lusha agencies; proceeding with all")
        existing_contacts = set()

    try:
        agency_rows = sql.execute_query(
            "SELECT DISTINCT agency_name FROM bronze.location_scraper_lusha_diagnostics "
            "WHERE has_contact = 1 AND agency_name IS NOT NULL"
        )
        existing_agencies: set[str] = {
            _agency_key(r["agency_name"]) for r in agency_rows if r.get("agency_name")
        }
    except Exception:
        logger.exception("Could not query existing Lusha diagnostics; proceeding without agency history")
        existing_agencies = set()

    agency_has_unsourced_listing: dict[str, bool] = {}
    if listings:
        cities = sorted({L.city for L in listings if L.city})
        existing_lusha_buildings: set[str] = set()
        try:
            for city in cities:
                rows = sql.execute_query(
                    """
                    SELECT DISTINCT b.latitude, b.longitude, b.floor
                    FROM bronze.n8n_location_scraper_buildings AS b
                    JOIN bronze.n8n_location_scraper_listings AS l
                        ON l.building_id = b.id
                    JOIN bronze.n8n_location_scraper_listing_contacts AS lc
                        ON lc.listing_id = l.id
                    JOIN bronze.n8n_location_scraper_contacts AS c
                        ON c.id = lc.contact_id
                    WHERE b.city = ? AND c.source = 'lusha'
                    """,
                    (city,),
                )
                for row in rows:
                    key = _building_key(
                        float(row["latitude"]) if row.get("latitude") is not None else None,
                        float(row["longitude"]) if row.get("longitude") is not None else None,
                        str(row["floor"]) if row.get("floor") is not None else None,
                    )
                    if key:
                        existing_lusha_buildings.add(key)
        except Exception:
            logger.exception("Could not query existing Lusha-linked buildings; treating listings as unsourced")
            existing_lusha_buildings = set()

        for listing in listings:
            matching = _agency_key(listing.matching_name())
            if not matching:
                continue
            key = _building_key(listing.latitude, listing.longitude, listing.floor)
            if not key or key not in existing_lusha_buildings:
                agency_has_unsourced_listing[matching] = True
            else:
                agency_has_unsourced_listing.setdefault(matching, False)

    filtered = []
    for raw in agencies:
        agency = Agency.from_dict(raw)
        agency_name = _agency_key(agency.company_name)
        person_name = _agency_key(f"{agency.first_name} {agency.last_name}".strip())
        if agency_name in existing_agencies:
            continue
        if person_name and person_name in existing_contacts:
            continue
        if listings and not agency_has_unsourced_listing.get(agency_name, False):
            continue
        filtered.append(raw)
    return filtered


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
    diagnostics = {
        "path": "individual" if agency.is_individual() else "company",
        "has_contact": False,
        "reason": "",
        "individual_email_source": "",
        "domains_found": 0,
        "google_domains": [],
        "company_name_cleaned": (_company_name_variants_for_lusha(agency.company_name, agency.source)[-1]),
        "lusha_search_mode": "",
        "domain_used": "",
        "raw_contacts_found": 0,
        "final_contacts_found": 0,
    }

    if agency.is_individual():
        diagnostics.update(_enrich_individual(agency))
        # Fall back to company path if the individual search returned nothing.
        if not agency.contacts and agency.allow_company_fallback:
            company_diag = _enrich_company(agency, country, country_code)
            diagnostics["path"] = "individual_fallback_company"
            diagnostics["domains_found"] = company_diag.get("domains_found", 0)
            diagnostics["google_domains"] = company_diag.get("google_domains", [])
            diagnostics["lusha_search_mode"] = (
                f"{diagnostics.get('lusha_search_mode') or 'individual_exact'}+"
                f"{company_diag.get('lusha_search_mode') or 'company'}"
            )
            diagnostics["domain_used"] = company_diag.get("domain_used", "")
            diagnostics["raw_contacts_found"] = company_diag.get("raw_contacts_found", 0)
            diagnostics["final_contacts_found"] = company_diag.get("final_contacts_found", 0)
            if agency.contacts:
                diagnostics["reason"] = "fallback_company_success"
            elif not diagnostics.get("reason"):
                diagnostics["reason"] = company_diag.get("reason", "fallback_company_no_contact")
    else:
        company_diag = _enrich_company(agency, country, country_code)
        diagnostics.update(company_diag)
        if not agency.contacts and company_diag.get("domain_used"):
            _enrich_via_website(agency, company_diag["domain_used"], diagnostics)

    diagnostics["has_contact"] = bool(agency.contacts)
    diagnostics["final_contacts_found"] = len(agency.contacts)
    out = agency.to_dict()
    out["_diagnostics"] = diagnostics
    return out


def _enrich_individual(agency: Agency) -> dict:
    """Port: Filter1 → Lusha Search Individuals → Normalize Individual Result."""
    cleaned_company = _company_name_variants_for_lusha(agency.company_name, agency.source)[-1]
    diag = {
        "reason": "",
        "individual_email_source": "",
        "company_name_cleaned": cleaned_company,
        "lusha_search_mode": "individual_exact",
    }
    try:
        person = lusha_client.search_individual(
            agency.first_name, agency.last_name, agency.company_name
        )
        if not person and cleaned_company and cleaned_company.lower() != agency.company_name.lower():
            diag["lusha_search_mode"] = "individual_cleaned"
            person = lusha_client.search_individual(
                agency.first_name, agency.last_name, cleaned_company
            )
        if not person:
            diag["reason"] = "individual_no_person"
            return diag
        best_email, best_confidence = lusha_client.extract_best_email(person)
        if not best_email:
            diag["reason"] = "individual_no_email"
            return diag
        full_name = (
            person.get("fullName")
            or f"{agency.first_name} {agency.last_name}".strip()
            or agency.company_name
        )
        diag["reason"] = "individual_success"
        diag["individual_email_source"] = (
            "primaryEmail" if (person.get("primaryEmail") or "").strip() else "emailAddresses"
        )
        agency.contacts = [
            {
                "full_name": full_name,
                "job_title": person.get("jobTitle", ""),
                "email": best_email,
                "email_confidence": best_confidence,
                "linkedin_url": person.get("linkedinProfile", ""),
                "seniority_rank": 3,  # individuals get highest priority per n8n logic
                "domain_rank": 1,
            }
        ]
    except Exception:
        logger.exception("Lusha individual search failed for %s %s", agency.first_name, agency.last_name)
        diag["reason"] = "individual_exception"
    return diag


def _enrich_company(agency: Agency, country: str, country_code: str) -> dict:
    """
    Port: Build Google Search Queries → Get companies domain →
          Extract Domain from Results → Extract Contact → Filter →
          Enrich Emails → Pick Best Contact by Seniority.
    """
    company = agency.company_name
    diag = {
        "reason": "",
        "domains_found": 0,
        "google_domains": [],
        "company_name_cleaned": (_company_name_variants_for_lusha(company, agency.source)[-1] if company else ""),
        "lusha_search_mode": "",
        "domain_used": "",
        "raw_contacts_found": 0,
        "final_contacts_found": 0,
    }
    if not company:
        diag["reason"] = "company_missing_name"
        return diag

    # 1. Google Search via Apify to find company domain
    query = f"{company.replace('&', 'and')} real estate official website"
    try:
        search_results = apify_client.run_sync(
            GOOGLE_SEARCH_ACTOR_ID,
            {"queries": query, "countryCode": country_code, "maxPagesPerQuery": 1, "resultsPerPage": 3},
            limit=10,
        )
    except Exception:
        logger.exception("Google Search Apify run failed for company=%s", company)
        diag["reason"] = "company_google_search_failed"
        return diag

    # 2. Extract up to 3 domains from organic results
    domains_with_rank: list[tuple[str, int]] = []
    seen_domains: set[str] = set()
    for result in search_results:
        organic = result.get("organicResults") or []
        for rank, hit in enumerate(organic[:3], start=1):
            url = hit.get("url", "")
            import re
            match = re.match(r"^https?://(?:www\.)?([^/]+)", url)
            if match:
                domain = match.group(1).lower().strip()
                if domain and domain not in seen_domains and not _is_blocked_domain(domain):
                    seen_domains.add(domain)
                    domains_with_rank.append((domain, rank))
    diag["domains_found"] = len(domains_with_rank)
    diag["google_domains"] = [
        {"domain": domain, "rank": rank}
        for domain, rank in domains_with_rank
    ]

    if not domains_with_rank:
        diag["reason"] = "company_no_domain_found"
        return diag

    # 3. Lusha contact search per domain, collect best contacts
    all_contacts: list[dict] = []
    last_domain_tried = ""
    last_mode_when_empty = ""
    company_variants = _company_name_variants_for_lusha(company, agency.source)
    for domain, domain_rank in domains_with_rank:
        last_domain_tried = domain
        modes = [
            ("company_job_titles_country", country, LUSHA_JOB_TITLES),
            ("company_job_titles_global", None, LUSHA_JOB_TITLES),
            ("company_domain_only_country", country, None),
            ("company_domain_only_global", None, None),
        ]
        for company_variant in company_variants:
            for mode, search_country, job_titles in modes:
                try:
                    raw_contacts = lusha_client.search_contacts_by_domain(
                        domain=domain,
                        country=search_country,
                        job_titles=job_titles,
                        company_name=company_variant,
                        limit=LUSHA_MAX_REVEALS_PER_AGENCY,
                    )
                except Exception:
                    logger.exception("Lusha search failed for domain=%s mode=%s", domain, mode)
                    last_mode_when_empty = f"{mode}_exception"
                    diag["company_name_cleaned"] = company_variant
                    continue

                diag["lusha_search_mode"] = mode
                diag["domain_used"] = domain
                diag["company_name_cleaned"] = company_variant
                if not raw_contacts:
                    last_mode_when_empty = f"{mode}_no_raw"
                    continue

                diag["raw_contacts_found"] += len(raw_contacts)

                # 4. Enrich each contact to get email addresses
                enriched = []
                for rc in raw_contacts:
                    if rc.get("_already_enriched"):
                        enriched.append(rc)
                        continue
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
                if best:
                    break
                last_mode_when_empty = f"{mode}_no_email"

            if all_contacts:
                break

        if all_contacts:
            break

    agency.contacts = all_contacts
    diag["final_contacts_found"] = len(all_contacts)
    if all_contacts:
        diag["reason"] = "company_success"
    else:
        diag["reason"] = "company_no_contacts"
        if last_domain_tried and not diag.get("domain_used"):
            diag["domain_used"] = last_domain_tried
            diag["lusha_search_mode"] = last_mode_when_empty or "company_exhausted"
    return diag


# ---------------------------------------------------------------------------
# Website scrape fallback (when Lusha finds a domain but zero contacts)
# ---------------------------------------------------------------------------

def _enrich_via_website(agency: Agency, domain: str, diagnostics: dict) -> None:
    """
    Scrape *domain* for email addresses and populate agency.contacts.
    Updates *diagnostics* in-place: reason, lusha_search_mode,
    raw_contacts_found, final_contacts_found.
    """
    try:
        emails = website_scraper.scrape_emails_from_domain(domain)
    except Exception:
        logger.exception("Website scrape failed for domain=%s", domain)
        diagnostics["reason"] = "website_exception"
        return

    diagnostics["lusha_search_mode"] = "website_scrape"
    if not emails:
        diagnostics["reason"] = "website_no_emails"
        return

    diagnostics["reason"] = "website_success"
    diagnostics["raw_contacts_found"] = len(emails)
    contacts = [
        {
            "full_name": "",
            "job_title": "",
            "email": email,
            "email_confidence": "website",
            "linkedin_url": "",
            "seniority_rank": 0,
            "domain_rank": 1,
        }
        for email in emails[:3]
    ]
    agency.contacts = contacts
    diagnostics["final_contacts_found"] = len(contacts)


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
