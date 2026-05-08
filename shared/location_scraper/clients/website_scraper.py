"""
Website email scraper — fallback when Lusha returns no contacts.

Fetches the agency's homepage and common contact pages, extracts email
addresses via regex, and returns unique non-system emails.

No Apify: plain HTTP GET with requests. Suitable for small agency sites
that don't use aggressive anti-bot measures.

System-only addresses (noreply@, bounce@, mailer@, etc.) are filtered out.
All other emails — including info@, contact@, hola@ — are kept intentionally,
because for small agencies these ARE the right contact address.

Extraction pipeline per page:
  1. Decode HTML entities (&#64; -> @, &amp; -> &, etc.)
  2. Replace common obfuscations ([at], (at), " at ")
  3. Run email regex on decoded text
  4. Also parse <script type="application/ld+json"> structured data for
     LocalBusiness / Organization email fields (server-side rendered on most
     frameworks, survives without JS execution)
"""
from __future__ import annotations

import html as html_module
import json
import logging
import re
from typing import Any

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_JSONLD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)

_SYSTEM_PREFIXES = frozenset(
    {
        "noreply",
        "no-reply",
        "donotreply",
        "do-not-reply",
        "bounce",
        "mailer",
        "daemon",
        "postmaster",
        "root",
        "abuse",
        "security",
        "alerts",
        "notifications",
        "unsubscribe",
        "webmaster",
    }
)

# Ordered by likelihood of having an email address.
# Homepage is first — many small agencies put their email there.
# Multilingual contact paths cover ES, IT, PL, DE, EN.
_CONTACT_PATHS = [
    "",
    "/contact",
    "/contacto",
    "/contatti",
    "/kontakt",
    "/contact-us",
    "/about",
    "/equipo",
    "/team",
    "/chi-siamo",
    "/uber-uns",
    "/o-nas",
    "/about-us",
]

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en,es;q=0.9,it;q=0.8,pl;q=0.7,de;q=0.6",
}


def _is_system_email(email: str) -> bool:
    prefix = email.split("@")[0].lower()
    return prefix in _SYSTEM_PREFIXES


def _fetch(url: str, timeout: int) -> str | None:
    try:
        resp = requests.get(
            url, headers=_HEADERS, timeout=timeout, verify=False, allow_redirects=True
        )
        if resp.ok and resp.content:
            return resp.text
    except Exception:
        pass
    return None


def _decode_html(raw: str) -> str:
    """Decode HTML entities and common @ obfuscations."""
    # Standard HTML entity decoding (&amp; &#64; &#x40; etc.)
    decoded = html_module.unescape(raw)
    # Common manual obfuscations used by small agencies to avoid spam scrapers.
    # Longer patterns first so they match before the shorter substring does.
    for obfuscated, replacement in [
        (" [at] ", "@"),
        (" (at) ", "@"),
        (" at ", "@"),
        ("[at]", "@"),
        ("(at)", "@"),
    ]:
        decoded = decoded.replace(obfuscated, replacement)
    return decoded


def _emails_from_jsonld(html: str) -> list[str]:
    """
    Parse <script type="application/ld+json"> blocks and return any email
    values found in LocalBusiness / Organization nodes.
    Server-side frameworks (Next.js, Nuxt, WordPress) inject this even when
    the rest of the page is JS-rendered.
    """
    emails: list[str] = []
    for m in _JSONLD_RE.finditer(html):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        emails.extend(_walk_jsonld(data))
    return emails


def _walk_jsonld(obj: Any, depth: int = 0) -> list[str]:
    if depth > 6:
        return []
    if isinstance(obj, str):
        return _EMAIL_RE.findall(obj)
    if isinstance(obj, dict):
        results: list[str] = []
        for key, value in obj.items():
            if key.lower() in ("email", "e-mail", "mail"):
                if isinstance(value, str) and "@" in value:
                    results.append(value)
            else:
                results.extend(_walk_jsonld(value, depth + 1))
        return results
    if isinstance(obj, list):
        results = []
        for item in obj:
            results.extend(_walk_jsonld(item, depth + 1))
        return results
    return []


def _extract_emails(html: str) -> list[str]:
    """Return unique non-system emails from a raw HTML string."""
    seen: set[str] = set()
    found: list[str] = []

    def _add(email: str) -> None:
        lower = email.lower()
        if lower not in seen and not _is_system_email(lower):
            seen.add(lower)
            found.append(email)

    # 1. Full-page regex after entity/obfuscation decoding
    decoded = _decode_html(html)
    for email in _EMAIL_RE.findall(decoded):
        _add(email)

    # 2. JSON-LD structured data (server-side rendered, survives SPA hydration)
    if not found:
        for email in _emails_from_jsonld(html):
            _add(email)

    return found


def scrape_emails_from_domain(domain: str, timeout: int = 5) -> list[str]:
    """
    Try homepage then contact pages for *domain* (and www.*domain* if needed).
    Returns unique non-system emails. Stops after the first page that yields
    at least one email to minimise requests.
    """
    domain = domain.lower().strip().rstrip("/")
    if not domain:
        return []

    # Try bare domain first, then www. prefix as fallback
    base_domains = [domain]
    if not domain.startswith("www."):
        base_domains.append(f"www.{domain}")

    found: list[str] = []

    for base in base_domains:
        for path in _CONTACT_PATHS:
            url = f"https://{base}{path}"
            html = _fetch(url, timeout)
            if html is None and not path:
                # HTTPS failed on homepage — try plain HTTP
                html = _fetch(f"http://{base}", timeout)
            if not html:
                continue

            page_emails = _extract_emails(html)
            for email in page_emails:
                lower = email.lower()
                if lower not in {e.lower() for e in found}:
                    found.append(email)

            if found:
                break

        if found:
            break

    logger.debug("website_scraper: domain=%s found=%d email(s)", domain, len(found))
    return found
