"""
shared/real_estate/building_contact_extractor.py

PDF Building Contact Extractor for Azure Functions.
Uses PyMuPDF (fitz) for PDF-to-image conversion — no system dependencies.
Uses Anthropic Claude API for contact extraction from page images.
Uses Google Maps Geocoding API for building address geocoding.

Adapted from AI-REAL-ESTATE/extract_building_contacts_improved.py.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict

import anthropic
import fitz  # PyMuPDF
import pandas as pd

from shared.gmaps.geocoding import GeocodingCache

logger = logging.getLogger(__name__)

# Extraction settings
IMAGE_DPI = 150
MAX_RETRIES = 3
RETRY_DELAY = 2
CLAUDE_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 4000

# Rate limiting (Claude Sonnet 4: 50 RPM)
RATE_LIMIT_RPM = 45
MIN_REQUEST_INTERVAL = 60.0 / RATE_LIMIT_RPM


class RateLimiter:
    """Sliding-window rate limiter to prevent API throttling."""

    def __init__(self, max_requests: int = 45, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.request_times: list[float] = []

    def wait_if_needed(self) -> None:
        current_time = time.time()
        self.request_times = [
            t for t in self.request_times
            if current_time - t < self.time_window
        ]
        if len(self.request_times) >= self.max_requests:
            sleep_time = self.time_window - (current_time - self.request_times[0])
            if sleep_time > 0:
                logger.info("Rate limit reached. Waiting %.1f seconds...", sleep_time)
                time.sleep(sleep_time)
                self.request_times.pop(0)
        self.request_times.append(current_time)


class BuildingContactExtractor:
    """Extract building and contact information from CoStar PDF pages using Claude."""

    def __init__(self, pdf_path: str, start_page: int, end_page: int, output_file: str):
        self.pdf_path = pdf_path
        self.start_page = start_page
        self.end_page = end_page
        self.output_file = output_file

        # Current building context (carried across pages)
        self.current_building: str | None = None
        self.current_building_address: str | None = None
        self.current_building_meta: dict[str, Any] = {}

        # Accumulated results
        self.all_rows: list[dict[str, Any]] = []
        self.buildings: list[dict[str, Any]] = []
        self._seen_buildings: set[str] = set()

        # API clients
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.rate_limiter = RateLimiter(max_requests=RATE_LIMIT_RPM, time_window=60)
        self.geocoding_cache = GeocodingCache()

        logger.info("Initialized extractor for pages %s-%s", start_page, end_page)

    def _page_to_png_bytes(self, doc: fitz.Document, page_num: int) -> bytes:
        """Convert a single PDF page to PNG bytes using PyMuPDF."""
        page = doc[page_num - 1]  # fitz uses 0-based indexing
        pix = page.get_pixmap(dpi=IMAGE_DPI)
        return pix.tobytes("png")

    def image_to_base64(self, png_bytes: bytes) -> str:
        """Convert PNG bytes to base64 string."""
        return base64.b64encode(png_bytes).decode("utf-8")

    def extract_from_page(self, png_bytes: bytes, page_number: int) -> Dict[str, Any]:
        """Extract building and contact information from a single page using Claude."""
        logger.info("Processing page %s...", page_number)

        base64_image = self.image_to_base64(png_bytes)

        prompt = """You are extracting contact information from a commercial real estate database page. Your task is to extract ONLY the information that is EXPLICITLY shown on this page.

CRITICAL RULES:
1. ONLY extract information you can SEE on the page
2. DO NOT invent, guess, or fabricate any information
3. If a field is not visible, use null
4. Extract EVERY contact shown on the page, no matter how minimal the information
5. Be precise with names, emails, and phone numbers - copy them exactly as shown

STRUCTURE TO EXTRACT:

**Building Information (from the TOP header of the page):**
- building_name: main heading at top (e.g. "50 California St - 50 California")
- building_address: the full subtitle line (e.g. "San Francisco, California 94111 (San Francisco County) - Financial District Submarket")
- building_city: city name (e.g. "San Francisco")
- building_state: state name (e.g. "California")
- building_zip: zip/postal code (e.g. "94111")
- building_county: county in parentheses (e.g. "San Francisco County")
- building_submarket: submarket after the dash (e.g. "Financial District")
- building_property_type: property type shown on the right (e.g. "Office")
- building_star_rating: number of filled stars shown (1-5 integer, count only filled/solid stars)

**Contacts (extract ALL contact sections):**
For each contact section (Leasing Company, Architect, Developer, Owner, Property Manager, etc.):
- contact_type: The section heading (e.g., "Leasing Company", "Architect", "Recorded Owner")
- contact_name: Individual person's name (if shown)
- contact_company: Company/organization name
- contact_job_title: Job title (if shown)
- contact_email: Email address (if shown)
- contact_phone: Phone number of the individual contact (if shown, in the same row as the person)
- company_address: Street address of the company (e.g. "415 Mission St, Suite 46th Floor")
- company_city: City of the company (e.g. "San Francisco")
- company_state: State abbreviation of the company (e.g. "CA")
- company_zip: Zip code of the company (e.g. "94105")
- company_country: Country of the company (e.g. "USA")
- company_phone: Main phone number of the company (shown below company name, NOT the individual contact's phone)
- company_website: Website of the company (e.g. "www.cbre.com")

IMPORTANT NOTES:
- Some contacts have full details (name, title, email, phone)
- Some contacts only have company name and phone
- Some contacts only have company name
- Extract them ALL, with whatever information is available
- If NO information for a field, use null
- company_phone is the company's main number, contact_phone is the individual's direct number
- Multiple contacts may share the same company info (same company_address, company_phone, etc.)

Return ONLY this JSON structure with NO additional text:
{
  "building_name": "exact name from page or null",
  "building_address": "exact subtitle line from page or null",
  "building_city": "city or null",
  "building_state": "state or null",
  "building_zip": "zip or null",
  "building_county": "county or null",
  "building_submarket": "submarket or null",
  "building_property_type": "property type or null",
  "building_star_rating": 3,
  "contacts": [
    {
      "contact_type": "section heading",
      "contact_name": "name or null",
      "contact_company": "company name",
      "contact_job_title": "title or null",
      "contact_email": "email or null",
      "contact_phone": "phone or null",
      "company_address": "street address or null",
      "company_city": "city or null",
      "company_state": "state or null",
      "company_zip": "zip or null",
      "company_country": "country or null",
      "company_phone": "company phone or null",
      "company_website": "website or null"
    }
  ]
}

Extract every contact you see, even if they only have minimal information."""

        for attempt in range(MAX_RETRIES):
            try:
                self.rate_limiter.wait_if_needed()

                response = self.client.messages.create(
                    model=CLAUDE_MODEL,
                    max_tokens=MAX_TOKENS,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": "image/png",
                                        "data": base64_image,
                                    },
                                },
                                {"type": "text", "text": prompt},
                            ],
                        }
                    ],
                )

                response_text = response.content[0].text.strip()

                if response_text.startswith("```json"):
                    response_text = response_text.split("```json")[1].split("```")[0].strip()
                elif response_text.startswith("```"):
                    response_text = response_text.split("```")[1].split("```")[0].strip()

                data = json.loads(response_text)

                # Carry forward building context from previous pages
                if not data.get("building_name") or data.get("building_name") == "null":
                    if self.current_building:
                        data["building_name"] = self.current_building
                        data["building_address"] = self.current_building_address
                        for k, v in self.current_building_meta.items():
                            data.setdefault(k, v)
                else:
                    self.current_building = data["building_name"]
                    self.current_building_address = data["building_address"]
                    self.current_building_meta = {
                        k: data.get(k)
                        for k in (
                            "building_city", "building_state", "building_zip",
                            "building_county", "building_submarket",
                            "building_property_type", "building_star_rating",
                        )
                    }

                data["page_number"] = page_number

                logger.info(
                    "Page %s: %s contacts extracted",
                    page_number,
                    len(data.get("contacts", [])),
                )
                return data

            except anthropic.RateLimitError:
                logger.warning("Rate limit hit on page %s, waiting 60s...", page_number)
                time.sleep(60)
                if attempt < MAX_RETRIES - 1:
                    continue

            except Exception as e:
                logger.error("Error on page %s, attempt %s: %s", page_number, attempt + 1, e)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    return {
                        "building_name": self.current_building,
                        "building_address": self.current_building_address,
                        "contacts": [],
                        "page_number": page_number,
                        "error": str(e),
                    }

        return {
            "building_name": self.current_building,
            "building_address": self.current_building_address,
            "contacts": [],
            "page_number": page_number,
            "error": "Max retries exceeded",
        }

    def _process_building(self, page_data: Dict[str, Any]) -> dict[str, Any] | None:
        """Register a new building (if not seen before) and geocode it.

        Returns the building dict with lat/lng, or None if already registered.
        """
        building_name = page_data.get("building_name")
        if not building_name or building_name in self._seen_buildings:
            return None

        self._seen_buildings.add(building_name)

        # Build a geocodable address string
        parts = []
        if building_name:
            # Extract just the street part (before " - " if present)
            street = building_name.split(" - ")[0].strip()
            parts.append(street)
        city = page_data.get("building_city")
        state = page_data.get("building_state")
        zip_code = page_data.get("building_zip")
        if city:
            parts.append(city)
        if state:
            parts.append(state)
        if zip_code:
            parts.append(zip_code)

        geocode_address = ", ".join(parts) if parts else None

        coords = self.geocoding_cache.get_or_geocode(geocode_address) if geocode_address else None

        building = {
            "building_name": building_name,
            "building_address": page_data.get("building_address"),
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "county": page_data.get("building_county"),
            "submarket": page_data.get("building_submarket"),
            "property_type": page_data.get("building_property_type"),
            "star_rating": page_data.get("building_star_rating"),
            "latitude": coords["latitude"] if coords else None,
            "longitude": coords["longitude"] if coords else None,
            "geocoded_at": datetime.now(timezone.utc).isoformat() if coords else None,
            "source_page": page_data.get("page_number"),
        }

        self.buildings.append(building)
        logger.info(
            "Registered building: %s (%s, %s)",
            building_name,
            building.get("latitude"),
            building.get("longitude"),
        )
        return building

    def _collect_rows(self, page_data: Dict[str, Any]) -> None:
        """Accumulate extracted rows in memory."""
        if "error" in page_data or not page_data.get("building_name"):
            return

        # Register building if new
        self._process_building(page_data)

        building_name = page_data["building_name"]
        building_address = page_data.get("building_address")
        page_number = page_data["page_number"]

        # Find the building's coordinates from our list
        building_coords = next(
            (b for b in self.buildings if b["building_name"] == building_name),
            {},
        )

        base_row = {
            "Building Name": building_name,
            "Building Address": building_address,
            "City": page_data.get("building_city"),
            "State": page_data.get("building_state"),
            "Zip Code": page_data.get("building_zip"),
            "County": page_data.get("building_county"),
            "Submarket": page_data.get("building_submarket"),
            "Property Type": page_data.get("building_property_type"),
            "Star Rating": page_data.get("building_star_rating"),
            "Latitude": building_coords.get("latitude"),
            "Longitude": building_coords.get("longitude"),
        }

        if not page_data.get("contacts"):
            self.all_rows.append({
                **base_row,
                "Contact Type": None,
                "Contact Name": None,
                "Contact Company": None,
                "Contact Job Title": None,
                "Contact Email": None,
                "Contact Phone": None,
                "Company Address": None,
                "Company City": None,
                "Company State": None,
                "Company Zip": None,
                "Company Country": None,
                "Company Phone": None,
                "Company Website": None,
                "Page": page_number,
            })
        else:
            for contact in page_data["contacts"]:
                self.all_rows.append({
                    **base_row,
                    "Contact Type": contact.get("contact_type"),
                    "Contact Name": contact.get("contact_name"),
                    "Contact Company": contact.get("contact_company"),
                    "Contact Job Title": contact.get("contact_job_title"),
                    "Contact Email": contact.get("contact_email"),
                    "Contact Phone": contact.get("contact_phone"),
                    "Company Address": contact.get("company_address"),
                    "Company City": contact.get("company_city"),
                    "Company State": contact.get("company_state"),
                    "Company Zip": contact.get("company_zip"),
                    "Company Country": contact.get("company_country"),
                    "Company Phone": contact.get("company_phone"),
                    "Company Website": contact.get("company_website"),
                    "Page": page_number,
                })

    def save_to_excel(self) -> None:
        """Write all accumulated rows to Excel in one pass."""
        if not self.all_rows:
            logger.warning("No rows to write to Excel")
            return
        df = pd.DataFrame(self.all_rows)
        df.to_excel(self.output_file, index=False, engine="openpyxl")
        logger.info("Wrote %s rows to %s", len(self.all_rows), self.output_file)

    def process_pdf(self) -> str:
        """Process all pages: extract contacts and write results to Excel."""
        logger.info(
            "Building Contact Extraction Started — PDF: %s, pages: %s-%s, output: %s",
            self.pdf_path, self.start_page, self.end_page, self.output_file,
        )

        total_pages = self.end_page - self.start_page + 1
        processed = 0
        errors = 0

        doc = fitz.open(self.pdf_path)
        try:
            for page_num in range(self.start_page, self.end_page + 1):
                try:
                    png_bytes = self._page_to_png_bytes(doc, page_num)
                    page_data = self.extract_from_page(png_bytes, page_num)
                    self._collect_rows(page_data)

                    if "error" in page_data:
                        errors += 1
                    processed += 1

                    if processed % 5 == 0:
                        logger.info(
                            "Progress: %s/%s (%.1f%%) | Errors: %s | Buildings: %s",
                            processed, total_pages, processed / total_pages * 100,
                            errors, len(self.buildings),
                        )

                except Exception as e:
                    logger.error("Error on page %s: %s", page_num, e)
                    errors += 1
                    continue
        finally:
            doc.close()

        self.save_to_excel()

        logger.info(
            "Extraction complete — processed: %s/%s, errors: %s, buildings: %s, contacts: %s, output: %s",
            processed, total_pages, errors, len(self.buildings), len(self.all_rows),
            self.output_file,
        )
        return str(self.output_file)
