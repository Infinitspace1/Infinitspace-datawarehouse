"""
shared/real_estate/building_contact_extractor.py

PDF Building Contact Extractor for Azure Functions.
Uses PyMuPDF (fitz) for PDF-to-image conversion — no system dependencies.
Uses Anthropic Claude API for contact extraction from page images.

Adapted from AI-REAL-ESTATE/extract_building_contacts_improved.py.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import time
from io import BytesIO
from typing import Any, Dict

import anthropic
import fitz  # PyMuPDF
import pandas as pd
from PIL import Image

logger = logging.getLogger(__name__)

# Extraction settings
IMAGE_DPI = 150
MAX_RETRIES = 3
RETRY_DELAY = 2
CLAUDE_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 2000

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
        self.current_building: str | None = None
        self.current_building_address: str | None = None
        self.all_rows: list[dict[str, Any]] = []

        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.rate_limiter = RateLimiter(max_requests=RATE_LIMIT_RPM, time_window=60)

        logger.info("Initialized extractor for pages %s-%s", start_page, end_page)

    def _page_to_image(self, doc: fitz.Document, page_num: int) -> Image.Image:
        """Convert a single PDF page to a PIL Image using PyMuPDF."""
        page = doc[page_num - 1]  # fitz uses 0-based indexing
        pix = page.get_pixmap(dpi=IMAGE_DPI)
        return Image.open(BytesIO(pix.tobytes("png")))

    def image_to_base64(self, image: Image.Image) -> str:
        """Convert PIL Image to base64 string."""
        buffered = BytesIO()
        image.save(buffered, format="PNG", optimize=True)
        return base64.b64encode(buffered.getvalue()).decode("utf-8")

    def extract_from_page(self, image: Image.Image, page_number: int) -> Dict[str, Any]:
        """Extract building and contact information from a single page using Claude."""
        logger.info("Processing page %s...", page_number)

        base64_image = self.image_to_base64(image)

        prompt = """You are extracting contact information from a commercial real estate database page. Your task is to extract ONLY the information that is EXPLICITLY shown on this page.

CRITICAL RULES:
1. ONLY extract information you can SEE on the page
2. DO NOT invent, guess, or fabricate any information
3. If a field is not visible, use null
4. Extract EVERY contact shown on the page, no matter how minimal the information
5. Be precise with names, emails, and phone numbers - copy them exactly as shown

STRUCTURE TO EXTRACT:

**Building Information (from the TOP of the page):**
- Building name (main heading at top)
- Building address (full address with city, state, zip, district if shown)

**Contacts (extract ALL contact sections):**
For each contact section (Leasing Company, Architect, Developer, Owner, Property Manager, etc.):
- contact_type: The section heading (e.g., "Leasing Company", "Architect", "Recorded Owner")
- contact_name: Individual person's name (if shown)
- contact_company: Company/organization name
- contact_job_title: Job title (if shown)
- contact_email: Email address (if shown)
- contact_phone: Phone number (if shown)

IMPORTANT NOTES:
- Some contacts have full details (name, title, email, phone)
- Some contacts only have company name and phone
- Some contacts only have company name
- Extract them ALL, with whatever information is available
- If NO information for a field, use null

Return ONLY this JSON structure with NO additional text:
{
  "building_name": "exact name from page",
  "building_address": "exact address from page",
  "contacts": [
    {
      "contact_type": "section heading",
      "contact_name": "name or null",
      "contact_company": "company name",
      "contact_job_title": "title or null",
      "contact_email": "email or null",
      "contact_phone": "phone or null"
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

                if not data.get("building_name") or data.get("building_name") == "null":
                    if self.current_building:
                        data["building_name"] = self.current_building
                        data["building_address"] = self.current_building_address
                else:
                    self.current_building = data["building_name"]
                    self.current_building_address = data["building_address"]

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

    def _collect_rows(self, page_data: Dict[str, Any]) -> None:
        """Accumulate extracted rows in memory."""
        if "error" in page_data or not page_data.get("building_name"):
            return

        building_name = page_data["building_name"]
        building_address = page_data["building_address"]
        page_number = page_data["page_number"]

        if not page_data.get("contacts"):
            self.all_rows.append({
                "Building Name": building_name,
                "Building Address": building_address,
                "Contact Type": None,
                "Contact Name": None,
                "Contact Company": None,
                "Contact Job Title": None,
                "Contact Email": None,
                "Contact Phone": None,
                "Page": page_number,
            })
        else:
            for contact in page_data["contacts"]:
                self.all_rows.append({
                    "Building Name": building_name,
                    "Building Address": building_address,
                    "Contact Type": contact.get("contact_type"),
                    "Contact Name": contact.get("contact_name"),
                    "Contact Company": contact.get("contact_company"),
                    "Contact Job Title": contact.get("contact_job_title"),
                    "Contact Email": contact.get("contact_email"),
                    "Contact Phone": contact.get("contact_phone"),
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
                    image = self._page_to_image(doc, page_num)
                    page_data = self.extract_from_page(image, page_num)
                    self._collect_rows(page_data)

                    if "error" in page_data:
                        errors += 1
                    processed += 1

                    if processed % 5 == 0:
                        logger.info(
                            "Progress: %s/%s (%.1f%%) | Errors: %s",
                            processed, total_pages, processed / total_pages * 100, errors,
                        )

                except Exception as e:
                    logger.error("Error on page %s: %s", page_num, e)
                    errors += 1
                    continue
        finally:
            doc.close()

        self.save_to_excel()

        logger.info(
            "Extraction complete — processed: %s/%s, errors: %s, output: %s",
            processed, total_pages, errors, self.output_file,
        )
        return str(self.output_file)
