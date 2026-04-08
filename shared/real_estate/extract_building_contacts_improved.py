#!/usr/bin/env python3
"""
PDF Building Contact Extractor - Production Version
Enhanced with better error handling, rate limiting, and progress tracking.
"""

import json
import base64
import time
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd
from pdf2image import convert_from_path
import anthropic
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime
import logging

load_dotenv()

# Configuration
START_PAGE = 210
END_PAGE = 776
IMAGE_DPI = 150
API_DELAY = 0.5
OUTPUT_FILENAME = 'building_contacts.xlsx'
PROGRESS_UPDATE_EVERY = 5
VERBOSE = True
MAX_RETRIES = 3
RETRY_DELAY = 2
CLAUDE_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 2000

# Rate limiting (Claude Sonnet 4: 50 RPM)
RATE_LIMIT_RPM = 45  # Slightly below limit for safety
MIN_REQUEST_INTERVAL = 60.0 / RATE_LIMIT_RPM


def setup_logging(log_file: str = "extraction.log"):
    """Setup logging to both file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def _find_poppler_path():
    """Find poppler installation path - Docker friendly version."""
    import shutil
    
    # First check if pdftoppm is in PATH (works for Docker and most Linux systems)
    if shutil.which('pdftoppm'):
        # Return None - pdf2image will use PATH
        return None
    
    # Fallback: check common paths
    possible_paths = [
        "/usr/bin",                                    # Docker/Linux default
        "/usr/local/bin",                              # Alternative Linux
        r"C:\Program Files\poppler\Library\bin",      # Windows
        r"C:\poppler\Library\bin",                    # Windows alternative
        "/opt/homebrew/bin",                          # macOS (ARM)
        "/usr/local/Cellar/poppler",                  # macOS (Intel)
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            pdftoppm = os.path.join(path, 'pdftoppm.exe' if os.name == 'nt' else 'pdftoppm')
            if os.path.exists(pdftoppm):
                logging.info(f"Found poppler at: {path}")
                return path
    
    return None


class RateLimiter:
    """Rate limiter to prevent API throttling."""
    
    def __init__(self, max_requests: int = 45, time_window: int = 60):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum number of requests allowed
            time_window: Time window in seconds (default: 60)
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.request_times = []
    
    def wait_if_needed(self):
        """Wait if necessary to maintain rate limit."""
        import time
        
        current_time = time.time()
        
        # Remove old requests outside the time window
        self.request_times = [
            t for t in self.request_times 
            if current_time - t < self.time_window
        ]
        
        # If at limit, wait
        if len(self.request_times) >= self.max_requests:
            sleep_time = self.time_window - (current_time - self.request_times[0])
            if sleep_time > 0:
                logging.info(f"Rate limit reached. Waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
                # Remove the oldest request
                self.request_times.pop(0)
        
        # Record this request
        self.request_times.append(current_time)


class ProgressTracker:
    """Track and save extraction progress."""
    
    def __init__(self, progress_file: str = "extraction_progress.json"):
        self.progress_file = progress_file
        self.data = self.load()
    
    def load(self) -> Dict:
        """Load progress from file."""
        if Path(self.progress_file).exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {
            "last_processed_page": None,
            "total_processed": 0,
            "errors": [],
            "start_time": None,
            "last_update": None
        }
    
    def save(self):
        """Save progress to file."""
        self.data["last_update"] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def update_page(self, page_num: int, has_error: bool = False):
        """Update progress for a processed page."""
        self.data["last_processed_page"] = page_num
        self.data["total_processed"] += 1
        if has_error:
            self.data["errors"].append(page_num)
        self.save()
    
    def get_resume_page(self, start_page: int) -> int:
        """Get the page to resume from."""
        if self.data["last_processed_page"] is not None:
            return self.data["last_processed_page"] + 1
        return start_page


class BuildingContactExtractor:
    """Extract building and contact information with maximum accuracy using Claude."""
    
    def __init__(self, pdf_path: str, start_page: int, end_page: int, output_file: str):
        """
        Initialize the extractor.
        
        Args:
            pdf_path: Path to PDF file
            start_page: First page to extract
            end_page: Last page to extract
            output_file: Output Excel file path
        """
        self.pdf_path = pdf_path
        self.start_page = start_page
        self.end_page = end_page
        self.output_file = output_file
        self.current_building = None
        self.current_building_address = None
        self.logger = logging.getLogger(__name__)
        
        # Initialize API client
        self.client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
        
        # Rate limiter for Claude API (50 RPM limit for Sonnet 4)
        self.rate_limiter = RateLimiter(max_requests=45, time_window=60)
        
        # Progress tracker
        self.progress_tracker = ProgressTracker(f"{output_file}.progress.json")
        
        # Check for poppler - Docker-friendly version
        import shutil
        if not shutil.which('pdftoppm'):
            # Try to provide helpful error message
            import subprocess
            try:
                result = subprocess.run(['which', 'pdftoppm'], 
                                    capture_output=True, text=True, timeout=5)
                logging.error(f"Poppler check: {result.stdout} {result.stderr}")
            except:
                pass
            raise RuntimeError("Poppler not found. Please install poppler-utils.")
        
        # For Docker/Linux, pdf2image can find poppler in PATH automatically
        self.poppler_path = None
        self.logger.info("Poppler found and accessible via PATH")
        
        self.logger.info(f"Initialized extractor for pages {start_page}-{end_page}")
    
    def image_to_base64(self, image) -> str:
        """Convert PIL Image to base64 string."""
        buffered = BytesIO()
        image.save(buffered, format="PNG", optimize=True)
        img_bytes = buffered.getvalue()
        return base64.b64encode(img_bytes).decode('utf-8')
    
    def extract_from_page(self, image, page_number: int) -> Dict[str, Any]:
        """Extract building and contact information from a single page using Claude."""
        self.logger.info(f"Processing page {page_number}...")
        
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
                # Rate limiting
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
                                        "data": base64_image
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": prompt
                                }
                            ]
                        }
                    ]
                )
                
                response_text = response.content[0].text.strip()
                
                # Remove markdown code blocks if present
                if response_text.startswith("```json"):
                    response_text = response_text.split("```json")[1].split("```")[0].strip()
                elif response_text.startswith("```"):
                    response_text = response_text.split("```")[1].split("```")[0].strip()
                
                # Parse JSON
                data = json.loads(response_text)
                
                # Handle continuation pages
                if not data.get('building_name') or data.get('building_name') == 'null':
                    if self.current_building:
                        data['building_name'] = self.current_building
                        data['building_address'] = self.current_building_address
                else:
                    self.current_building = data['building_name']
                    self.current_building_address = data['building_address']
                
                data['page_number'] = page_number
                
                self.logger.info(f"✅ Page {page_number}: {len(data.get('contacts', []))} contacts")
                return data
                
            except anthropic.RateLimitError as e:
                self.logger.warning(f"Rate limit hit on page {page_number}, waiting 60s...")
                time.sleep(60)
                if attempt < MAX_RETRIES - 1:
                    continue
                    
            except Exception as e:
                self.logger.error(f"Error on page {page_number}, attempt {attempt + 1}: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
                else:
                    return {
                        "building_name": self.current_building,
                        "building_address": self.current_building_address,
                        "contacts": [],
                        "page_number": page_number,
                        "error": str(e)
                    }
        
        return {
            "building_name": self.current_building,
            "building_address": self.current_building_address,
            "contacts": [],
            "page_number": page_number,
            "error": "Max retries exceeded"
        }
    
    def append_to_excel(self, page_data: Dict[str, Any]):
        """Append extracted data to Excel file immediately after processing each page."""
        if 'error' in page_data or not page_data.get('building_name'):
            return
        
        building_name = page_data['building_name']
        building_address = page_data['building_address']
        page_number = page_data['page_number']
        
        rows = []
        
        if not page_data.get('contacts'):
            rows.append({
                'Building Name': building_name,
                'Building Address': building_address,
                'Contact Type': None,
                'Contact Name': None,
                'Contact Company': None,
                'Contact Job Title': None,
                'Contact Email': None,
                'Contact Phone': None,
                'Page': page_number
            })
        else:
            for contact in page_data['contacts']:
                rows.append({
                    'Building Name': building_name,
                    'Building Address': building_address,
                    'Contact Type': contact.get('contact_type'),
                    'Contact Name': contact.get('contact_name'),
                    'Contact Company': contact.get('contact_company'),
                    'Contact Job Title': contact.get('contact_job_title'),
                    'Contact Email': contact.get('contact_email'),
                    'Contact Phone': contact.get('contact_phone'),
                    'Page': page_number
                })
        
        new_df = pd.DataFrame(rows)
        
        try:
            if Path(self.output_file).exists():
                existing_df = pd.read_excel(self.output_file, engine='openpyxl')
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            combined_df.to_excel(self.output_file, index=False, engine='openpyxl')
            
        except Exception as e:
            self.logger.error(f"Error appending to Excel: {e}")
    
    def process_pdf(self, resume: bool = False) -> str:
        """Main processing function - processes one page at a time."""
        self.logger.info("="*80)
        self.logger.info("🚀 Building Contact Extraction Started")
        self.logger.info(f"📁 PDF: {self.pdf_path}")
        self.logger.info(f"📄 Pages: {self.start_page} to {self.end_page}")
        self.logger.info(f"💾 Output: {self.output_file}")
        self.logger.info("="*80)
        
        # Resume logic
        actual_start_page = self.start_page
        if resume and self.progress_tracker.data["last_processed_page"]:
            actual_start_page = self.progress_tracker.get_resume_page(self.start_page)
            self.logger.info(f"📍 Resuming from page {actual_start_page}")
        else:
            # Fresh start - clear output file
            if Path(self.output_file).exists():
                Path(self.output_file).unlink()
                self.logger.info("🗑️  Cleared previous output file")
            self.progress_tracker = ProgressTracker(f"{self.output_file}.progress.json")  # Reset progress

        if not self.progress_tracker.data["start_time"]:
            self.progress_tracker.data["start_time"] = datetime.now().isoformat()
            self.progress_tracker.save()
        
        total_pages = self.end_page - actual_start_page + 1
        processed = 0
        
        try:
            # Process pages one at a time
            for page_num in range(actual_start_page, self.end_page + 1):
                try:
                    # Convert single page to image
                    images = convert_from_path(
                        self.pdf_path,
                        first_page=page_num,
                        last_page=page_num,
                        dpi=IMAGE_DPI,
                        poppler_path=self.poppler_path
                    )
                    
                    if not images:
                        self.logger.warning(f"No image for page {page_num}")
                        self.progress_tracker.update_page(page_num, has_error=True)
                        continue
                    
                    image = images[0]
                    
                    # Extract data
                    page_data = self.extract_from_page(image, page_num)
                    
                    # Append to Excel
                    self.append_to_excel(page_data)
                    
                    # Update progress
                    has_error = 'error' in page_data
                    self.progress_tracker.update_page(page_num, has_error=has_error)
                    
                    processed += 1
                    
                    # Progress update
                    if processed % PROGRESS_UPDATE_EVERY == 0:
                        progress_pct = processed / total_pages * 100
                        self.logger.info(f"📊 Progress: {processed}/{total_pages} ({progress_pct:.1f}%) | Errors: {len(self.progress_tracker.data['errors'])}")
                    
                except KeyboardInterrupt:
                    self.logger.info(f"⚠️  Interrupted at page {page_num}")
                    raise
                    
                except Exception as e:
                    self.logger.error(f"Error on page {page_num}: {str(e)}")
                    self.progress_tracker.update_page(page_num, has_error=True)
                    continue
            
            # Final summary
            self.logger.info("="*80)
            self.logger.info("✅ Extraction Complete!")
            self.logger.info(f"📊 Processed: {processed}/{total_pages} pages")
            self.logger.info(f"⚠️  Errors: {len(self.progress_tracker.data['errors'])} pages")
            self.logger.info(f"💾 Output: {Path(self.output_file).absolute()}")
            self.logger.info("="*80)
            
        except KeyboardInterrupt:
            self.logger.info("✅ Progress saved. Run with --resume to continue.")
        
        return str(self.output_file)


def main():
    """Main function."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python extract_building_contacts_improved.py <pdf_file> [start_page] [end_page] [--resume]")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    start_page = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] != '--resume' else START_PAGE
    end_page = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] != '--resume' else END_PAGE
    resume = '--resume' in sys.argv
    
    if not Path(pdf_path).exists():
        print(f"❌ Error: PDF file not found: {pdf_path}")
        sys.exit(1)
    
    extractor = BuildingContactExtractor(
        pdf_path=pdf_path,
        start_page=start_page,
        end_page=end_page
    )
    
    output_path = extractor.process_pdf(resume=resume)
    print(f"✅ Complete! Output: {output_path}")


if __name__ == "__main__":
    main()
