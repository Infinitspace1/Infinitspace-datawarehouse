"""
membership_agreement_test/extract.py

Classifies each downloaded PDF as "new" (< 10 pages) or "old" (>= 10 pages),
then uses the Claude API to extract contract details page-by-page (1 API call
per page) and stores the results as JSON + a summary CSV.

Output:
  membership_agreement_test/extracted/{stem}.json   — per-PDF result
  membership_agreement_test/extracted/summary.csv   — all results in one table

Usage:
    python membership_agreement_test/extract.py                          # process all
    python membership_agreement_test/extract.py --test STEM              # test 1 file
    python membership_agreement_test/extract.py --reprocess-section5     # re-run section 5 on all old contracts
"""
import argparse
import base64
import csv
import json
import logging
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import anthropic
import fitz  # PyMuPDF

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

PDF_DIR       = Path(__file__).resolve().parent / "pdfs"
EXTRACTED_DIR = Path(__file__).resolve().parent / "extracted"
MODEL         = "claude-sonnet-4-6"
DPI           = 150  # resolution for page rendering

# ── Hardcoded values for new contracts ───────────────────────────────────────

NEW_CONTRACT_RENEWAL_SYSTEM = "same duration as the Initial Commitment Term"

NEW_CONTRACT_TERMINATION_TABLE = {
    "1-11 months":  {"0-24": "2 months", "25-74": "2 months", "75+": "3 months"},
    "12-23 months": {"0-24": "2 months", "25-74": "3 months", "75+": "6 months"},
    "24+ months":   {"0-24": "3 months", "25-74": "6 months", "75+": "6 months"},
}

# ── Claude prompts ────────────────────────────────────────────────────────────

PROMPT_PAGE_1 = """\
This is page 1 of a membership agreement. Find the section called \
"Contract term details" (or similar heading) and extract the following fields:
- start_date
- end_date
- additional_commitment_term_options (any options or flexibility mentioned for commitment length)
- termination_notice_timeline (any termination notice period or timeline requirement)

Return ONLY a valid JSON object with exactly these keys. Use null for any field \
you cannot find. Do not include any explanation or markdown fencing.
Example: {"start_date":"2024-01-01","end_date":"2024-12-31","additional_commitment_term_options":null,"termination_notice_timeline":"2 months"}"""

PROMPT_PAGE_2 = """\
This is page 2 of a membership agreement. Find the section called \
"Additional Items (If applicable)" and inside it look for any "Additional Notes".
Extract:
- additional_notes: any notes about renewal, different dates, different time periods, \
  special conditions, or other relevant information. If multiple notes exist, \
  combine them into a single string.

Return ONLY a valid JSON object with exactly this key. Use null if the section \
or notes are not present. Do not include any explanation or markdown fencing.
Example: {"additional_notes":"Auto-renews for 12 months unless 2 months notice given."}"""

PROMPT_FIND_SECTION_5 = """\
This is a page from a membership agreement. Does this page contain section \
"5. TERM AND TERMINATION" (or "Section 5" with that title)?

If YES:
1. From subsection "a. Term": copy the complete sentence(s) that describe what \
   happens after the initial commitment term ends (e.g. auto-renewal, \
   month-to-month continuation, etc.). Copy the exact wording.
2. From subsection "f. Member Company Termination Notice Periods Required:": \
   extract the full termination notice table if it appears on this page. \
   The table has rows for commitment term durations (e.g. "1-11 months", \
   "12-23 months", "24+ months") and columns for member count ranges \
   (e.g. "0-24", "25-74", "75+"). Each cell contains a notice period \
   like "1 month" or "3 months".

Return ONLY valid JSON (no markdown):
- If section IS present:
  {"has_section_5": true, "renewal_text": "...", "termination_notice_table": {"1-11 months": {"0-24": "...", ...}, ...}}
  Use null for termination_notice_table if subsection f is not visible on this page.
- If section IS NOT present:
  {"has_section_5": false}"""

PROMPT_TABLE_CONTINUATION = """\
This page may contain all or part of a termination notice table from a \
membership agreement. The table may be titled something like \
"Member Company Termination Notice Periods Required" or may be an \
unlabelled continuation of such a table.

Extract every row and column you can see. Rows represent commitment term \
durations (e.g. "1-11 months", "12-23 months", "24+ months") and columns \
represent member count ranges (e.g. "0-24", "25-74", "75+"). \
Each cell contains a notice period like "1 month" or "3 months".

Return ONLY valid JSON (no markdown):
- {"has_table": true, "termination_notice_table": {"1-11 months": {"0-24": "...", "25-74": "...", "75+": "..."}, ...}}
- {"has_table": false}  — if there is no such table on this page."""


# ── Renewal classification ────────────────────────────────────────────────────

def classify_renewal(text: str) -> str:
    """
    Map raw renewal text to one of three categories:
      - "monthly"                       e.g. "month-to-month basis"
      - "same_period_initial_commitment" e.g. "same duration as the Initial Commitment Term"
      - "personalized"                   anything else / custom term
    """
    if not text:
        return "unknown"
    t = text.lower()
    if any(p in t for p in ["month-to-month", "month to month", "monthly basis", "month basis"]):
        return "monthly"
    if any(p in t for p in ["same duration", "same period", "initial commitment term", "same term"]):
        return "same_period_initial_commitment"
    return "personalized"


def merge_tables(base: dict, extra: dict) -> dict:
    """Merge two partial termination notice tables; base takes priority on conflicts."""
    merged = {**base}
    for row_key, cols in (extra or {}).items():
        if row_key not in merged:
            merged[row_key] = cols
        else:
            for col_key, val in cols.items():
                if col_key not in merged[row_key]:
                    merged[row_key][col_key] = val
    return merged


# ── Helpers ───────────────────────────────────────────────────────────────────

def page_to_base64(page: fitz.Page) -> str:
    """Render a PDF page to a base64-encoded PNG string."""
    pix = page.get_pixmap(dpi=DPI)
    return base64.standard_b64encode(pix.tobytes("png")).decode()


def image_file_to_base64(path: Path) -> str:
    """Base64-encode an image file directly."""
    return base64.standard_b64encode(path.read_bytes()).decode()


def call_claude(client: anthropic.Anthropic, image_b64: str, prompt: str) -> dict:
    """Send one page image + prompt to Claude and return parsed JSON."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": image_b64,
                        },
                    },
                    {"type": "text", "text": prompt},
                ],
            }
        ],
    )
    raw = response.content[0].text.strip()
    # Strip any accidental markdown fencing
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(f"  JSON parse failed — storing raw response")
        return {"raw_response": raw}


def parse_stem(stem: str) -> tuple[str, str, str]:
    """Extract coworker_id, coworker_name, file_id from filename stem."""
    parts = stem.split("_", 2)
    coworker_id   = parts[0] if len(parts) > 0 else stem
    file_id       = parts[-1] if len(parts) > 1 else ""
    coworker_name = parts[1] if len(parts) > 2 else ""
    return coworker_id, coworker_name, file_id


# ── Section 5 scan ────────────────────────────────────────────────────────────

def scan_section5(
    client: anthropic.Anthropic,
    doc: fitz.Document,
    total_pages: int,
) -> tuple[dict | None, dict | None, int | None]:
    """
    Scans pages 3→N looking for "5. TERM AND TERMINATION".

    When found on page P:
      - Extracts renewal text from subsection a and classifies it.
      - Extracts the termination notice table from page P.
      - Always also checks page P+1 for table continuation and merges.

    Returns (renewal_system_dict, termination_notice_table, 1-based page number)
    or (None, None, None) if section not found.
    """
    for page_num in range(2, total_pages):  # 0-indexed, page 3 onward
        logger.info(f"  Scanning page {page_num + 1}/{total_pages} for Section 5")
        img_b64 = page_to_base64(doc[page_num])
        parsed  = call_claude(client, img_b64, PROMPT_FIND_SECTION_5)

        if not parsed.get("has_section_5"):
            continue

        logger.info(f"  Section 5 found on page {page_num + 1}")

        # ── Renewal system ────────────────────────────────────────────────────
        renewal_text     = parsed.get("renewal_text") or ""
        renewal_category = classify_renewal(renewal_text)
        renewal = {"category": renewal_category, "raw_text": renewal_text}

        # ── Termination table — this page ─────────────────────────────────────
        table = parsed.get("termination_notice_table") or {}

        # ── Termination table — next page (always check for continuation) ─────
        if page_num + 1 < total_pages:
            logger.info(
                f"  Checking page {page_num + 2} for table continuation"
            )
            next_img = page_to_base64(doc[page_num + 1])
            next_parsed = call_claude(client, next_img, PROMPT_TABLE_CONTINUATION)
            if next_parsed.get("has_table"):
                extra = next_parsed.get("termination_notice_table") or {}
                table = merge_tables(table, extra)
                logger.info(f"  Table continuation merged from page {page_num + 2}")

        return renewal, table, page_num + 1  # 1-based

    return None, None, None


# ── Core extraction ───────────────────────────────────────────────────────────

def extract_pdf(client: anthropic.Anthropic, pdf_path: Path) -> dict:
    """Run the full extraction pipeline for one PDF. Returns the result dict."""
    stem = pdf_path.stem
    coworker_id, coworker_name, file_id = parse_stem(stem)

    is_png = pdf_path.suffix.lower() == ".png"

    if is_png:
        total_pages = 1
        contract_type = "new"
    else:
        doc = fitz.open(str(pdf_path))
        total_pages = len(doc)
        contract_type = "new" if total_pages < 10 else "old"

    logger.info(
        f"  Type={contract_type}  pages={total_pages}"
        f"{'  [PNG]' if is_png else ''}"
    )

    result: dict = {
        "coworker_id":   coworker_id,
        "coworker_name": coworker_name,
        "file_id":       file_id,
        "pdf_path":      str(pdf_path),
        "contract_type": contract_type,
        "total_pages":   total_pages,
    }

    # ── Page 1 ────────────────────────────────────────────────────────────────
    logger.info("  Analysing page 1 (Contract term details)")
    if is_png:
        img_b64 = image_file_to_base64(pdf_path)
    else:
        img_b64 = page_to_base64(doc[0])
    result["page_1"] = call_claude(client, img_b64, PROMPT_PAGE_1)

    # ── Page 2 (only if the document has at least 2 pages) ────────────────────
    if not is_png and total_pages >= 2:
        logger.info("  Analysing page 2 (Additional Items / Additional Notes)")
        img_b64 = page_to_base64(doc[1])
        result["page_2"] = call_claude(client, img_b64, PROMPT_PAGE_2)
    else:
        result["page_2"] = {"additional_notes": None}

    # ── Hardcoded values for new contracts ────────────────────────────────────
    if contract_type == "new":
        result["renewal_system"]           = NEW_CONTRACT_RENEWAL_SYSTEM
        result["termination_notice_table"] = NEW_CONTRACT_TERMINATION_TABLE
        result["section_5_found_on_page"]  = None

    # ── Section 5 scan for old contracts ─────────────────────────────────────
    else:
        renewal, table, found_page = scan_section5(client, doc, total_pages)
        result["renewal_system"]           = renewal
        result["termination_notice_table"] = table
        result["section_5_found_on_page"]  = found_page
        if found_page is None:
            logger.warning("  Section 5 NOT found in this document")

    if not is_png:
        doc.close()

    return result


# ── Summary CSV ───────────────────────────────────────────────────────────────

CSV_FIELDS = [
    "coworker_id", "coworker_name", "file_id", "contract_type", "total_pages",
    "start_date", "end_date",
    "additional_commitment_term_options", "termination_notice_timeline",
    "additional_notes",
    "renewal_system", "renewal_system_raw_text", "section_5_found_on_page",
    "pdf_path",
]


def flatten_for_csv(result: dict) -> dict:
    row = {k: result.get(k, "") for k in CSV_FIELDS}
    p1 = result.get("page_1") or {}
    p2 = result.get("page_2") or {}
    row["start_date"]                          = p1.get("start_date", "")
    row["end_date"]                            = p1.get("end_date", "")
    row["additional_commitment_term_options"]  = p1.get("additional_commitment_term_options", "")
    row["termination_notice_timeline"]         = p1.get("termination_notice_timeline", "")
    row["additional_notes"]                    = p2.get("additional_notes", "")
    # renewal_system is now a dict for old contracts; flatten to category + raw_text
    rs = result.get("renewal_system")
    if isinstance(rs, dict):
        row["renewal_system"]          = rs.get("category", "")
        row["renewal_system_raw_text"] = rs.get("raw_text", "")
    else:
        row["renewal_system"]          = rs or ""
        row["renewal_system_raw_text"] = ""
    return row


def write_summary_csv(results: list[dict], csv_path: Path) -> None:
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(flatten_for_csv(r))
    logger.info(f"Summary CSV written: {csv_path}")


# ── Reprocess section 5 only ─────────────────────────────────────────────────

def reprocess_section5_for_pdf(
    client: anthropic.Anthropic, pdf_path: Path, out_path: Path
) -> dict:
    """Load existing JSON, re-run section 5 extraction, save updated JSON."""
    existing    = json.loads(out_path.read_text(encoding="utf-8"))
    doc         = fitz.open(str(pdf_path))
    total_pages = len(doc)

    renewal, table, found_page = scan_section5(client, doc, total_pages)
    doc.close()

    existing["renewal_system"]           = renewal
    existing["termination_notice_table"] = table
    existing["section_5_found_on_page"]  = found_page

    if found_page is None:
        logger.warning("  Section 5 NOT found in this document")

    out_path.write_text(
        json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logger.info(f"  Updated: {out_path.name}")
    return existing


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test", metavar="STEM",
        help="Re-run section 5 on ONE file by stem (filename without extension). "
             "Prints result and updates the JSON.",
    )
    parser.add_argument(
        "--reprocess-section5", action="store_true",
        help="Re-run section 5 extraction on ALL old contracts (updates existing JSONs).",
    )
    args = parser.parse_args()

    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
    client = anthropic.Anthropic()

    # ── Test mode: single file ────────────────────────────────────────────────
    if args.test:
        stem    = args.test
        matches = list(PDF_DIR.glob(f"{stem}.*"))
        if not matches:
            logger.error(f"No file found in {PDF_DIR} with stem: {stem}")
            return
        pdf_path = matches[0]
        out_path = EXTRACTED_DIR / f"{stem}.json"
        if not out_path.exists():
            logger.error(f"No existing JSON for {stem} — run full extraction first.")
            return

        logger.info(f"TEST — reprocessing section 5 for: {pdf_path.name}")
        result = reprocess_section5_for_pdf(client, pdf_path, out_path)
        print(f"\n  renewal_system           : {json.dumps(result.get('renewal_system'), indent=4)}")
        print(f"  termination_notice_table : {json.dumps(result.get('termination_notice_table'), indent=4)}")
        print(f"  section_5_found_on_page  : {result.get('section_5_found_on_page')}")
        return

    # ── Reprocess section 5 for all old contracts ─────────────────────────────
    if args.reprocess_section5:
        json_files = sorted(EXTRACTED_DIR.glob("*.json"))
        old_jsons  = [
            j for j in json_files
            if json.loads(j.read_text(encoding="utf-8")).get("contract_type") == "old"
        ]
        logger.info(f"Reprocessing section 5 for {len(old_jsons)} old contracts")
        updated = errors = 0
        for i, out_path in enumerate(old_jsons, 1):
            stem    = out_path.stem
            matches = list(PDF_DIR.glob(f"{stem}.*"))
            if not matches:
                logger.warning(f"[{i}/{len(old_jsons)}] PDF not found for {stem} — skipping")
                continue
            logger.info(f"[{i}/{len(old_jsons)}] {matches[0].name}")
            try:
                reprocess_section5_for_pdf(client, matches[0], out_path)
                updated += 1
            except Exception as exc:
                logger.error(f"  FAILED: {exc}")
                errors += 1

        all_results = [
            json.loads(j.read_text(encoding="utf-8"))
            for j in sorted(EXTRACTED_DIR.glob("*.json"))
        ]
        if all_results:
            write_summary_csv(all_results, EXTRACTED_DIR / "summary.csv")

        print(f"\n{'─'*60}")
        print(f"  Updated  : {updated}")
        print(f"  Errors   : {errors}")
        return

    # ── Normal mode: process all files ───────────────────────────────────────
    files = sorted(PDF_DIR.glob("*.pdf")) + sorted(PDF_DIR.glob("*.png"))
    if not files:
        logger.error(f"No files found in {PDF_DIR}")
        return

    all_results: list[dict] = []
    skipped = processed = errors = 0

    for i, pdf_path in enumerate(files, 1):
        out_path = EXTRACTED_DIR / f"{pdf_path.stem}.json"
        logger.info(f"[{i}/{len(files)}] {pdf_path.name}")

        if out_path.exists():
            logger.info("  Already extracted — loading cached result")
            all_results.append(json.loads(out_path.read_text(encoding="utf-8")))
            skipped += 1
            continue

        try:
            result = extract_pdf(client, pdf_path)
            out_path.write_text(
                json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            logger.info(f"  Saved: {out_path.name}")
            all_results.append(result)
            processed += 1
        except Exception as exc:
            logger.error(f"  FAILED: {exc}")
            errors += 1

    if all_results:
        write_summary_csv(all_results, EXTRACTED_DIR / "summary.csv")

    print(f"\n{'─'*60}")
    print(f"  Total files      : {len(files)}")
    print(f"  Newly processed  : {processed}")
    print(f"  Cached (skipped) : {skipped}")
    print(f"  Errors           : {errors}")
    print(f"  Results in       : {EXTRACTED_DIR}")


if __name__ == "__main__":
    main()
