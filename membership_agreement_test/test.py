"""
membership_agreement_test/test.py

Downloads the latest membership agreement PDF for each coworker ID.

Steps:
  1. Fetches coworkerdatafiles from Nexudus API for each coworker ID
  2. Selects the most recent file per coworker (sorted by CreatedOn desc)
  3. Downloads the PDF and saves to membership_agreement_test/pdfs/

Usage:
    python membership_agreement_test/test.py
"""
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

MAX_RETRIES = 6
BASE_DELAY = 1.0  # seconds between successful requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import requests
from shared.nexudus.auth import get_bearer_token

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://spaces.nexudus.com/api"

COWORKER_IDS = [
    1418571930,
    1421613509,
    1420300555,
    1421595984,
    1419993257,
    1421514305,
    1419993302,
    1419993285,
    1419993267,
    1419993330,
    1420517850,
    1419993331,
    1419993325,
    1419993268,
    1419993309,
    1421350691,
    1421418413,
    1421031635,
    1417796101,
    1415686807,
    1419993274,
    1419993255,
    1418126535,
    1418746700,
    1421217069,
    1421156108,
    1421165095,
    1419892995,
    1419331867,
    1421135670,
    1418658663,
    1415754617,
    1421088213,
    1421085235,
    1421069994,
    1421038636,
    1420877277,
    1418225045,
    1420967583,
    1418507711,
    1418664146,
    1418664146,
    1419993276,
    1417158018,
    1416220082,
    1418082630,
    1419408163,
    1418049569,
    1417159415,
    1417546767,
    1418180999,
    1419573216,
    1418596930,
    1420602557,
    1420524279,
    1418919000,
    1420428582,
    1419138904,
    1418070472,
    1416207332,
    1418891544,
    1420133672,
    1419993319,
    1419993313,
    1419993310,
    1419993295,
    1419993298,
    1419993327,
    1419993271,
    1419993266,
    1420235924,
    1419993286,
    1419998182,
    1419993314,
    1419993326,
    1419993280,
    1419993299,
    1419993258,
    1419993253,
    1420031099,
    1419993304,
    1419993300,
    1419993339,
    1419993291,
    1419729236,
    1416620923,
    1419720299,
    1419715299,
    1419667108,
    1417901599,
    1418237279,
    1419482383,
    1415764949,
    1418748315,
    1419113294,
    1417796101,
    1417625361,
    1418904521,
    1418904521,
    1418865000,
    1418752693,
    1418086977,
    1416560940
]


OUTPUT_DIR = Path(__file__).resolve().parent / "pdfs"
FOXCOURT_FILE = Path(__file__).resolve().parent / "foxcourt_coworkers.json"


def _get_with_retry(session: requests.Session, url: str, **kwargs) -> requests.Response:
    """GET with automatic retry on 429, honouring Retry-After."""
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(url, **kwargs)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning(
                f"  429 rate-limited — waiting {retry_after}s "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()  # raise on the last 429
    return resp  # unreachable, satisfies type checker


def fetch_data_files(session: requests.Session, coworker_id: int) -> list[dict]:
    """Fetch all coworkerdatafiles for a given coworker ID (all pages)."""
    records = []
    page = 1
    while True:
        resp = _get_with_retry(
            session,
            f"{BASE_URL}/spaces/coworkerdatafiles",
            params={"page": page, "size": 25, "CoworkerDataFile_Coworker": coworker_id},
            timeout=30,
        )
        data = resp.json()
        page_records = data.get("Records", [])
        records.extend(page_records)
        if not data.get("HasNextPage", False):
            break
        page += 1
    return records


def download_pdf(session: requests.Session, file_id: int, output_path: Path) -> None:
    """Stream-download a file by data file ID."""
    url = f"{BASE_URL}/spaces/coworkerdatafiles/getFileData/{file_id}"
    resp = _get_with_retry(session, url, timeout=60, stream=True)

    # Honour the content-type for the extension if not PDF
    content_type = resp.headers.get("Content-Type", "")
    if "pdf" not in content_type.lower() and output_path.suffix.lower() == ".pdf":
        ext = content_type.split("/")[-1].split(";")[0].strip() or "bin"
        output_path = output_path.with_suffix(f".{ext}")

    with open(output_path, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=8192):
            fh.write(chunk)


def parse_created_on(value: str) -> datetime:
    """Parse a Nexudus CreatedOn value (/Date(ms)/ or ISO 8601)."""
    if not value:
        return datetime.min
    if value.startswith("/Date("):
        ms = int(value[6:value.index(")")])
        return datetime.fromtimestamp(ms / 1000)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min


def safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in " -_" else "_" for c in name).strip()


def main():
    # Deduplicate while preserving order
    coworker_ids = list(dict.fromkeys(COWORKER_IDS))
    logger.info(f"Processing {len(coworker_ids)} unique coworker IDs")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    token = get_bearer_token()
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"

    downloaded = []
    foxcourt = []   # coworkers with no data files — assumed Foxcourt location
    errors = []

    for i, coworker_id in enumerate(coworker_ids, 1):
        logger.info(f"[{i}/{len(coworker_ids)}] coworker {coworker_id}")

        try:
            files = fetch_data_files(session, coworker_id)
        except requests.HTTPError as exc:
            logger.error(f"  HTTP error fetching files: {exc}")
            errors.append(coworker_id)
            continue

        if not files:
            logger.warning(f"  No data files found — added to Foxcourt list")
            foxcourt.append(coworker_id)
            continue

        # Most recent file first
        files.sort(key=lambda f: parse_created_on(f.get("CreatedOn", "")), reverse=True)
        latest = files[0]

        file_id = latest["Id"]
        coworker_name = (
            latest.get("CoworkerFullName")
            or latest.get("Coworker")
            or str(coworker_id)
        )
        created_on = latest.get("CreatedOn", "")

        out_path = OUTPUT_DIR / f"{coworker_id}_{safe_filename(coworker_name)}_{file_id}.pdf"

        if out_path.exists():
            logger.info(f"  Already exists: {out_path.name} — skipping download")
            downloaded.append((coworker_id, file_id, out_path))
            continue

        logger.info(
            f"  Downloading file {file_id}  coworker='{coworker_name}'"
            f"  created={created_on}  ({len(files)} file(s) total)"
        )
        try:
            download_pdf(session, file_id, out_path)
            logger.info(f"  Saved: {out_path.name}")
            downloaded.append((coworker_id, file_id, out_path))
        except requests.HTTPError as exc:
            logger.error(f"  Failed to download file {file_id}: {exc}")
            errors.append(coworker_id)

        time.sleep(BASE_DELAY)

    # Save Foxcourt list to disk so count_pages.py can read it
    FOXCOURT_FILE.write_text(json.dumps(foxcourt, indent=2))

    print(f"\n{'─'*60}")
    print(f"  Total unique coworkers : {len(coworker_ids)}")
    print(f"  Downloaded / cached    : {len(downloaded)}")
    print(f"  Foxcourt (no files)    : {len(foxcourt)}")
    print(f"  Errors                 : {len(errors)}")
    if foxcourt:
        print(f"\n  Foxcourt coworker IDs (no membership agreement in Nexudus):")
        for cid in foxcourt:
            print(f"    {cid}")
        print(f"\n  Saved to: {FOXCOURT_FILE}")
    if errors:
        print(f"\n  Coworkers with API errors : {errors}")
    print(f"\n  PDFs saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
