"""
membership_agreement_test/count_pages.py

Counts pages in every PDF inside membership_agreement_test/pdfs/,
flags documents that likely need manual review (< 3 pages or unreadable),
and lists Foxcourt coworkers who have no agreement at all.

Usage:
    python membership_agreement_test/count_pages.py
"""
import json
from pathlib import Path

from pypdf import PdfReader
from pypdf.errors import PdfReadError

BASE_DIR   = Path(__file__).resolve().parent
PDF_DIR    = BASE_DIR / "pdfs"
FOXCOURT_FILE = BASE_DIR / "foxcourt_coworkers.json"

FLAG_THRESHOLD = 3  # pages below this are flagged for manual review


def count_pages(pdf_path: Path) -> int | str:
    try:
        return len(PdfReader(pdf_path).pages)
    except PdfReadError as exc:
        return f"ERROR: {exc}"
    except Exception as exc:
        return f"ERROR: {exc}"


def main():
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {PDF_DIR}")
    else:
        results = [(p, count_pages(p)) for p in pdfs]

        # Sort: errors last, then by page count ascending (flagged ones bubble up)
        results.sort(key=lambda r: (isinstance(r[1], str), r[1] if isinstance(r[1], int) else 0))

        col = max(len(p.name) for p, _ in results)
        print(f"\n  {'File':<{col}}  Pages  Status")
        print(f"  {'─' * col}  {'─' * 5}  {'─' * 20}")
        for pdf_path, pages in results:
            if isinstance(pages, str):
                status = "[UNREADABLE - review manually]"
            elif pages < FLAG_THRESHOLD:
                status = "[FLAG - review manually]"
            else:
                status = ""
            print(f"  {pdf_path.name:<{col}}  {str(pages):>5}  {status}")

        numeric  = [p for _, p in results if isinstance(p, int)]
        flagged  = [(f, p) for f, p in results if isinstance(p, int) and p < FLAG_THRESHOLD]
        unreadable = [(f, p) for f, p in results if isinstance(p, str)]

        print(f"\n  Total PDFs   : {len(pdfs)}")
        if numeric:
            print(f"  Total pages  : {sum(numeric)}")
            print(f"  Min pages    : {min(numeric)}")
            print(f"  Max pages    : {max(numeric)}")
            print(f"  Avg pages    : {sum(numeric) / len(numeric):.1f}")
        if flagged or unreadable:
            print(f"\n  {'─'*60}")
            print(f"  NEEDS MANUAL REVIEW ({len(flagged) + len(unreadable)} files)")
            print(f"  {'─'*60}")
            for pdf_path, pages in flagged:
                print(f"    [< {FLAG_THRESHOLD} pages]  {pdf_path.name}  ({pages} page(s))")
            for pdf_path, err in unreadable:
                print(f"    [UNREADABLE]  {pdf_path.name}  ({err})")

    # Foxcourt coworkers — no agreement in Nexudus at all
    if FOXCOURT_FILE.exists():
        foxcourt = json.loads(FOXCOURT_FILE.read_text())
        if foxcourt:
            print(f"\n  {'─'*60}")
            print(f"  FOXCOURT — no membership agreement in Nexudus ({len(foxcourt)} coworkers)")
            print(f"  {'─'*60}")
            for cid in foxcourt:
                print(f"    {cid}")
    else:
        print(f"\n  (Run test.py first to generate the Foxcourt list)")


if __name__ == "__main__":
    main()
