"""
scripts/python_scripts/apply_detail_view_type4.py

gold.vw_landlord_monthly_contract_detail was resolving its product link against
item_type IN (1, 2, 3) while gold.vw_landlord_contract_book_monthly - the view it
is documented to reconcile with - uses (1, 2, 3, 4).

A contract whose ONLY products are item_type 4 (Other: storeroom / parking)
therefore resolved no link at all, and was then dropped by the shared
`pl.contract_source_id IS NOT NULL` gate. That view is what the dashboard's
revenue-bar drill-down modal lists contracts from, while the modal's header
shows the contract book's month total - so the rows did not add up to the number
printed beside them. The shortfall was a fixed per-building offset in EVERY
month, past and future alike:

    Zuidtoren       -2,000     Heidestrasse    -2,093
    Papaverhof      -2,590     Chausseestrasse -1,000
    Fox Court          -20     (Aldgate / Bower / Herengracht / Stack already 0)

After this change six of nine buildings reconcile to the cent. What remains is
recorded below, deliberately unfixed.

The capacity CASE in that CTE already scores item_type 4 as 0 desks, which is
the contract book's treatment too - type 4 carries fee, not workstations. So
adding it to the join changes revenue coverage only, never desk counts.

Dry-run before applying: 0 rows lost, 480 contract-months ADDED (every one with
capacity 0), and only 10 pre-existing rows changed - future-signed
storeroom-only contracts whose `is_unlinked_future` flag correctly flips 1 -> 0
now that they resolve a real product, picking up their type-4 list price. That
flag is not read by the dashboard (the data-quality panel reads
gold.vw_landlord_data_quality_issues).

STILL OPEN - NOT addressed here, because fixing it restates forecast revenue and
needs a decision on which view is canonical:
  * A residual detail-vs-book offset of -50 (Papaverhof), -493 (Heidestrasse)
    and -20 (Fox Court), constant in every month. Both views now carry byte-
    identical status and product gates, so the cause is in month membership,
    not contract selection.
  * The type view disagrees with the book on FUTURE months only (closed months
    are exact): Fox Court +11,420, Aldgate +10,000, Herengracht +2,700,
    Zuidtoren +1,670, Heidestrasse -6,300, Papaverhof -1,566.40. Named
    contributors at Heidestrasse Sep-2026: Cranberry Apps GmbH (detail 6,600 vs
    type 3,300), SereneDB GmbH (4,200 vs 1,200), Typst GmbH (-5,320 vs -4,720).
    The two families of view also still use different future-signed tests -
    the type view CAST(DATEADD(HOUR, 4, start_date) AS DATE) >= GETUTCDATE(),
    the book views CAST(start_date AS DATE) > GETUTCDATE().

Usage:
    python scripts/python_scripts/apply_detail_view_type4.py [--dry-run]
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client

SQL_DIR = ROOT / "scripts" / "sql_scripts"
MARKER = "VIEW gold.vw_landlord_monthly_contract_detail"


def split_batches(script: str) -> list[str]:
    batches, current = [], []
    for line in script.splitlines():
        if line.strip().upper() == "GO":
            batch = "\n".join(current).strip()
            if batch:
                batches.append(batch)
            current = []
        else:
            current.append(line)
    batch = "\n".join(current).strip()
    if batch:
        batches.append(batch)
    return batches


def find_batch(sql_file: Path, marker: str) -> str:
    text = sql_file.read_text(encoding="utf-8-sig")
    matches = [b for b in split_batches(text) if marker in b]
    if len(matches) != 1:
        raise SystemExit(
            f"Expected exactly 1 batch containing {marker!r} in {sql_file.name}, "
            f"found {len(matches)}"
        )
    return matches[0]


RECONCILE = """
SELECT c.period, c.location_name,
       c.sold_monthly_revenue AS book,
       (SELECT SUM(d.sold_monthly_fee)
          FROM gold.vw_landlord_monthly_contract_detail d
         WHERE d.location_source_id = c.location_source_id
           AND d.period = c.period) AS detail
FROM gold.vw_landlord_contract_book_monthly c
WHERE c.period IN ('2026-08', '2026-09', '2026-10')
  AND c.sold_monthly_revenue <> 0
ORDER BY c.location_name, c.period
"""


def report(sql, title):
    print(f"\n=== {title} ===")
    worst = 0.0
    for r in sql.execute_query(RECONCILE):
        book = float(r["book"] or 0)
        detail = float(r["detail"] or 0)
        gap = detail - book
        worst = max(worst, abs(gap))
        flag = "" if abs(gap) < 1 else "  <-- does not reconcile"
        print(f"  {str(r['location_name'])[:40]:<40} {r['period']} "
              f"book={book:>12,.2f} detail={detail:>12,.2f} gap={gap:>+10,.2f}{flag}")
    print(f"  worst absolute gap: {worst:,.2f}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="print the batch that would run, change nothing")
    args = parser.parse_args()

    batch = find_batch(SQL_DIR / "landlord_dashboard_schema.sql", MARKER)

    if args.dry_run:
        print(f"-- from landlord_dashboard_schema.sql ({MARKER}): {len(batch)} chars")
        print("-- one view, no materialized table to refresh")
        return

    sql = get_sql_client()
    report(sql, "BEFORE")
    print(f"\napplying {MARKER} ...")
    sql.execute_non_query(batch)
    report(sql, "AFTER")


if __name__ == "__main__":
    main()
