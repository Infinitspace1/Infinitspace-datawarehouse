"""
Apply the month-end-cancellation fix:
  - re-deploys gold.vw_landlord_current_contracts and
    gold.vw_landlord_contract_book_monthly from landlord_dashboard_schema.sql
  - verifies the fix by re-checking Cainiao and the per-location revenue deltas

Idempotent: uses CREATE OR ALTER, so running multiple times is safe.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


SCHEMA_PATH = ROOT / "scripts" / "sql_scripts" / "landlord_dashboard_schema.sql"
VIEWS_TO_DEPLOY = [
    "gold.vw_landlord_current_contracts",
    "gold.vw_landlord_contract_book_monthly",
    "gold.vw_landlord_current_companies",
]


def extract_view_ddl(sql_text: str, fq_view_name: str) -> str:
    """
    Pull the single GO-terminated batch that contains:
        CREATE OR ALTER VIEW <fq_view_name>
    Returns the batch WITHOUT the trailing GO (pyodbc doesn't accept GO).
    """
    pattern = re.compile(
        r"(CREATE\s+OR\s+ALTER\s+VIEW\s+" + re.escape(fq_view_name) + r"\b.*?)(?:^|\n)GO\b",
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )
    m = pattern.search(sql_text)
    if not m:
        raise RuntimeError(f"Could not locate DDL for {fq_view_name}")
    return m.group(1).strip()


def main() -> None:
    sql_text = SCHEMA_PATH.read_text(encoding="utf-8")
    db = get_sql_client()

    # --- 1. apply DDL ---------------------------------------------------------
    for view in VIEWS_TO_DEPLOY:
        ddl = extract_view_ddl(sql_text, view)
        print(f"\nDeploying {view} ({len(ddl):,} chars)...")
        db.execute_non_query(ddl)
        print(f"  OK -- {view} altered")

    # --- 2. verify on Cainiao -------------------------------------------------
    print("\n" + "=" * 80)
    print("VERIFICATION: Cainiao in vw_landlord_current_contracts (should now show 2 rows)")
    print("=" * 80)
    rows = db.execute_query("""
        SELECT
            contract_source_id,
            sold_monthly_fee,
            capacity,
            is_negative_adjustment,
            status,
            start_date,
            cancellation_date
        FROM gold.vw_landlord_current_contracts
        WHERE member_company_name LIKE '%Cainiao%'
        ORDER BY sold_monthly_fee DESC
    """)
    for r in rows:
        print(f"  contract={r['contract_source_id']}  fee={r['sold_monthly_fee']:>10}  "
              f"cap={r['capacity']!s:>4}  neg={r['is_negative_adjustment']}  "
              f"status={r['status']}  cancel={r['cancellation_date']}")
    print(f"  ({len(rows)} rows)")

    print("\n" + "=" * 80)
    print("VERIFICATION: Cainiao in vw_landlord_current_companies (1 row, net fee)")
    print("=" * 80)
    rows = db.execute_query("""
        SELECT
            location_name,
            member_company_name,
            capacity,
            sold_monthly_fee,
            list_monthly_fee
        FROM gold.vw_landlord_current_companies
        WHERE member_company_name LIKE '%Cainiao%'
    """)
    for r in rows:
        print(f"  loc={r['location_name']}  cap={r['capacity']}  "
              f"sold_fee={r['sold_monthly_fee']}  list_fee={r['list_monthly_fee']}")

    print("\n" + "=" * 80)
    print("VERIFICATION: per-location revenue impact this month")
    print("=" * 80)
    rows = db.execute_query("""
        SELECT
            location_name,
            sold_monthly_revenue,
            adjustment_contract_count,
            adjustment_monthly_value,
            active_contract_count
        FROM gold.vw_landlord_pricing_summary
        ORDER BY location_name
    """)
    print(f"  {'location':<42} | {'net_rev':>12} | {'adj#':>4} | {'adj_val':>12} | {'pos#':>5}")
    print("  " + "-" * 84)
    for r in rows:
        print(f"  {r['location_name']:<42} | {r['sold_monthly_revenue']:>12} | "
              f"{r['adjustment_contract_count']:>4} | {r['adjustment_monthly_value']:>12} | "
              f"{r['active_contract_count']:>5}")

    print("\n" + "=" * 80)
    print("VERIFICATION: Taurusavenue monthly book (current month onward)")
    print("=" * 80)
    rows = db.execute_query("""
        SELECT
            period, active_contract_count, adjustment_contract_count,
            occupied_workstations, sold_monthly_revenue, adjustment_monthly_value
        FROM gold.vw_landlord_contract_book_monthly
        WHERE location_source_id = 1414964753
          AND period BETWEEN '2026-04' AND '2026-10'
        ORDER BY period
    """)
    print(f"  {'period':<8} | {'pos#':>4} | {'neg#':>4} | {'desks':>5} | {'net_rev':>10} | {'adj_val':>10}")
    print("  " + "-" * 65)
    for r in rows:
        print(f"  {r['period']:<8} | {r['active_contract_count']:>4} | "
              f"{r['adjustment_contract_count']:>4} | {r['occupied_workstations']:>5} | "
              f"{r['sold_monthly_revenue']:>10} | {r['adjustment_monthly_value']:>10}")

    print("\nDone.")


if __name__ == "__main__":
    main()
