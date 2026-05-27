"""
Check whether the "Not terminating - check replacement product" issue
is a data/dashboard problem or just a manual annotation.

Companies from the screenshot:
  - ADP Nederland B.V.
  - Allianz Direct Versicherungs-AG
  - A2Z-CM NV          (NOT flagged in the screenshot — included for comparison)
  - RxSight BV

For each: list all silver contracts (incl. future-signed), then show what
gold.vw_landlord_current_contracts and gold.vw_landlord_current_companies
expose. If the future contract exists in silver but doesn't surface to the
"current companies" view (which is what the screenshot's table reads from),
the data is fine and the dashboard is hiding it.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


OUT = ROOT / "termination_investigation.txt"
_buf = []

COMPANIES = [
    ("ADP Nederland", "ADP Nederland"),
    ("Allianz Direct", "Allianz"),
    ("A2Z-CM NV",       "A2Z-CM"),
    ("RxSight",         "RxSight"),
]


def out(line=""):
    _buf.append(line)


def section(title):
    out("")
    out("=" * 100)
    out(title)
    out("=" * 100)


def run(sql, params=None):
    rows = get_sql_client().execute_query(sql, params)
    if not rows:
        out("(no rows)")
        return []
    cols = list(rows[0].keys())
    widths = {c: max(len(c), max((len(str(r[c])) for r in rows), default=0)) for c in cols}
    out(" | ".join(c.ljust(widths[c]) for c in cols))
    out("-+-".join("-" * widths[c] for c in cols))
    for r in rows:
        out(" | ".join(str(r[c]).ljust(widths[c]) for c in cols))
    out(f"({len(rows)} rows)")
    return rows


out("TERMINATION-DATE INVESTIGATION FOR DASHBOARD SCREENSHOT")
out("Today: 2026-05-27. The screenshot's table shows 'end' = cancellation_date of")
out("the CURRENT contract, ignoring whether a follow-up contract has been signed.")


for header, like in COMPANIES:
    section(f"{header} -- ALL silver contracts")
    run(f"""
        SELECT
            c.source_id                         AS contract_id,
            loc.name                            AS location,
            COALESCE(NULLIF(c.coworker_company,''), c.coworker_billing_name, c.coworker_name) AS company,
            c.active, c.cancelled,
            CAST(c.start_date AS DATE)          AS start_date,
            CAST(c.cancellation_date AS DATE)   AS cancellation_date,
            CAST(c.contract_term AS DATE)       AS contract_term,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS monthly_fee,
            c.tariff_name,
            CASE
                WHEN c.is_deleted = 1                                              THEN 'DELETED'
                WHEN c.active = 0 AND c.cancelled = 0 AND c.start_date > GETUTCDATE() THEN 'FUTURE-SIGNED'
                WHEN c.active = 1                                                  THEN 'ACTIVE'
                WHEN c.cancelled = 1                                               THEN 'CANCELLED'
                ELSE 'ABANDONED'
            END AS state
        FROM silver.nexudus_contracts c
        LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
        WHERE (c.coworker_company LIKE '%{like}%' OR c.coworker_name LIKE '%{like}%')
        ORDER BY c.start_date
    """)

    out(f"\n--- {header} -- what gold.vw_landlord_current_contracts shows ---")
    run(f"""
        SELECT
            contract_source_id, status,
            CAST(start_date AS DATE)         AS start_date,
            CAST(cancellation_date AS DATE)  AS cancellation_date,
            capacity, sold_monthly_fee, is_negative_adjustment
        FROM gold.vw_landlord_current_contracts
        WHERE member_company_name LIKE '%{like}%'
        ORDER BY start_date
    """)

    out(f"\n--- {header} -- what gold.vw_landlord_current_companies shows ---")
    out("(this is what the 'Membership Schedule' table on the dashboard reads from)")
    run(f"""
        SELECT
            location_name, member_company_name,
            capacity, sold_monthly_fee, status,
            CAST(start_date AS DATE) AS start_date,
            CAST(cancellation_date AS DATE) AS cancellation_date
        FROM gold.vw_landlord_current_companies
        WHERE member_company_name LIKE '%{like}%'
    """)


section("SUMMARY: which companies have a follow-up contract that the dashboard hides?")
out("Lists every company in `current_companies` whose `cancellation_date` is set,")
out("for which there's ALSO a silver contract starting on/after that date.")
out("If a row appears here, the dashboard is misleadingly showing it as 'terminating'.")
run("""
    WITH cur AS (
        SELECT location_source_id, member_company_name,
               CAST(cancellation_date AS DATE) AS cancel_date,
               capacity, sold_monthly_fee
        FROM gold.vw_landlord_current_companies
        WHERE cancellation_date IS NOT NULL
    ),
    followups AS (
        SELECT
            cur.location_source_id, cur.member_company_name,
            cur.cancel_date,
            cur.sold_monthly_fee     AS current_fee,
            COUNT(*)                 AS followup_contract_count,
            SUM(COALESCE(c.price_with_products, c.price, c.tariff_price, 0))
                                     AS followup_total_fee,
            MIN(CAST(c.start_date AS DATE))      AS earliest_followup_start,
            MAX(CAST(c.contract_term AS DATE))   AS latest_followup_term
        FROM cur
        INNER JOIN silver.nexudus_contracts c
            ON  c.location_source_id = cur.location_source_id
            AND COALESCE(NULLIF(c.coworker_company,''), c.coworker_billing_name, c.coworker_name)
                = cur.member_company_name
            AND c.is_deleted = 0
            AND CAST(c.start_date AS DATE) >= cur.cancel_date
            AND COALESCE(c.price_with_products, c.price, c.tariff_price, 0) > 0
        GROUP BY cur.location_source_id, cur.member_company_name,
                 cur.cancel_date, cur.sold_monthly_fee
    )
    SELECT
        loc.name AS location,
        f.member_company_name,
        f.cancel_date            AS shown_as_terminating,
        f.current_fee,
        f.followup_contract_count,
        f.earliest_followup_start,
        f.followup_total_fee,
        f.latest_followup_term
    FROM followups f
    LEFT JOIN silver.nexudus_locations loc ON loc.source_id = f.location_source_id
    ORDER BY loc.name, f.cancel_date, f.member_company_name
""")


OUT.write_text("\n".join(_buf), encoding="utf-8")
print(f"Wrote {OUT} ({OUT.stat().st_size:,} bytes, {len(_buf)} lines)")
