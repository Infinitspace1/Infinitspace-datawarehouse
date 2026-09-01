"""
scripts/python_scripts/apply_office_occupancy_view.py

Creates gold.vw_landlord_office_occupancy_monthly - the physical inventory list
behind the occupancy KPI: one row per (location, month, product) showing how
many workstations each office holds and which company occupies it, or that it
is vacant.

Neither existing view could answer "what exactly is empty?": the contract book
aggregates to a location total, and vw_landlord_monthly_contract_detail is per
CONTRACT, so vacant space - having no contract - appears in neither.

The file only CREATEs a new view; there is nothing destructive to guard against,
so it is applied whole rather than by extracted batch.

Verified before applying: capacity reconciles with
gold.vw_landlord_contract_book_monthly on 34/34 location-months and occupied
desks match to the desk, because occupied-but-unpriced products are emitted with
is_in_capacity = 0 rather than dropped.

Usage:
    python scripts/python_scripts/apply_office_occupancy_view.py [--dry-run]
"""
import argparse, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from shared.azure_clients.sql_client import get_sql_client

SQL_FILE = ROOT / "scripts" / "sql_scripts" / "landlord_office_occupancy_schema.sql"


def batches(script):
    out, cur = [], []
    for line in script.splitlines():
        if line.strip().upper() == "GO":
            if "\n".join(cur).strip():
                out.append("\n".join(cur).strip())
            cur = []
        else:
            cur.append(line)
    if "\n".join(cur).strip():
        out.append("\n".join(cur).strip())
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    bs = batches(SQL_FILE.read_text(encoding="utf-8-sig"))
    if args.dry_run:
        for i, b in enumerate(bs, 1):
            print(f"-- batch {i}: {len(b)} chars, starts {b.splitlines()[0][:60]!r}")
        return
    sql = get_sql_client()
    for i, b in enumerate(bs, 1):
        print(f"applying batch {i}/{len(bs)} ...")
        sql.execute_non_query(b)
    print("\n=== reconciliation vs gold.vw_landlord_contract_book_monthly ===")
    for r in sql.execute_query("""
        SELECT c.period, c.location_name,
               c.total_workstation_capacity AS book_capacity,
               c.occupied_workstations      AS book_occupied,
               o.cap_desks, o.occ_desks, o.offcap_desks
        FROM gold.vw_landlord_contract_book_monthly c
        OUTER APPLY (
            SELECT SUM(CASE WHEN is_in_capacity = 1 THEN workstations ELSE 0 END) AS cap_desks,
                   SUM(CASE WHEN is_occupied    = 1 THEN workstations ELSE 0 END) AS occ_desks,
                   SUM(CASE WHEN is_occupied = 1 AND is_in_capacity = 0 THEN workstations ELSE 0 END) AS offcap_desks
            FROM gold.vw_landlord_office_occupancy_monthly o
            WHERE o.location_source_id = c.location_source_id AND o.period = c.period
        ) o
        WHERE c.period IN ('2026-08','2026-09') AND c.total_workstation_capacity > 0
        ORDER BY c.location_name, c.period"""):
        ok = (r["book_capacity"] == (r["cap_desks"] or 0)) and (r["book_occupied"] == (r["occ_desks"] or 0))
        print("  %-40s %s cap %s/%s  occ %s/%s  off-cap=%s  %s"
              % (str(r["location_name"])[:40], r["period"], r["book_capacity"], r["cap_desks"],
                 r["book_occupied"], r["occ_desks"], r["offcap_desks"], "OK" if ok else "MISMATCH"))


if __name__ == "__main__":
    main()
