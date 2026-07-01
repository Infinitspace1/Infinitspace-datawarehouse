"""Phase 5 — VALIDATE. The cutover gate. Exits non-zero if any hard check fails.

  * row-count parity vs exact Firestore aggregation counts
  * soft-FK orphan report (informational; compare to Firestore baseline manually)
  * dual-shape leadListId sanity

  python -m migration.etl.validate
"""
from __future__ import annotations
import sys
from . import common, manifest

S = common.SQL_SCHEMA   # 'teamandy'

# soft references to report orphans for: (child_table, child_col, parent_table, parent_col)
ORPHAN_CHECKS = [
    ("leads", "assigned_to", "users", "uid"),
    ("leads", "campaign_id", "campaigns", "uid"),
    ("leads", "lead_list_id", "lead_lists", "uid"),
    ("lead_lists", "created_by", "users", "uid"),
    ("campaigns", "lead_list_id", "lead_lists", "uid"),
    ("replies", "contact_id", "contacts", "uid"),
    ("scraping_jobs", "lead_list_id", "lead_lists", "uid"),
    ("lead_lead_lists", "lead_list_uid", "lead_lists", "uid"),
]


def scalar(cur, sql, *args):
    cur.execute(sql, *args)
    return cur.fetchone()[0]


def main():
    conn = common.sql_connect()
    cur = conn.cursor()
    failures = 0

    # Prefer freeze-time counts captured by extract (production drifts); fall back to the static snapshot.
    expected_counts = dict(manifest.GROUND_TRUTH_COUNTS)
    cf = common.WORK / "extract_counts.json"
    if cf.exists():
        import json
        live = json.loads(cf.read_text(encoding="utf-8"))
        for k, v in live.items():
            if k in expected_counts:
                expected_counts[k] = v
        print(f"(using freeze-time counts from {cf.name})")

    print(f"== Row-count parity (schema {S}, vs exact Firestore counts) ==")
    for table, expected in expected_counts.items():
        got = scalar(cur, f"SELECT COUNT(*) FROM {S}.{table}")
        ok = got == expected
        failures += 0 if ok else 1
        print(f"  {'OK ' if ok else 'FAIL'} {table:34} expected={expected:<7} got={got}")

    print("\n== Soft-FK orphan report (informational — must match Firestore baseline, not be zero) ==")
    for ct, cc, pt, pc in ORPHAN_CHECKS:
        n = scalar(cur,
            f"SELECT COUNT(*) FROM {S}.{ct} c WHERE c.{cc} IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {S}.{pt} p WHERE p.{pc} = c.{cc})")
        print(f"  {ct}.{cc} -> {pt}.{pc}: {n} orphans")

    print("\n== Dual-shape leadListId sanity ==")
    scalar_only = scalar(cur,
        f"SELECT COUNT(*) FROM {S}.leads l WHERE l.lead_list_id IS NOT NULL "
        f"AND NOT EXISTS (SELECT 1 FROM {S}.lead_lead_lists j WHERE j.lead_uid=l.uid)")
    print(f"  leads with scalar lead_list_id but NO junction row (legacy-only): {scalar_only}")
    both = scalar(cur, f"SELECT COUNT(DISTINCT lead_uid) FROM {S}.lead_lead_lists")
    print(f"  distinct leads with >=1 junction row: {both}")

    print("\n== Child-table totals ==")
    for t in ("lead_contact_persons", "lead_notes", "competence_new_competitors",
              "company_index_lead_list", "sequence_steps", "location_workspaces"):
        print(f"  {t:34} {scalar(cur, f'SELECT COUNT(*) FROM {S}.{t}')}")

    cur.close()
    conn.close()
    print("\n" + ("FAILED" if failures else "PASSED") + f" — {failures} count mismatch(es)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
