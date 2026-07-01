"""Phase 1 — EXTRACT. Stream Firestore -> NDJSON (one file per manifest collection).
Captures __id (doc id), __path, and __parent_id (for subcollection records).
READS ONLY. Run during the write-freeze window.

  python -m migration.etl.extract --key path/to/serviceAccountKey.json
"""
from __future__ import annotations
import argparse, sys

from . import common, manifest  # common puts the repo root on sys.path
from shared.firebase.client import get_firestore_client

# Root collections we extract (everything not '__'-prefixed and not drop/recompute/keyvault).
ROOT_TARGETS = {"sql", "table", "archive"}

# Subcollection walks: (output_name, parent_collection, subcollection_name, tag_parent)
SUBCOLLECTION_WALKS = [
    ("__competitors", "competence_new", "competitors", True),
    ("__apollo_neg",  "cache_unified_workflow", "apollo_neg", False),
    ("__lusha_neg",   "cache_unified_workflow", "lusha_neg",  False),
    ("__lusha_quota", "cache_unified_workflow", "lusha_quota", False),
    ("__apollo",      "cache_unified_workflow", "apollo",     False),
    ("__lusha",       "cache_unified_workflow", "lusha",      False),
    ("__maps",        "cache_unified_workflow", "maps",       False),
    ("cache__data",   "cache", "data", True),
]

PHANTOMS = ["graph_subscriptions", "pre-leads"]


def _doc_record(snap, parent_id=None):
    d = snap.to_dict() or {}
    d["__id"] = snap.id
    d["__path"] = snap.reference.path
    if parent_id is not None:
        d["__parent_id"] = parent_id
    return d


def extract_root(db, name):
    out = common.NDJSON_DIR / f"{name}.ndjson"
    n = common.write_ndjson(out, (_doc_record(s) for s in db.collection(name).stream()))
    print(f"  root {name:32} -> {n} docs")
    return n


def extract_subcollection(db, out_name, parent_coll, sub_name, tag_parent):
    out = common.NDJSON_DIR / f"{out_name}.ndjson"
    total = 0
    with open(out, "w", encoding="utf-8") as f:
        for parent in db.collection(parent_coll).stream():
            pid = parent.id if tag_parent else None
            for snap in parent.reference.collection(sub_name).stream():
                f.write(common.dumps(_doc_record(snap, pid)) + "\n")
                total += 1
    print(f"  sub  {parent_coll}/*/{sub_name:20} -> {total} docs ({out_name})")
    return total


def probe_phantoms(db):
    print("\n  Phantom re-probe (must be 0 or schema/staging needs attention):")
    for name in PHANTOMS:
        try:
            n = int(db.collection(name).count().get()[0][0].value)
        except Exception as e:
            n = f"err:{e}"
        flag = "" if n == 0 else "  <-- NON-EMPTY: extract + load before cutover!"
        print(f"    {name:24} = {n}{flag}")
        if isinstance(n, int) and n > 0:
            extract_root(db, name.replace("-", "_"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", help="extract only these collection names")
    args = ap.parse_args()

    db = get_firestore_client()   # warehouse repo's TeamAndy Firestore client (.env creds)

    print("EXTRACT -> NDJSON")
    grand = 0
    counts = {}
    for c in manifest.COLLECTIONS:
        if c.target not in ROOT_TARGETS or c.name.startswith("__"):
            continue
        if c.name in manifest.EXCLUDE_COLLECTIONS:   # user-confirmed not-migrated
            continue
        if args.only and c.name not in args.only:
            continue
        counts[c.name] = extract_root(db, c.name)
        grand += counts[c.name]

    if not args.only:
        for out_name, pc, sub, tag in SUBCOLLECTION_WALKS:
            counts[out_name] = extract_subcollection(db, out_name, pc, sub, tag)
            grand += counts[out_name]
        probe_phantoms(db)

    # alias the competitors subcollection count to its SQL table name so the validate gate
    # (which matches by table name) uses the live freeze-time count, not the stale static one
    if "__competitors" in counts:
        counts["competence_new_competitors"] = counts["__competitors"]
    # freeze-time counts -> validate.py prefers these over the static GROUND_TRUTH snapshot
    import json
    (common.WORK / "extract_counts.json").write_text(json.dumps(counts, indent=1), encoding="utf-8")
    print(f"\nTotal docs extracted: {grand}")
    print(f"NDJSON written to: {common.NDJSON_DIR}")
    print(f"Freeze-time counts -> {common.WORK / 'extract_counts.json'}")


if __name__ == "__main__":
    sys.exit(main())
