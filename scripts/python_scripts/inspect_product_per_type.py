"""
scripts/python_scripts/inspect_products_by_type.py

Reads bronze.nexudus_products and analyses each ItemType separately.
Run this to understand which columns are relevant per product type
before defining silver tables.

ItemType mapping:
  1 = Private Office
  2 = Dedicated Desk
  3 = Hot Desk
  4 = Other (day passes, parking, wellness, etc.)
  5 = Meeting Room

Usage:
    python scripts/python_scripts/inspect_products_by_type.py
    python scripts/python_scripts/inspect_products_by_type.py --type 1
    python scripts/python_scripts/inspect_products_by_type.py --type 5
    python scripts/python_scripts/inspect_products_by_type.py --diff   # show columns that differ across types
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

ITEM_TYPE_LABELS = {
    1: "Private Office",
    2: "Dedicated Desk",
    3: "Hot Desk",
    4: "Other",
    5: "Meeting Room",
}

# Fields that are Nexudus internals — never useful in silver
ALWAYS_DROP = {
    "ArchilogicUniqueId", "FloorPlanArchilogicUniqueId", "FloorPlanBackgroundScale",
    "FloorPlanCapacity", "FloorPlanLayoutUniqueId", "FloorPlanLayoutAssetUniqueId",
    "IsNew", "IsSensorOccupied", "PositionX", "PositionY", "PositionZ", "PositionZ",
    "SensorId", "SensorLastReceivedValue", "SensorLastValue",
    "SensorLastValueTriggeredAction", "SensorName", "SensorSensorType", "SensorUnit",
    "TunnelPrivateGroupId", "SystemId", "LocalizationDetails",
    "ToStringText",     # = Name
    "FloorPlanBackgroundScale",
    "CancellationDate", # always null in products
    "ErrorCode",
    "Bookings", "Contracts", "Proposals",  # always null
    "UpdatedBy",        # internal
    "UniqueId",         # Nexudus internal UUID, Id is the stable key
}


def load_records_by_type(sql) -> dict[int, list[dict]]:
    """Load all bronze products and group by ItemType."""
    rows = sql.execute_query("""
        SELECT b.raw_json, b.item_type
        FROM bronze.nexudus_products b
        INNER JOIN (
            SELECT source_id, MAX(synced_at) AS latest
            FROM bronze.nexudus_products
            GROUP BY source_id
        ) latest ON b.source_id = latest.source_id
                AND b.synced_at = latest.latest
    """)

    by_type: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        raw = json.loads(row["raw_json"])
        item_type = raw.get("ItemType", 0)
        by_type[item_type].append(raw)

    return dict(by_type)


def analyse_type(records: list[dict], type_id: int, type_label: str):
    total = len(records)
    print(f"\n{'='*65}")
    print(f"  ItemType {type_id}: {type_label.upper()}  ({total} records)")
    print(f"{'='*65}")

    if not records:
        print("  No records.")
        return {}

    key_stats: dict[str, dict] = defaultdict(lambda: {
        "count": 0, "null_count": 0, "sample_values": set(), "types": set()
    })

    for record in records:
        _walk(record, "", key_stats)

    # Filter out always-drop fields
    key_stats = {k: v for k, v in key_stats.items() if k.split(".")[0] not in ALWAYS_DROP}

    # Sort: by coverage desc, then name
    sorted_keys = sorted(
        key_stats.items(),
        key=lambda x: (-x[1]["count"], x[0])
    )

    print(f"\n  {'Field':<50} {'Coverage':>9}  {'Types':<20}  Samples")
    print("  " + "-" * 105)

    coverage_map = {}
    for key, stats in sorted_keys:
        coverage = stats["count"] / total * 100
        null_pct  = stats["null_count"] / total * 100
        types     = ", ".join(sorted(stats["types"]))
        samples   = list(stats["sample_values"])[:3]
        sample_str = " | ".join(str(s)[:28] for s in samples)

        flag = ""
        if coverage < 5:
            flag = "  ← very sparse"
        elif null_pct > 90:
            flag = "  ← mostly null"
        elif len(stats["sample_values"]) == 1:
            flag = "  ← always same value"
        elif coverage == 100 and null_pct == 0:
            flag = "  ✓"  # highlight reliable fields

        print(f"  {key:<50} {coverage:>8.0f}%  {types:<20}  {sample_str}{flag}")
        coverage_map[key] = coverage

    print(f"\n  Total fields (excl. always-drop): {len(key_stats)}")
    return coverage_map


def show_diff(by_type: dict[int, list[dict]]):
    """Show which fields are type-specific vs shared across all types."""
    print(f"\n{'='*65}")
    print(f"  CROSS-TYPE FIELD COMPARISON")
    print(f"{'='*65}")

    all_coverage: dict[int, dict[str, float]] = {}
    for type_id, records in by_type.items():
        total = len(records)
        if not total:
            continue
        key_stats: dict[str, dict] = defaultdict(lambda: {"count": 0})
        for record in records:
            _walk(record, "", key_stats)
        all_coverage[type_id] = {
            k: v["count"] / total * 100
            for k, v in key_stats.items()
            if k.split(".")[0] not in ALWAYS_DROP
        }

    all_fields = set()
    for cov in all_coverage.values():
        all_fields.update(cov.keys())

    type_ids = sorted(all_coverage.keys())
    labels   = [f"T{t}({ITEM_TYPE_LABELS.get(t,'?')[:6]})" for t in type_ids]

    print(f"\n  {'Field':<50} " + "  ".join(f"{l:>12}" for l in labels))
    print("  " + "-" * (50 + 14 * len(type_ids)))

    # Only show fields that differ meaningfully between types
    interesting = []
    for field in sorted(all_fields):
        coverages = [all_coverage.get(t, {}).get(field, 0) for t in type_ids]
        if max(coverages) - min(coverages) > 20:  # significant difference
            interesting.append((field, coverages))

    for field, coverages in interesting:
        vals = "  ".join(f"{c:>11.0f}%" for c in coverages)
        print(f"  {field:<50} {vals}")

    print(f"\n  Showing {len(interesting)} fields with >20% coverage difference across types")
    print(f"  These are candidates for type-specific extension tables")


def _walk(obj: Any, prefix: str, stats: dict, max_depth: int = 2):
    if not isinstance(obj, dict) or prefix.count(".") >= max_depth:
        return
    for key, value in obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        stats[full_key]["count"] += 1
        stats[full_key]["types"].add(type(value).__name__)
        if value is None:
            stats[full_key]["null_count"] += 1
        else:
            sample = str(value)[:50] if not isinstance(value, (dict, list)) else f"[{type(value).__name__}]"
            stats[full_key]["sample_values"].add(sample)
        if isinstance(value, dict):
            _walk(value, full_key, stats, max_depth)
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            _walk(value[0], f"{full_key}[]", stats, max_depth)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", type=int, choices=[1, 2, 3, 4, 5], help="Analyse one ItemType only")
    parser.add_argument("--diff", action="store_true", help="Show cross-type field comparison")
    args = parser.parse_args()

    from shared.azure_clients.sql_client import get_sql_client
    sql = get_sql_client()

    by_type = load_records_by_type(sql)

    # Print count summary first
    print("\nProduct counts in bronze by ItemType:")
    for type_id in sorted(ITEM_TYPE_LABELS.keys()):
        count = len(by_type.get(type_id, []))
        print(f"  Type {type_id} ({ITEM_TYPE_LABELS[type_id]:<16}): {count:>4} records")
    unknown = [k for k in by_type if k not in ITEM_TYPE_LABELS]
    for k in unknown:
        print(f"  Type {k} (UNKNOWN): {len(by_type[k])} records  ← investigate!")

    if args.diff:
        show_diff(by_type)
        return

    types_to_show = [args.type] if args.type else sorted(ITEM_TYPE_LABELS.keys())

    for type_id in types_to_show:
        records = by_type.get(type_id, [])
        label   = ITEM_TYPE_LABELS.get(type_id, "Unknown")
        analyse_type(records, type_id, label)

    print("\n\nDone. Next steps:")
    print("  1. Run with --diff to see which fields are type-specific")
    print("  2. Use this output to define silver extension tables per type")


if __name__ == "__main__":
    main()