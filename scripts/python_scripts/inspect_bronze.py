"""
scripts/inspect_bronze.py

Run this AFTER the first bronze sync to understand the JSON structure
and decide which fields to promote to silver columns.

Prints:
  - All keys found across all records (with % coverage and sample values)
  - Flags fields that are always null, always the same value, or sparse

Usage:
    python scripts/inspect_bronze.py --entity locations
    python scripts/inspect_bronze.py --entity products
    python scripts/inspect_bronze.py --entity contracts
    python scripts/inspect_bronze.py --entity resources
    python scripts/inspect_bronze.py --entity extra_services
    python scripts/inspect_bronze.py --entity all
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Any
from zipfile import Path
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

from shared.azure_clients.sql_client import get_sql_client

ENTITY_TABLE_MAP = {
    "locations":      "bronze.nexudus_locations",
    "products":       "bronze.nexudus_products",
    "contracts":      "bronze.nexudus_contracts",
    "resources":      "bronze.nexudus_resources",
    "extra_services": "bronze.nexudus_extra_services",
}

SAMPLE_LIMIT = 500  # max rows to load for inspection


def load_records(sql, table: str) -> list[dict]:
    rows = sql.execute_query(f"""
        SELECT TOP {SAMPLE_LIMIT} raw_json
        FROM {table}
        ORDER BY synced_at DESC
    """)
    return [json.loads(r["raw_json"]) for r in rows]


def analyse(records: list[dict], entity: str):
    if not records:
        print(f"  ⚠️  No records found in {entity}")
        return

    total = len(records)
    print(f"\n{'='*60}")
    print(f"  {entity.upper()} — {total} records sampled")
    print(f"{'='*60}")

    # Collect stats per key
    key_stats: dict[str, dict] = defaultdict(lambda: {
        "count": 0, "null_count": 0, "sample_values": set(), "types": set()
    })

    for record in records:
        _walk(record, "", key_stats)

    # Sort: non-null first, then alphabetical
    sorted_keys = sorted(
        key_stats.items(),
        key=lambda x: (-x[1]["count"], x[0])
    )

    print(f"\n{'Field':<50} {'Coverage':>9}  {'Types':<20}  Samples")
    print("-" * 110)

    for key, stats in sorted_keys:
        coverage = stats["count"] / total * 100
        null_pct = stats["null_count"] / total * 100
        types = ", ".join(sorted(stats["types"]))
        samples = list(stats["sample_values"])[:3]
        sample_str = " | ".join(str(s)[:30] for s in samples)

        # Highlight interesting / suspicious fields
        flag = ""
        if coverage < 10:
            flag = "  ← sparse (<10%)"
        elif null_pct > 90:
            flag = "  ← mostly null"
        elif len(stats["sample_values"]) == 1:
            flag = "  ← always same value"

        print(f"  {key:<48} {coverage:>8.0f}%  {types:<20}  {sample_str}{flag}")

    print(f"\n  Total unique fields: {len(key_stats)}")
    print(f"\n  💡 Fields with 100% coverage and varied values → good silver column candidates")
    print(f"  💡 Sparse fields → consider keeping in JSON or making nullable")


def _walk(obj: Any, prefix: str, stats: dict, max_depth: int = 3):
    """Recursively walk a dict/list to collect field stats."""
    if not isinstance(obj, dict) or not prefix.count(".") < max_depth:
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
    parser.add_argument(
        "--entity",
        choices=list(ENTITY_TABLE_MAP.keys()) + ["all"],
        default="all",
        help="Which entity to inspect",
    )
    args = parser.parse_args()

    sql = get_sql_client()

    entities = list(ENTITY_TABLE_MAP.keys()) if args.entity == "all" else [args.entity]

    for entity in entities:
        table = ENTITY_TABLE_MAP[entity]
        try:
            records = load_records(sql, table)
            analyse(records, entity)
        except Exception as e:
            print(f"\n❌ Failed to inspect {entity}: {e}")

    print("\n\nDone. Use this output to define silver columns in 03_silver.sql\n")


if __name__ == "__main__":
    main()