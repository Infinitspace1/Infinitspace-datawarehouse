"""
scripts/python_scripts/test_products_silver.py

Usage:
    python scripts/python_scripts/test_products_silver.py            # dry run
    python scripts/python_scripts/test_products_silver.py --write    # write to silver
    python scripts/python_scripts/test_products_silver.py --type 5   # dry run one type
"""
import argparse
import json
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

ITEM_TYPE_LABELS = {1: "Private Office", 2: "Dedicated Desk", 3: "Hot Desk", 4: "Other", 5: "Meeting Room"}


def _section(title):
    print(f"\n{'─'*60}\n  {title}\n{'─'*60}")


def test_bronze(sql):
    _section("1. Bronze counts by type")
    rows = sql.execute_query("""
        SELECT item_type, COUNT(*) AS cnt
        FROM bronze.nexudus_products
        GROUP BY item_type ORDER BY item_type
    """)
    total = 0
    for r in rows:
        label = ITEM_TYPE_LABELS.get(r["item_type"], "Unknown")
        print(f"  Type {r['item_type']} ({label:<16}): {r['cnt']:>4}")
        total += r["cnt"]
    print(f"  {'Total':<22}: {total:>4}")
    if not total:
        print("  ❌  No data — run test_local.py --step products first")
        sys.exit(1)


def test_transform(sql, filter_type=None):
    _section(f"2. Transform dry-run{f' (type {filter_type})' if filter_type else ''}")
    from shared.nexudus.transformers.products import transform_product

    where = f"AND item_type = {filter_type}" if filter_type else ""
    rows = sql.execute_query(f"""
        SELECT TOP 15 b.id, b.raw_json
        FROM bronze.nexudus_products b
        INNER JOIN (
            SELECT source_id, MAX(synced_at) AS latest
            FROM bronze.nexudus_products
            GROUP BY source_id
        ) latest ON b.source_id = latest.source_id AND b.synced_at = latest.latest
        WHERE 1=1 {where}
        ORDER BY b.item_type, b.source_id
    """)

    ok = errors = 0
    for row in rows:
        raw = json.loads(row["raw_json"])
        try:
            p = transform_product(raw, row["id"], "dry-run")
            type_label = ITEM_TYPE_LABELS.get(p["item_type"], "?")
            print(f"\n  ✅  [{p['item_type']}:{type_label}] {p['source_id']}  '{p['name']}'")
            print(f"      location='{p['location_name']}'  price={p['price']} {p['currency_code']}")
            print(f"      available={p['is_available']}  capacity={p['capacity']}  size={p['size_sqm']} ({p['custom_size_sqm']})")
            print(f"      contracts='{p['contract_ids_raw']}'")
            if p["resource_id"]:
                amenities = [k.replace("amenity_","") for k,v in p.items() if k.startswith("amenity_") and v == 1]
                print(f"      resource_id={p['resource_id']}  type='{p['resource_type_name']}'")
                print(f"      amenities={amenities}")
            ok += 1
        except Exception as e:
            print(f"\n  ❌  source_id={raw.get('Id')}: {e}")
            errors += 1

    print(f"\n  {ok} ok, {errors} errors")
    return errors == 0


def test_write_silver(sql):
    _section("3. Writing to silver.nexudus_products")
    from shared.azure_clients.silver_writer_products import SilverProductsWriter

    counts = SilverProductsWriter(uuid.uuid4()).run()
    print(f"\n  products: {counts['products']}  errors: {counts['errors']}")

    _section("4. Verification")
    summary = sql.execute_query("""
        SELECT item_type, product_type_label,
               COUNT(*) AS total,
               SUM(CAST(is_available AS INT)) AS available,
               COUNT(contract_ids_raw) AS with_contracts,
               COUNT(resource_id) AS with_resource
        FROM silver.nexudus_products
        GROUP BY item_type, product_type_label
        ORDER BY item_type
    """)
    print(f"\n  {'T':<3} {'Label':<18} {'Total':>6} {'Avail':>6} {'Contracts':>10} {'Resource':>9}")
    print(f"  {'─'*57}")
    for r in summary:
        print(f"  {r['item_type']:<3} {r['product_type_label']:<18} {r['total']:>6} "
              f"{r['available']:>6} {r['with_contracts']:>10} {r['with_resource']:>9}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--type", type=int, choices=[1, 2, 3, 4, 5])
    args = parser.parse_args()

    from shared.azure_clients.sql_client import get_sql_client
    sql = get_sql_client()

    test_bronze(sql)
    ok = test_transform(sql, filter_type=args.type)

    if not ok:
        print("\n❌  Fix errors before writing")
        sys.exit(1)

    if args.write:
        test_write_silver(sql)
    else:
        print("\n  ℹ️  Dry run complete. Add --write to push to silver.")


if __name__ == "__main__":
    main()