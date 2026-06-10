"""
scripts/python_scripts/apply_schema_script.py

Apply a SQL schema script (with GO batch separators) to the warehouse DB.

Usage:
    python scripts/python_scripts/apply_schema_script.py scripts/sql_scripts/nexudus_events_schema.sql
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("script", help="Path to the .sql script")
    args = parser.parse_args()

    path = Path(args.script)
    if not path.is_absolute():
        path = ROOT / path
    script = path.read_text(encoding="utf-8")
    batches = split_batches(script)
    print(f"Applying {path.name}: {len(batches)} batch(es)")

    sql = get_sql_client()
    with sql.get_connection() as conn:
        cursor = conn.cursor()
        for i, batch in enumerate(batches, 1):
            first_line = next(
                (l.strip() for l in batch.splitlines() if l.strip() and not l.strip().startswith("--")),
                "",
            )
            print(f"  [{i}/{len(batches)}] {first_line[:80]}")
            cursor.execute(batch)
        conn.commit()
    print("Done.")


if __name__ == "__main__":
    main()
