"""Phase 3 — LOAD (SQL). Bulk-load _work/sql/<table>.ndjson into Azure SQL in
FK-dependency order via pyodbc fast_executemany. On a batch integrity error it
falls back to row-by-row so one bad row (e.g. a duplicate natural key) is logged
to rejects rather than failing the whole table.

  python -m migration.etl.load_sql            # all tables, in order
  python -m migration.etl.load_sql --truncate # TRUNCATE each table first (rehearsal)
"""
from __future__ import annotations
import argparse, datetime
from collections import OrderedDict
from . import common, manifest

SQL_DIR = common.WORK / "sql"
BATCH = 2000


def decode(v):
    """Reverse the NDJSON encoding for scalar values headed to typed columns."""
    if isinstance(v, dict):
        t = v.get("__t")
        if t == "ts":
            dt = datetime.datetime.fromisoformat(v["v"])
            # DATETIME2 is tz-naive: convert to UTC then drop tzinfo
            return dt.astimezone(datetime.timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
        if t == "date":
            return datetime.date.fromisoformat(v["v"])
        # any other dict reaching a column is a bug upstream; store as JSON text
        return common.dumps(v)
    return v


def group_by_present(rows):
    """Group rows by their set of non-NULL columns (stable order). Omitting a NULL
    column from the INSERT lets the SQL column DEFAULT fire (e.g. BIT NOT NULL
    DEFAULT(0)) instead of violating NOT NULL. Returns OrderedDict[cols_tuple -> rows]."""
    order = []
    for r in rows:
        for k in r:
            if k not in order:
                order.append(k)
    groups = OrderedDict()
    for r in rows:
        keys = tuple(c for c in order if r.get(c) is not None)
        groups.setdefault(keys, []).append(r)
    return groups


def load_table(cur, table, truncate=False):
    path = SQL_DIR / f"{table}.ndjson"
    if not path.exists():
        return 0, 0
    rows = list(common.read_ndjson(path))
    if not rows:
        return 0, 0

    conn = cur.connection
    loaded = rejected = 0
    for cols, grp in group_by_present(rows).items():
        if not cols:                                # all-NULL row: nothing to insert
            for r in grp:
                common.reject(table, r, "all_null_row")
                rejected += 1
            continue
        sql = f"INSERT INTO {common.SQL_SCHEMA}.{table} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})"
        tup = lambda r: tuple(decode(r.get(c)) for c in cols)
        # NOTE: fast_executemany is intentionally OFF. With NVARCHAR(MAX) columns it
        # pre-allocates a ~2GB buffer per cell and the process segfaults (0xC0000005).
        # Plain batched executemany streams MAX columns safely.
        for i in range(0, len(grp), BATCH):
            chunk = grp[i:i + BATCH]
            try:
                cur.executemany(sql, [tup(r) for r in chunk])
                conn.commit()                       # commit the good batch
                loaded += len(chunk)
            except Exception:                       # fall back to row-by-row
                conn.rollback()                     # undo the partial failed batch (else its rows
                                                    # re-insert below -> false PK duplicates)
                for r in chunk:
                    try:
                        cur.execute(sql, tup(r))
                        conn.commit()
                        loaded += 1
                    except Exception as e:
                        conn.rollback()
                        common.reject(table, r, f"insert_failed: {e}")
                        rejected += 1
    return loaded, rejected


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--truncate", action="store_true", help="clear each table before load (rehearsal)")
    ap.add_argument("--only", nargs="*")
    args = ap.parse_args()

    conn = common.sql_connect()
    cur = conn.cursor()
    print("LOAD -> Azure SQL")
    total_l = total_r = 0
    # truncate in reverse order first so FK cascade doesn't fight us
    if args.truncate:
        for t in reversed(manifest.TABLE_LOAD_ORDER):
            try:
                cur.execute(f"DELETE FROM {common.SQL_SCHEMA}.{t}")
            except Exception as e:
                print(f"  (pre-clear {t}: {e})")
        conn.commit()

    for table in manifest.TABLE_LOAD_ORDER:
        if args.only and table not in args.only:
            continue
        l, r = load_table(cur, table, truncate=False)
        conn.commit()
        if l or r:
            print(f"  {table:34} loaded={l:<7} rejected={r}")
        total_l += l
        total_r += r
    cur.close()
    conn.close()
    print(f"\nTotal loaded={total_l}  rejected={total_r}")
    if total_r:
        print(f"Rejects in {common.REJECT_DIR}")


if __name__ == "__main__":
    main()
