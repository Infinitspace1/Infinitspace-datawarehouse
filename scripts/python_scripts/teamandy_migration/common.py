"""Shared helpers for the Firestore -> Azure ETL.

Contracts used across modules:
  * NDJSON record: one JSON object per line == Firestore doc, plus:
        "__id"   : the Firestore document id (authoritative PK source)
        "__path" : full collection path (for subcollection provenance)
    Firestore types are serialized losslessly (see FirestoreJSONEncoder).
  * Transformed batch: dict { table_name: [ {col: value, ...}, ... ] }
    Values are native Python (str/int/float/bool/datetime/None) or JSON strings.
  * Reject: a row that failed a coercion is written to rejects/<table>.ndjson
    and NULLed in the loaded row (never aborts the batch).
"""
from __future__ import annotations
import os, json, base64, datetime, decimal, pathlib

# ---------------------------------------------------------------- paths / config
ROOT = pathlib.Path(__file__).resolve().parent
WORK = pathlib.Path(os.environ.get("ETL_WORKDIR", ROOT / "_work"))
NDJSON_DIR = WORK / "ndjson"      # extract output
REJECT_DIR = WORK / "rejects"     # transform rejects
ARCHIVE_DIR = WORK / "archive"    # dropped-cache cold archive (pre-blob)
for d in (NDJSON_DIR, REJECT_DIR, ARCHIVE_DIR):
    d.mkdir(parents=True, exist_ok=True)

def env(name: str, default=None, required=False):
    v = os.environ.get(name, default)
    if required and not v:
        raise SystemExit(f"Missing required env var {name}")
    return v

# Target SQL schema for the migrated CRM (lives alongside the existing `bronze` warehouse).
# Must match the schema name in scripts/sql_scripts/teamandy_*.sql (default 'teamandy').
SQL_SCHEMA = env("AZURE_SQL_SCHEMA", "teamandy")

# Put the warehouse repo root on sys.path and load its .env, so we can reuse the
# repo's shared clients (get_sql_client, get_firestore_client) + AZURE_* env.
import sys as _sys
REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]   # ...->teamandy_migration->python_scripts->scripts->ROOT
if str(REPO_ROOT) not in _sys.path:
    _sys.path.insert(0, str(REPO_ROOT))
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(REPO_ROOT / ".env")
except Exception:
    pass

# ---------------------------------------------------------------- Firestore JSON
class FirestoreJSONEncoder(json.JSONEncoder):
    """Lossless serialization of the python objects firebase-admin returns."""
    def default(self, o):
        # firestore Timestamp comes back as datetime (DatetimeWithNanoseconds subclass)
        if isinstance(o, datetime.datetime):
            return {"__t": "ts", "v": o.astimezone(datetime.timezone.utc).isoformat()}
        if isinstance(o, datetime.date):
            return {"__t": "date", "v": o.isoformat()}
        if isinstance(o, (bytes, bytearray)):
            return {"__t": "bytes", "v": base64.b64encode(bytes(o)).decode()}
        if isinstance(o, decimal.Decimal):
            return float(o)
        # GeoPoint / DocumentReference (duck-typed to avoid importing google types here)
        if hasattr(o, "latitude") and hasattr(o, "longitude"):
            return {"__t": "geo", "lat": o.latitude, "lng": o.longitude}
        if hasattr(o, "path"):
            return {"__t": "ref", "v": o.path}
        return super().default(o)

def dumps(obj) -> str:
    return json.dumps(obj, cls=FirestoreJSONEncoder, ensure_ascii=False)

def write_ndjson(path: pathlib.Path, rows):
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(dumps(r) + "\n")
            n += 1
    return n

def read_ndjson(path: pathlib.Path):
    if not path.exists():
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

# ---------------------------------------------------------------- value coercion
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

def to_dt(value):
    """Normalize Firestore/legacy temporals to a tz-aware UTC datetime, or None.
    Handles: encoded ts/date dicts, epoch ms int, ISO-8601 str, datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, dict):                     # our encoded forms
        t = value.get("__t")
        if t in ("ts", "date"):
            value = value["v"]
        else:
            return None
    if isinstance(value, datetime.datetime):
        return value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)
    if isinstance(value, (int, float)):             # epoch — ms if large, else s
        ms = value if value > 1e11 else value * 1000
        return _EPOCH + datetime.timedelta(milliseconds=ms)
    if isinstance(value, str):
        s = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.datetime.fromisoformat(s)
            return dt if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    return datetime.datetime.strptime(value.strip(), fmt).replace(tzinfo=datetime.timezone.utc)
                except ValueError:
                    continue
    raise ValueError(f"uncoercible datetime: {value!r}")

def to_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return None

def to_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def to_str(value):
    """Widen mixed int|str fields to text; pass dicts/lists through as JSON."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return dumps(value)
    return str(value)

def to_json(value):
    """Serialize a map/array field to a JSON string for an NVARCHAR(MAX) column.
    Re-hydrates our encoded Firestore types into plain JSON values first."""
    if value is None:
        return None
    return json.dumps(_plain(value), ensure_ascii=False)

def _plain(o):
    """Recursively convert encoded Firestore types back to plain JSON-friendly values."""
    if isinstance(o, dict):
        t = o.get("__t")
        if t == "ts" or t == "date":
            return o["v"]
        if t == "geo":
            return {"lat": o["lat"], "lng": o["lng"]}
        if t == "ref":
            return o["v"]
        if t == "bytes":
            return o["v"]
        return {k: _plain(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_plain(v) for v in o]
    return o

# ---------------------------------------------------------------- reject logging
_reject_files: dict = {}

def reject(table: str, row: dict, reason: str):
    f = _reject_files.get(table)
    if f is None:
        f = open(REJECT_DIR / f"{table}.ndjson", "a", encoding="utf-8")
        _reject_files[table] = f
    f.write(dumps({"reason": reason, "row": row}) + "\n")
    f.flush()

# ---------------------------------------------------------------- SQL connection
def sql_connect(autocommit=False):
    """Open a raw pyodbc connection via the warehouse repo's SQLClient, which resolves
    AZURE_SQL_CONNECTION_STRING (or Entra/SQL-auth) and handles serverless auto-resume."""
    from shared.azure_clients.sql_client import get_sql_client
    conn = get_sql_client()._open_connection()
    conn.autocommit = autocommit
    return conn
