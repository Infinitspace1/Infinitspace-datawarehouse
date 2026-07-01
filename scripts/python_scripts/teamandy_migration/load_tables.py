"""Phase 3 — LOAD (Azure Table Storage) for ALL cache collections.
Goal: keep every cache (to fully close Firebase) cheaply. Each doc becomes a
Table entity (PartitionKey/RowKey + promoted scalar fields + the full body as a
'body' JSON property). Bodies larger than the Table 64 KB property limit spill to
Blob ('cache-bodies' container) and the entity stores 'body_blob' = the path.

  AZURE_TABLES_CONN  required (Table Storage connection string, or Azurite)
  AZURE_BLOB_CONN    required only if any body exceeds the spill threshold

  python -m migration.etl.load_tables
"""
from __future__ import annotations
import argparse, json, re
from . import common

SPILL_BYTES = 32000          # body larger than this -> Blob (Table prop cap is 64KB)
BLOB_CONTAINER = "cache-bodies"
_KEY_BAD = re.compile(r"[/\\#?\x00-\x1f\x7f-\x9f]")
_PROP_BAD = re.compile(r"[^A-Za-z0-9_]")

# (ndjson file, Table name, partition-key spec, row-key spec)
# spec: "=const" literal | "__id" | "__parent_id" | a field name (falls back to __id split on '_')
CACHE_PLAN = [
    ("__apollo_neg",            "apolloNeg",             "lead_list_id", "domain"),
    ("__lusha_neg",             "lushaNeg",              "lead_list_id", "domain"),
    ("__lusha_quota",           "lushaQuota",            "=quota",       "=status"),
    ("__apollo",                "apollo",                "lead_list_id", "domain"),
    ("__lusha",                 "lusha",                 "lead_list_id", "domain"),
    ("__maps",                  "maps",                  "=maps",        "__id"),
    ("cache_scraping_settings", "cacheScrapingSettings", "=scraping_settings", "__id"),
    ("cache_unified_v2",        "cacheUnifiedV2",        "leadListId",   "__id"),
    ("company_enrichment_cache","companyEnrichmentCache","lead_list_id", "domain"),
    ("cache_main_competence",   "cacheMainCompetence",   "leadListId",   "__id"),
    ("cache__data",             "cacheData",             "__parent_id",  "__id"),
    ("cacheRoot_placeholder",   "cacheRoot",             "=cache",       "__id"),   # cache.ndjson parent docs
    ("cache_main",              "cacheMain",             "=cache_main",  "__id"),
    ("test_leads_20250717_121935", "testLeads",          "=test",        "__id"),
]
# the 'cache' root collection extracts to cache.ndjson (parent docs); map it explicitly:
_FILE_OVERRIDE = {"cacheRoot_placeholder": "cache"}


def sanitize_key(v):
    s = _KEY_BAD.sub("_", str(v if v is not None else "_"))
    return (s or "_")[:1024]


def sanitize_prop(name):
    n = _PROP_BAD.sub("_", name)
    return ("_" + n) if n[:1].isdigit() else n


def key_of(doc, spec):
    if spec.startswith("="):
        return spec[1:]
    if spec in ("__id", "__parent_id"):
        return doc.get(spec) or "_"
    v = doc.get(spec)
    if v is None and "_" in (doc.get("__id") or ""):     # fallback: split '{a}_{b}' doc id
        a, b = doc["__id"].split("_", 1)
        return a if spec.endswith(("list", "list_id", "List", "ListId")) else b
    return v if v is not None else "_"


def entity(doc, pk_spec, rk_spec):
    ent = {"PartitionKey": sanitize_key(key_of(doc, pk_spec)),
           "RowKey": sanitize_key(key_of(doc, rk_spec))}
    # promote small top-level scalars as queryable properties
    for k, v in doc.items():
        if k.startswith("__") or k in ("PartitionKey", "RowKey"):
            continue
        if isinstance(v, bool):
            ent[sanitize_prop(k)] = v
        elif isinstance(v, int):
            # Table Storage default int is Edm.Int32; epoch-ms ints overflow it -> store as string
            ent[sanitize_prop(k)] = v if -2147483648 <= v <= 2147483647 else str(v)
        elif isinstance(v, float):
            ent[sanitize_prop(k)] = v
        elif isinstance(v, str) and len(v) < 256:
            ent[sanitize_prop(k)] = v
    body = json.dumps(common._plain({k: v for k, v in doc.items() if not k.startswith("__")}),
                      ensure_ascii=False)
    return ent, body


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", help="only these Table names")
    args = ap.parse_args()
    # Storage: prefer AZURE_STORAGE_CONNECTION_STRING (account key — avoids needing the
    # Table data-plane RBAC role); else DefaultAzureCredential (az login / Managed Identity).
    from azure.data.tables import TableServiceClient
    conn = common.env("AZURE_STORAGE_CONNECTION_STRING")
    account = common.env("AZURE_STORAGE_ACCOUNT_NAME", required=not conn)
    cred = None
    if conn:
        svc = TableServiceClient.from_connection_string(conn)
    else:
        from azure.identity import DefaultAzureCredential
        cred = DefaultAzureCredential()
        svc = TableServiceClient(endpoint=f"https://{account}.table.core.windows.net", credential=cred)
    blob_container = None    # lazy

    from collections import defaultdict
    print(f"LOAD CACHES -> Azure Table Storage on {account} (+ Blob spillover)")
    grand = spilled = errs = 0
    from . import manifest as _m
    for fname, table, pk_spec, rk_spec in CACHE_PLAN:
        if table in _m.EXCLUDE_CACHE_TABLES:     # user-confirmed not-migrated
            continue
        if args.only and table not in args.only:
            continue
        src = common.NDJSON_DIR / f"{_FILE_OVERRIDE.get(fname, fname)}.ndjson"
        if not src.exists():
            print(f"  (skip {table}: no {src.name})")
            continue
        try:
            svc.create_table(table)
        except Exception:
            pass
        tc = svc.get_table_client(table)
        # build entities, spill big bodies to Blob, group by PartitionKey for batching
        groups = defaultdict(list)
        n = 0
        for doc in common.read_ndjson(src):
            ent, body = entity(doc, pk_spec, rk_spec)
            if len(body.encode("utf-8")) > SPILL_BYTES:
                if blob_container is None:
                    from azure.storage.blob import BlobServiceClient
                    if conn:
                        bsvc = BlobServiceClient.from_connection_string(conn)
                    else:
                        bsvc = BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net", credential=cred)
                    try:
                        bsvc.create_container(BLOB_CONTAINER)
                    except Exception:
                        pass
                    blob_container = bsvc.get_container_client(BLOB_CONTAINER)
                path = f"{table}/{ent['PartitionKey']}/{ent['RowKey']}.json"
                blob_container.upload_blob(path, body, overwrite=True)
                ent["body_blob"] = path
                spilled += 1
            else:
                ent["body"] = body
            groups[ent["PartitionKey"]].append(ent)
            n += 1
        # submit per partition in batches of 50 (Table txn cap 100/4MB; 50 stays safe with inline bodies)
        for pk, ents in groups.items():
            for i in range(0, len(ents), 50):
                chunk = ents[i:i + 50]
                try:
                    tc.submit_transaction([("upsert", e) for e in chunk])
                except Exception:                       # fall back to single upserts
                    for e in chunk:
                        try:
                            tc.upsert_entity(e)
                        except Exception as ex:
                            common.reject(table, dict(e), f"table_upsert_failed: {ex}")
                            errs += 1
        grand += n
        print(f"  {table:24} {n} entities ({len(groups)} partitions)")
    print(f"\nTotal cache entities: {grand}  (spilled to Blob: {spilled}, errors: {errs})")


if __name__ == "__main__":
    main()
