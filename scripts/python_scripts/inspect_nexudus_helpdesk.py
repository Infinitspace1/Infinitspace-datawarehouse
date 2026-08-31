"""
scripts/python_scripts/probe_nexudus_helpdesk.py

Discovery probe for the Nexudus help-desk ("customer requests") endpoints.

Confirms:
  1. which support/* endpoints exist
  2. the field shape of each (for transformer mapping)
  3. whether the standard `UpdatedSince` incremental watermark works
  4. pagination + per-location (BusinessId) distribution

Usage:
    .\venv\Scripts\python.exe scripts\python_scripts\probe_nexudus_helpdesk.py
"""
import asyncio
import io
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Ticket text contains emoji; force UTF-8 on a cp1252 console.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import aiohttp

from shared.nexudus.auth import get_bearer_token
from shared.nexudus.client import NexudusClient

CANDIDATES = [
    "support/helpdeskmessages",
    "support/helpdeskdepartments",
    # thread / conversation sub-entities
    "support/helpdeskmessagecomments",
    "support/helpdeskmessagereplies",
    "support/helpdeskmessagemessages",
    "support/helpdeskmessagenotes",
    "support/helpdeskmessagehistories",
    "support/helpdeskmessageattachments",
    "support/helpdeskmessagefiles",
    "support/helpdeskcomments",
    "support/comments",
    "support/messages",
    "support/replies",
    "support/notes",
    # department members / assignment
    "support/helpdeskdepartmentmembers",
    "support/helpdeskdepartmentusers",
]


async def probe(client, path):
    try:
        data = await client.get(path, {"page": 1, "size": 1})
    except aiohttp.ClientResponseError as e:
        return path, f"HTTP {e.status}", None
    except Exception as e:  # noqa: BLE001
        return path, f"ERR {type(e).__name__}", None
    if isinstance(data, dict):
        return path, f"OK  TotalItems={data.get('TotalItems')}", data
    return path, "OK (list)", None


def _t(value, width=100):
    s = json.dumps(value, default=str, ensure_ascii=False)
    return s if len(s) <= width else s[: width - 3] + "..."


async def main():
    token = get_bearer_token()
    async with NexudusClient(token, max_concurrent=4) as client:
        print(f"=== 1. ENDPOINT DISCOVERY ({len(CANDIDATES)} candidates) ===\n")
        results = await asyncio.gather(*(probe(client, p) for p in CANDIDATES))
        hits = [p for p, s, _ in results if s.startswith("OK")]
        for path, status, _ in results:
            mark = " <== EXISTS" if status.startswith("OK") else ""
            print(f"  {path:42s} {status}{mark}")

        # ── 2. Field shape ──────────────────────────────────────
        for path in hits:
            print(f"\n\n=== 2. FIELD SHAPE: GET /api/{path} ===")
            data = await client.get(path, {"page": 1, "size": 5})
            records = data.get("Records", [])
            print(f"  TotalItems={data.get('TotalItems')}  "
                  f"HasNextPage={data.get('HasNextPage')}  "
                  f"fields={len(records[0]) if records else 0}\n")
            fields = {}
            for r in records:
                for k, v in r.items():
                    if k not in fields or (fields[k] in (None, "", []) and v not in (None, "", [])):
                        fields[k] = v
            for k in sorted(fields):
                print(f"    {k:38s} = {_t(fields[k])}")

        # ── 3. Incremental watermark support ────────────────────
        print("\n\n=== 3. INCREMENTAL WATERMARK (UpdatedSince) ===")
        base = await client.get("support/helpdeskmessages", {"page": 1, "size": 1})
        total = base.get("TotalItems")
        print(f"  unfiltered TotalItems                 = {total}")
        for since in ["2026-08-01", "2026-01-01", "2020-01-01"]:
            try:
                d = await client.get(
                    "support/helpdeskmessages",
                    {"page": 1, "size": 1, "UpdatedSince": since},
                )
                t = d.get("TotalItems")
                verdict = "FILTER WORKS" if t != total else "ignored (same as unfiltered)"
                print(f"  UpdatedSince={since:12s} TotalItems = {t:6}  -> {verdict}")
            except Exception as e:  # noqa: BLE001
                print(f"  UpdatedSince={since:12s} FAILED: {e}")

        # ── 4. Volumetry / distribution ─────────────────────────
        print("\n\n=== 4. VOLUMETRY (full pagination of helpdeskmessages) ===")
        rows = await client.get_all("support/helpdeskmessages")
        print(f"  fetched {len(rows)} records (TotalItems said {total})")
        by_biz = Counter(r.get("BusinessId") for r in rows)
        by_dept = Counter(r.get("HelpDeskDepartmentName") for r in rows)
        closed = Counter(bool(r.get("Closed")) for r in rows)
        dates = sorted(r.get("CreatedOn") or "" for r in rows)
        print(f"  date range: {dates[0][:10]} -> {dates[-1][:10]}")
        print(f"  closed=True: {closed[True]}   open: {closed[False]}")
        print(f"  distinct BusinessId (locations): {len(by_biz)}")
        for biz, n in by_biz.most_common():
            print(f"      {biz} : {n}")
        print(f"  top 12 departments (of {len(by_dept)}):")
        for dept, n in by_dept.most_common(12):
            print(f"      {str(dept)[:45]:45s} : {n}")
        with_owner = sum(1 for r in rows if r.get("OwnerId"))
        with_cow = sum(1 for r in rows if r.get("CoworkerId"))
        resp = [r.get("FirstResponseTimeInMinutes") for r in rows
                if isinstance(r.get("FirstResponseTimeInMinutes"), (int, float))]
        print(f"  linkage: CoworkerId set on {with_cow}/{len(rows)}, "
              f"OwnerId (assignee) set on {with_owner}/{len(rows)}")
        if resp:
            print(f"  FirstResponseTimeInMinutes populated on {len(resp)}/{len(rows)}, "
                  f"median ~{sorted(resp)[len(resp)//2]}")

        # ── 5. One full raw record ──────────────────────────────
        print("\n\n=== 5. ONE FULL RAW RECORD ===")
        print(json.dumps(rows[0], indent=2, default=str, ensure_ascii=False)[:2500])


if __name__ == "__main__":
    asyncio.run(main())
