"""
scripts/python_scripts/probe_nexudus_helpdesk_fit.py

Part 2 of the help-desk discovery: can it be synced like every other entity?

Checks:
  A. Nexudus-native incremental filter `from_HelpDeskMessage_UpdatedOn`
     (the convention already used for coworker invoices), since the generic
     `UpdatedSince` is ignored on this endpoint.
  B. Per-ticket comment filtering (thread reconstruction).
  C. Warehouse fit: do BusinessId / CoworkerId / HelpDeskDepartmentId
     actually join to the existing silver tables?

Usage:
    .\venv\Scripts\python.exe scripts\python_scripts\probe_nexudus_helpdesk_fit.py
"""
import asyncio
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")

from shared.nexudus.auth import get_bearer_token          # noqa: E402
from shared.nexudus.client import NexudusClient           # noqa: E402
from shared.azure_clients.sql_client import get_sql_client  # noqa: E402


async def main():
    token = get_bearer_token()
    async with NexudusClient(token, max_concurrent=4) as client:

        # ── A. Incremental filter conventions ───────────────────
        print("=== A. INCREMENTAL FILTER CANDIDATES (helpdeskmessages) ===")
        base = await client.get("support/helpdeskmessages", {"page": 1, "size": 1})
        total = base.get("TotalItems")
        print(f"  baseline (no filter) TotalItems = {total}\n")
        for key in ["from_HelpDeskMessage_UpdatedOn",
                    "from_HelpDeskMessage_CreatedOn",
                    "HelpDeskMessage_UpdatedOn",
                    "UpdatedSince"]:
            for val in ["2026-08-01", "2026-06-01"]:
                try:
                    d = await client.get("support/helpdeskmessages",
                                         {"page": 1, "size": 1, key: val})
                    t = d.get("TotalItems")
                    verdict = "WORKS" if t != total else "ignored"
                    print(f"  {key:36s} = {val}  -> TotalItems={t:6}  {verdict}")
                except Exception as e:  # noqa: BLE001
                    print(f"  {key:36s} = {val}  -> FAILED {type(e).__name__}")

        print("\n  same test on helpdeskcomments:")
        cbase = await client.get("support/helpdeskcomments", {"page": 1, "size": 1})
        ctotal = cbase.get("TotalItems")
        print(f"    baseline TotalItems = {ctotal}")
        for key in ["from_HelpDeskComment_UpdatedOn", "from_HelpDeskComment_CreatedOn"]:
            try:
                d = await client.get("support/helpdeskcomments",
                                     {"page": 1, "size": 1, key: "2026-08-01"})
                t = d.get("TotalItems")
                print(f"    {key:34s} -> TotalItems={t:6}  "
                      f"{'WORKS' if t != ctotal else 'ignored'}")
            except Exception as e:  # noqa: BLE001
                print(f"    {key:34s} -> FAILED {type(e).__name__}")

        # ── B. Comment -> ticket filtering ──────────────────────
        print("\n\n=== B. THREAD RECONSTRUCTION (comments per ticket) ===")
        sample = await client.get("support/helpdeskcomments", {"page": 1, "size": 1})
        msg_id = sample["Records"][0]["HelpDeskMessageId"]
        for key in ["HelpDeskComment_HelpDeskMessage", "HelpDeskMessageId"]:
            try:
                d = await client.get("support/helpdeskcomments",
                                     {"page": 1, "size": 50, key: msg_id})
                t = d.get("TotalItems")
                ok = all(r.get("HelpDeskMessageId") == msg_id
                         for r in d.get("Records", []))
                print(f"  {key:34s}={msg_id} -> TotalItems={t:5}  "
                      f"all_match={ok}  {'WORKS' if t != ctotal else 'ignored'}")
            except Exception as e:  # noqa: BLE001
                print(f"  {key:34s} -> FAILED {type(e).__name__}")

        # ── C. Warehouse fit ────────────────────────────────────
        print("\n\n=== C. WAREHOUSE FIT (joins to existing silver) ===")
        msgs = await client.get_all("support/helpdeskmessages")
        comments = await client.get_all("support/helpdeskcomments")
        depts = await client.get_all("support/helpdeskdepartments")
        print(f"  fetched: {len(msgs)} messages, {len(comments)} comments, "
              f"{len(depts)} departments")

        sql = get_sql_client()
        loc_ids = {r["source_id"] for r in sql.execute_query(
            "SELECT source_id FROM silver.nexudus_locations WHERE is_deleted = 0")}
        cow_ids = {r["source_id"] for r in sql.execute_query(
            "SELECT source_id FROM silver.nexudus_coworkers WHERE is_deleted = 0")}

        biz = {m.get("BusinessId") for m in msgs if m.get("BusinessId")}
        matched_biz = biz & loc_ids
        print(f"\n  BusinessId -> silver.nexudus_locations : "
              f"{len(matched_biz)}/{len(biz)} distinct match")
        for b in sorted(biz - matched_biz):
            print(f"      UNMATCHED BusinessId: {b}")

        cow = {m.get("CoworkerId") for m in msgs if m.get("CoworkerId")}
        mrows = sum(1 for m in msgs if m.get("CoworkerId") in cow_ids)
        print(f"  CoworkerId -> silver.nexudus_coworkers : "
              f"{len(cow & cow_ids)}/{len(cow)} distinct match "
              f"({mrows}/{len(msgs)} rows)")

        dept_ids = {d["Id"] for d in depts}
        mdept = {m.get("HelpDeskDepartmentId") for m in msgs
                 if m.get("HelpDeskDepartmentId")}
        print(f"  HelpDeskDepartmentId -> departments endpoint : "
              f"{len(mdept & dept_ids)}/{len(mdept)} distinct match")
        no_dept = sum(1 for m in msgs if not m.get("HelpDeskDepartmentId"))
        print(f"  messages with NO department: {no_dept}")

        cmsg = {c.get("HelpDeskMessageId") for c in comments
                if c.get("HelpDeskMessageId")}
        msg_ids = {m["Id"] for m in msgs}
        print(f"  comment.HelpDeskMessageId -> message.Id : "
              f"{len(cmsg & msg_ids)}/{len(cmsg)} distinct match "
              f"({len(cmsg - msg_ids)} orphan threads)")
        internal = sum(1 for c in comments if c.get("Internal"))
        print(f"  comments flagged Internal (staff-only notes): "
              f"{internal}/{len(comments)}")

        # sizing
        avg_msg = sum(len(str(m.get("MessageText") or "")) for m in msgs) / max(len(msgs), 1)
        print(f"\n  sizing: avg MessageText {avg_msg:.0f} chars; "
              f"est. bronze payload ~{(len(msgs) + len(comments)) * 1.2:.0f} rows/night")


if __name__ == "__main__":
    asyncio.run(main())
