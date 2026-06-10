"""
scripts/python_scripts/test_hubspot_sync.py

Local end-to-end check for the HubSpot marketing emails sync.

  --dry-run (default): connect to HubSpot, fetch all marketing emails with
      stats, print counts, a field inventory (incl. stats counters/ratios key
      names) and a transformed sample. No SQL writes — needs only
      HUBSPOT_ACCESS_TOKEN.
  --write: full path — HubSpot -> bronze -> silver -> reconcile. Needs SQL too
      (and the tables from scripts/sql_scripts/hubspot_marketing_emails_schema.sql).

Usage:
    python scripts/python_scripts/test_hubspot_sync.py
    python scripts/python_scripts/test_hubspot_sync.py --write
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def _section(title: str):
    print(f"\n{'─'*60}\n  {title}\n{'─'*60}")


def fetch_emails():
    _section("1. Connect to HubSpot + fetch marketing emails")
    from shared.hubspot.client import fetch_marketing_emails

    emails = fetch_marketing_emails(include_stats=True)
    print(f"\n  Marketing emails: {len(emails)}")

    states: dict = {}
    for e in emails:
        states[e.get("state")] = states.get(e.get("state"), 0) + 1
    for state, count in sorted(states.items(), key=lambda kv: -kv[1]):
        print(f"    {state or '?':<30} {count}")

    # Field inventory — verify the stats counters/ratios key names against
    # the defensive mapping in the transformer.
    with_stats = next((e for e in emails if e.get("stats")), None)
    if with_stats:
        stats = with_stats.get("stats") or {}
        print(f"\n  Top-level keys:       {sorted(with_stats.keys())}")
        print(f"  stats keys:           {sorted(stats.keys())}")
        print(f"  stats.counters keys:  {sorted((stats.get('counters') or {}).keys())}")
        print(f"  stats.ratios keys:    {sorted((stats.get('ratios') or {}).keys())}")
    else:
        print("\n  No email carries a stats object (none sent yet?)")
    return emails


def dry_run_transform(emails):
    _section("2. Transform dry-run (no SQL writes)")
    from shared.hubspot.transformers.marketing_emails import transform_marketing_email

    sample = next((e for e in emails if e.get("stats")), emails[0] if emails else None)
    if not sample:
        print("\n  No marketing emails found.")
        return

    em = transform_marketing_email(sample, 0, "dry-run")
    print(f"\n  Email sample:")
    print(f"    source_id   = {em['source_id']}")
    print(f"    name        = {em['name']}")
    print(f"    subject     = {em['subject']}")
    print(f"    state/type  = {em['state']} / {em['email_type']}")
    print(f"    published   = {em['published_at']}")
    print(f"    from        = {em['from_name']}  reply-to {em['reply_to']}")
    print(f"    body (text) = {(em['body_plain_text'] or '')[:120]!r}")
    print(f"    sent={em['stat_sent']} delivered={em['stat_delivered']} "
          f"opens={em['stat_opens']} clicks={em['stat_clicks']}")
    print(f"    open_rate={em['open_rate']} click_rate={em['click_rate']} "
          f"bounce_rate={em['bounce_rate']} unsub_rate={em['unsubscribed_rate']}")


def run_sync():
    """Run the real production path (RunTracker rows advance the silver
    watermark exactly like the deployed timer)."""
    import asyncio

    from functions.hubspot_sync import run_hubspot_sync

    _section("3. Running HubSpot sync (bronze -> silver -> reconcile)")
    summary = asyncio.run(run_hubspot_sync())
    print(f"\n  summary: {summary}")
    _verify()


def _verify():
    _section("4. Verification (silver.hubspot_marketing_emails)")
    from shared.azure_clients.sql_client import get_sql_client

    sql = get_sql_client()
    rows = sql.execute_query(
        """
        SELECT state,
               COUNT(*) AS total,
               SUM(CASE WHEN is_deleted = 0 THEN 1 ELSE 0 END) AS active,
               SUM(CASE WHEN stat_sent IS NOT NULL THEN 1 ELSE 0 END) AS with_stats,
               AVG(open_rate) AS avg_open_rate
        FROM silver.hubspot_marketing_emails
        GROUP BY state
        ORDER BY total DESC
        """
    )
    print(f"\n  {'State':<28} {'Total':>7} {'Active':>7} {'w/Stats':>8} {'AvgOpen':>9}")
    print(f"  {'-'*62}")
    for r in rows:
        avg = f"{r['avg_open_rate']:.3f}" if r["avg_open_rate"] is not None else "—"
        print(f"  {(r['state'] or '?'):<28} {r['total']:>7} {r['active']:>7} "
              f"{r['with_stats']:>8} {avg:>9}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true",
                        help="Run the sync (bronze + silver + reconcile).")
    args = parser.parse_args()

    if args.write:
        run_sync()
    else:
        emails = fetch_emails()
        dry_run_transform(emails)
        print("\n  Dry run complete. Add --write to persist.")


if __name__ == "__main__":
    main()
