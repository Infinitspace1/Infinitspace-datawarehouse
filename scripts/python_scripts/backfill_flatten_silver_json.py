"""
scripts/python_scripts/backfill_flatten_silver_json.py

One-off backfill after scripts/sql_scripts/silver_flatten_json_migration.sql:
re-transforms EVERY bronze row (bypassing the incremental silver watermark,
which would otherwise skip unchanged bronze rows and leave the new flattened
columns NULL) and MERGEs into the flattened silver tables.

  - silver.hubspot_marketing_emails  (content widgets, device breakdown, counters/ratios)
  - silver.eventbrite_events         (venue details, ticket price details)

Idempotent — safe to re-run.

Usage:
    python scripts/python_scripts/backfill_flatten_silver_json.py
"""
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from shared.azure_clients.sql_client import get_sql_client
from shared.azure_clients.silver_writer_eventbrite import SilverEventbriteEventsWriter
from shared.azure_clients.silver_writer_hubspot import SilverHubspotMarketingEmailsWriter


def _load_all_bronze(table: str) -> list[dict]:
    """Full read — these bronze tables hold exactly one row per source_id
    (latest-payload MERGE), so no dedup needed."""
    return get_sql_client().execute_query(f"SELECT id, raw_json FROM {table}")


class _FullHubspotWriter(SilverHubspotMarketingEmailsWriter):
    def _load_latest_bronze(self) -> list[dict]:
        return _load_all_bronze("bronze.hubspot_marketing_emails")


class _FullEventbriteWriter(SilverEventbriteEventsWriter):
    def _load_latest_bronze(self) -> list[dict]:
        return _load_all_bronze("bronze.eventbrite_events")


def main():
    run_id = uuid.uuid4()

    print("Backfilling silver.hubspot_marketing_emails (full bronze re-read)...")
    print(f"  {_FullHubspotWriter(run_id).run()}")

    print("Backfilling silver.eventbrite_events (full bronze re-read)...")
    print(f"  {_FullEventbriteWriter(run_id).run()}")

    sql = get_sql_client()
    print("\nVerification:")
    rows = sql.execute_query("""
        SELECT COUNT(*) AS total,
               COUNT(body_html) AS with_body_html,
               COUNT(body_plain_text) AS with_body_text,
               COUNT(stat_hard_bounces) AS with_hard_bounces,
               COUNT(hard_bounce_rate) AS with_hard_bounce_rate,
               COUNT(content_primary_widget_html) AS with_primary_widget_html,
               COUNT(opens_computer) AS with_device_breakdown
        FROM silver.hubspot_marketing_emails
    """)
    r = rows[0]
    print(f"  hubspot: total={r['total']}  body_html={r['with_body_html']}  "
          f"body_text={r['with_body_text']}  hard_bounces={r['with_hard_bounces']}  "
          f"hard_bounce_rate={r['with_hard_bounce_rate']}  "
          f"primary_widget_html={r['with_primary_widget_html']}  "
          f"device_breakdown={r['with_device_breakdown']}")

    rows = sql.execute_query("""
        SELECT COUNT(*) AS total,
               COUNT(venue_address_1) AS with_addr1,
               COUNT(venue_resource_uri) AS with_resource_uri,
               COUNT(ticket_currency) AS with_currency,
               COUNT(minimum_ticket_price_minor) AS with_price_minor
        FROM silver.eventbrite_events
    """)
    r = rows[0]
    print(f"  eventbrite: total={r['total']}  venue_address_1={r['with_addr1']}  "
          f"venue_resource_uri={r['with_resource_uri']}  "
          f"ticket_currency={r['with_currency']}  price_minor={r['with_price_minor']}")


if __name__ == "__main__":
    main()
