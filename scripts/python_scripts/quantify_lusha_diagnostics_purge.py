"""Quantify the one-off Lusha diagnostics purge BEFORE doing it (read-only).

Context (2026-06-11): the persist bug dropped Lusha contacts for agencies whose
buildings already existed in SQL, but those agencies still have has_contact=1
rows in bronze.location_scraper_lusha_diagnostics, so filter_new_agencies skips
them forever. Purging those rows would let the next weekly run re-enrich them
(now that contacts are linked on the update path too) — at a Lusha credit cost.

This script estimates that cost per city:
  - upper bound: distinct has_contact=1 agencies in diagnostics
  - refined: among them, agencies that (per the latest globe run) still have at
    least one listing without any lusha_email — i.e. the ones the post-purge
    filter would actually send back to Lusha (its building-coverage check
    re-skips fully covered agencies).
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

from shared.azure_clients.sql_client import get_db

REVEALS_PER_AGENCY = 5  # LUSHA_MAX_REVEALS_PER_AGENCY — worst-case credits

db = get_db()


def show(title: str, rows: list) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("(no rows)")
        return
    for r in rows:
        print(dict(r) if not isinstance(r, dict) else r)


show(
    "Upper bound — distinct has_contact=1 agencies in diagnostics, per city/source",
    db.fetch_all(
        """
        SELECT source, city,
               COUNT(DISTINCT LOWER(LTRIM(RTRIM(agency_name)))) AS skiplisted_agencies
        FROM bronze.location_scraper_lusha_diagnostics
        WHERE has_contact = 1 AND agency_name IS NOT NULL
        GROUP BY source, city
        ORDER BY source, city
        """
    ),
)

# Refined: skip-listed agencies that still have >=1 uncovered listing in the
# latest globe run of their city. Agency identity mirrors matching_name():
# contact_name for idealista, company_name for otodom/immobilienscout.
refined = db.fetch_all(
    """
    WITH latest_run AS (
        SELECT source, run_city, run_id,
               ROW_NUMBER() OVER (PARTITION BY source, run_city ORDER BY MAX(inserted_at) DESC) AS rn
        FROM silver.location_scraper_globe_v2
        WHERE source IN ('idealista', 'otodom', 'immobilienscout')
        GROUP BY source, run_city, run_id
    ),
    latest_items AS (
        SELECT g.source, g.run_city,
               LOWER(LTRIM(RTRIM(
                   CASE WHEN g.source = 'idealista' THEN g.contact_name ELSE g.company_name END
               ))) AS agency_key,
               CASE WHEN g.lusha_email_1 IS NULL OR g.lusha_email_1 = '' THEN 1 ELSE 0 END AS uncovered
        FROM silver.location_scraper_globe_v2 g
        JOIN latest_run lr
          ON lr.source = g.source AND lr.run_city = g.run_city AND lr.run_id = g.run_id
        WHERE lr.rn = 1
    ),
    agency_cover AS (
        SELECT source, run_city, agency_key,
               MAX(uncovered) AS has_uncovered_listing,
               COUNT(*) AS listings
        FROM latest_items
        WHERE agency_key IS NOT NULL AND agency_key <> ''
        GROUP BY source, run_city, agency_key
    ),
    skiplist AS (
        SELECT DISTINCT LOWER(LTRIM(RTRIM(agency_name))) AS agency_key
        FROM bronze.location_scraper_lusha_diagnostics
        WHERE has_contact = 1 AND agency_name IS NOT NULL
    )
    SELECT a.source, a.run_city,
           COUNT(*) AS agencies_in_latest_run,
           SUM(CASE WHEN s.agency_key IS NOT NULL THEN 1 ELSE 0 END) AS skiplisted,
           SUM(CASE WHEN s.agency_key IS NOT NULL AND a.has_uncovered_listing = 1 THEN 1 ELSE 0 END)
               AS would_reenrich_after_purge
    FROM agency_cover a
    LEFT JOIN skiplist s ON s.agency_key = a.agency_key
    GROUP BY a.source, a.run_city
    ORDER BY would_reenrich_after_purge DESC
    """
)
show("Refined — agencies that would actually go back to Lusha after a purge", refined)

total = sum((r["would_reenrich_after_purge"] or 0) for r in refined)
print(f"\nTOTAL agencies re-enriched after purge: {total}")
print(f"Worst-case Lusha reveals ({REVEALS_PER_AGENCY}/agency): {total * REVEALS_PER_AGENCY}")
