"""
scripts/python_scripts/enrich_locations_gmaps.py

Enriches coworking locations with Google Maps data (POIs, transit, neighborhoods).

This is a manual/on-demand script — NOT scheduled.
Run it when:
  - First time setup (enrich all locations)
  - A new location is added to Nexudus
  - You want to refresh data for a specific location

Usage:
    # Enrich all un-enriched locations
    python scripts/python_scripts/enrich_locations_gmaps.py

    # Enrich a specific location by source_id
    python scripts/python_scripts/enrich_locations_gmaps.py --location 1415402817

    # Force re-enrich all (even already done)
    python scripts/python_scripts/enrich_locations_gmaps.py --force

    # Dry run — show what would be enriched without calling Google
    python scripts/python_scripts/enrich_locations_gmaps.py --dry-run

    # Show current enrichment status
    python scripts/python_scripts/enrich_locations_gmaps.py --status

Required:
    GOOGLE_MAPS_API_KEY in .env or environment
"""
import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("enrich_gmaps")


def _section(title: str):
    print(f"\n{'─'*60}\n  {title}\n{'─'*60}")


def show_status(sql):
    """Show which locations are enriched and which aren't."""
    _section("Enrichment Status")

    rows = sql.execute_query("""
        SELECT
            l.source_id,
            l.name,
            l.city,
            l.latitude,
            l.longitude,
            g.status AS enrichment_status,
            g.pois_found,
            g.transit_found,
            g.finished_at AS last_enriched
        FROM silver.nexudus_locations l
        LEFT JOIN (
            SELECT location_source_id, status, pois_found, transit_found, finished_at,
                   ROW_NUMBER() OVER (PARTITION BY location_source_id ORDER BY started_at DESC) AS rn
            FROM meta.gmaps_enrichment_log
        ) g ON g.location_source_id = l.source_id AND g.rn = 1
        ORDER BY l.city, l.name
    """)

    print(f"\n  {'Location':<45} {'City':<15} {'Status':<10} {'POIs':>5} {'Transit':>8} {'Last Enriched'}")
    print(f"  {'─'*110}")

    enriched = pending = 0
    for r in rows:
        status = r["enrichment_status"] or "pending"
        pois = r["pois_found"] or "-"
        transit = r["transit_found"] or "-"
        last = str(r["last_enriched"])[:16] if r["last_enriched"] else "-"

        icon = "✅" if status == "success" else ("❌" if status == "failed" else "⏳")
        print(f"  {icon} {r['name'][:43]:<43} {(r['city'] or '?'):<15} {status:<10} {pois:>5} {transit:>8} {last}")

        if status == "success":
            enriched += 1
        else:
            pending += 1

    print(f"\n  Total: {enriched} enriched, {pending} pending")


def show_dry_run(sql):
    """Show what would be enriched."""
    _section("Dry Run — Locations to Enrich")

    rows = sql.execute_query("""
        SELECT l.source_id, l.name, l.city, l.latitude, l.longitude
        FROM silver.nexudus_locations l
        LEFT JOIN meta.gmaps_enrichment_log g
            ON g.location_source_id = l.source_id AND g.status = 'success'
        WHERE l.latitude IS NOT NULL
          AND l.longitude IS NOT NULL
          AND g.id IS NULL
        ORDER BY l.city, l.name
    """)

    if not rows:
        print("\n  All locations are already enriched! Use --force to re-enrich.")
        return

    print(f"\n  Would enrich {len(rows)} locations:\n")
    for r in rows:
        print(f"    [{r['source_id']}] {r['name']} ({r['city']}) — ({r['latitude']}, {r['longitude']})")

    # Estimate API calls
    poi_calls = len(rows) * len(__import__("shared.gmaps.enrichment", fromlist=["POI_SEARCHES"]).POI_SEARCHES)
    transit_calls = len(rows) * len(__import__("shared.gmaps.enrichment", fromlist=["TRANSIT_SEARCHES"]).TRANSIT_SEARCHES)
    geo_calls = len(rows)  # reverse geocode
    landmark_calls = len(rows)  # landmark search
    total = poi_calls + transit_calls + geo_calls + landmark_calls

    print(f"\n  Estimated Google Maps API calls: ~{total}")
    print(f"    POI searches:     {poi_calls}")
    print(f"    Transit searches: {transit_calls}")
    print(f"    Reverse geocode:  {geo_calls}")
    print(f"    Landmark search:  {landmark_calls}")


def main():
    parser = argparse.ArgumentParser(description="Enrich locations with Google Maps data")
    parser.add_argument("--location", type=int, help="Enrich a specific location by source_id")
    parser.add_argument("--force", action="store_true", help="Re-enrich even if already done")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be enriched")
    parser.add_argument("--status", action="store_true", help="Show enrichment status")
    args = parser.parse_args()

    from shared.azure_clients.sql_client import get_sql_client
    sql = get_sql_client()

    if args.status:
        show_status(sql)
        return

    if args.dry_run:
        show_dry_run(sql)
        return

    # Actual enrichment
    from shared.gmaps.enrichment import LocationEnricher

    try:
        enricher = LocationEnricher()
    except EnvironmentError as e:
        print(f"\n  ❌ {e}")
        print("  Set GOOGLE_MAPS_API_KEY in your .env file")
        sys.exit(1)

    if args.location:
        _section(f"Enriching location {args.location}")
        result = enricher.enrich_location(args.location)
        print(f"\n  ✅ Done: {result['pois']} POIs, {result['transit']} transit stations")
    else:
        _section("Enriching all locations")
        results = enricher.enrich_all(force=args.force)
        print(f"\n  ✅ Done: {results['enriched']} enriched, {results['skipped']} skipped, {results['failed']} failed")

    _section("Verification")
    show_status(sql)


if __name__ == "__main__":
    main()