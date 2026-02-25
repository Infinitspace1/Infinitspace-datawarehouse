"""
functions/enrich_gmaps.py

Blueprint: HTTP trigger to enrich new/specific locations with Google Maps data.

This is NOT scheduled — it's triggered manually or by a webhook when
new locations are detected.

Endpoints:
    POST /api/enrich-gmaps                    → enrich all un-enriched locations
    POST /api/enrich-gmaps?location_id=123    → enrich one specific location
    POST /api/enrich-gmaps?force=true         → re-enrich all locations
    GET  /api/enrich-gmaps?status=true        → show enrichment status

Auth: ADMIN level (requires function key or master key)
"""
import json
import logging

import azure.functions as func

logger = logging.getLogger(__name__)

bp = func.Blueprint()


@bp.route(route="enrich-gmaps", auth_level=func.AuthLevel.ADMIN, methods=["GET", "POST"])
async def enrich_gmaps(req: func.HttpRequest) -> func.HttpResponse:
    """On-demand Google Maps enrichment for coworking locations."""

    # GET ?status=true → show enrichment status
    if req.method == "GET" or req.params.get("status"):
        return _get_status()

    # POST → run enrichment
    location_id = req.params.get("location_id")
    force = req.params.get("force", "").lower() == "true"

    try:
        from shared.gmaps.enrichment import LocationEnricher
        enricher = LocationEnricher()

        if location_id:
            result = enricher.enrich_location(int(location_id))
            return func.HttpResponse(
                json.dumps({"status": "ok", "location_id": location_id, **result}),
                mimetype="application/json",
            )
        else:
            results = enricher.enrich_all(force=force)
            return func.HttpResponse(
                json.dumps({"status": "ok", **results}),
                mimetype="application/json",
            )

    except Exception as e:
        logger.error(f"Enrichment failed: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            status_code=500,
            mimetype="application/json",
        )


def _get_status() -> func.HttpResponse:
    """Return enrichment status for all locations."""
    from shared.azure_clients.sql_client import get_sql_client
    sql = get_sql_client()

    rows = sql.execute_query("""
        SELECT
            l.source_id,
            l.name,
            l.city,
            CASE WHEN g.status = 'success' THEN 'enriched' ELSE 'pending' END AS enrichment_status,
            g.pois_found,
            g.transit_found,
            g.finished_at
        FROM silver.nexudus_locations l
        LEFT JOIN (
            SELECT location_source_id, status, pois_found, transit_found, finished_at,
                   ROW_NUMBER() OVER (PARTITION BY location_source_id ORDER BY started_at DESC) AS rn
            FROM meta.gmaps_enrichment_log
        ) g ON g.location_source_id = l.source_id AND g.rn = 1
        ORDER BY l.city, l.name
    """)

    # Convert datetime objects to strings
    for r in rows:
        if r.get("finished_at"):
            r["finished_at"] = str(r["finished_at"])

    return func.HttpResponse(
        json.dumps(rows, default=str),
        mimetype="application/json",
    )