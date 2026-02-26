"""
shared/gmaps/enrichment.py

Enriches coworking locations with Google Maps data:
  - Nearby POIs (restaurants, cafes, gyms, supermarkets, etc.)
  - Transit stations (metro, train, tram, bus)
  - Neighborhood context (district, landmarks)

Usage (from scripts):
    from shared.gmaps.enrichment import LocationEnricher
    enricher = LocationEnricher()
    enricher.enrich_all()          # enrich all un-enriched locations
    enricher.enrich_location(id)   # enrich one specific location

This is NOT a scheduled job. Run manually or trigger when new locations appear.

Required env var:
    GOOGLE_MAPS_API_KEY — Google Maps Platform API key with Places API enabled
"""
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests

from shared.azure_clients.sql_client import get_sql_client

logger = logging.getLogger(__name__)

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")

# ── POI categories to search ─────────────────────────────────
# Maps our category name → Google Places "type" for Nearby Search
# See: https://developers.google.com/maps/documentation/places/web-service/supported_types

POI_SEARCHES = [
    # Food & Drink
    {"category": "restaurant",     "type": "restaurant",     "radius": 500, "max_results": 15},
    {"category": "cafe",           "type": "cafe",           "radius": 500, "max_results": 10},
    {"category": "bar",            "type": "bar",            "radius": 500, "max_results": 8},

    # Daily needs
    {"category": "supermarket",    "type": "supermarket",    "radius": 800, "max_results": 5},
    {"category": "pharmacy",       "type": "pharmacy",       "radius": 800, "max_results": 3},
    {"category": "atm",            "type": "atm",            "radius": 500, "max_results": 3},

    # Fitness & Wellness
    {"category": "gym",            "type": "gym",            "radius": 1000, "max_results": 5},

    # Services
    {"category": "parking",        "type": "parking",        "radius": 500, "max_results": 5},
    {"category": "hotel",          "type": "lodging",        "radius": 1000, "max_results": 5},

    # Leisure
    {"category": "park",           "type": "park",           "radius": 800, "max_results": 5},
]

# Transit station types to search
TRANSIT_SEARCHES = [
    {"transit_type": "metro",    "type": "subway_station",    "radius": 1000, "max_results": 5},
    {"transit_type": "train",    "type": "train_station",     "radius": 2000, "max_results": 3},
    {"transit_type": "tram",     "type": "light_rail_station","radius": 800,  "max_results": 5},
    {"transit_type": "bus",      "type": "bus_station",       "radius": 500,  "max_results": 5},
]


# ── Helpers ───────────────────────────────────────────────────

def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
    """Calculate straight-line distance between two points in meters."""
    R = 6_371_000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return int(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def _walking_minutes(distance_meters: int) -> int:
    """Estimate walking time: ~80 meters per minute (4.8 km/h)."""
    return max(1, round(distance_meters / 80))


class LocationEnricher:
    """
    Enriches coworking locations with Google Maps data.

    Finds un-enriched locations in silver.nexudus_locations
    and populates:
      - silver.location_nearby_pois
      - silver.location_transit_stations
      - silver.location_neighborhoods
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or GOOGLE_MAPS_API_KEY
        if not self.api_key:
            raise EnvironmentError("Set GOOGLE_MAPS_API_KEY environment variable")
        self.sql = get_sql_client()
        self._request_count = 0

    # ── Public API ────────────────────────────────────────────

    def enrich_all(self, force: bool = False) -> dict:
        """
        Enrich all locations that haven't been enriched yet.

        Args:
            force: If True, re-enrich even if already done.

        Returns:
            {"enriched": count, "skipped": count, "failed": count}
        """
        locations = self._get_locations_to_enrich(force=force)
        logger.info(f"Found {len(locations)} locations to enrich")

        results = {"enriched": 0, "skipped": 0, "failed": 0}

        for loc in locations:
            try:
                self.enrich_location(
                    location_source_id=loc["source_id"],
                    lat=loc["latitude"],
                    lng=loc["longitude"],
                    name=loc["name"],
                )
                results["enriched"] += 1
            except Exception as e:
                logger.error(f"Failed to enrich {loc['name']} ({loc['source_id']}): {e}")
                self._log_enrichment(loc["source_id"], loc["name"], "failed", error=str(e))
                results["failed"] += 1

        logger.info(f"Enrichment complete: {results}")
        return results

    def enrich_location(
        self,
        location_source_id: int,
        lat: float = None,
        lng: float = None,
        name: str = None,
    ) -> dict:
        """
        Enrich a single location with Google Maps data.

        If lat/lng not provided, looks them up from silver.nexudus_locations.
        """
        if lat is None or lng is None:
            loc = self._get_location(location_source_id)
            lat, lng, name = loc["latitude"], loc["longitude"], loc["name"]

        if not lat or not lng:
            raise ValueError(f"Location {location_source_id} has no coordinates")

        logger.info(f"Enriching: {name} ({location_source_id}) at ({lat}, {lng})")
        started_at = datetime.now(timezone.utc)

        # 1. Fetch and store nearby POIs
        poi_count = self._enrich_pois(location_source_id, lat, lng)

        # 2. Fetch and store transit stations
        transit_count = self._enrich_transit(location_source_id, lat, lng)

        # 3. Build neighborhood context
        self._enrich_neighborhood(location_source_id, lat, lng)

        # 4. Log success
        self._log_enrichment(
            location_source_id, name, "success",
            pois=poi_count, transit=transit_count,
            started_at=started_at,
        )

        logger.info(f"Enriched {name}: {poi_count} POIs, {transit_count} transit stations")
        return {"pois": poi_count, "transit": transit_count}

    # ── POI enrichment ────────────────────────────────────────

    def _enrich_pois(self, location_source_id: int, lat: float, lng: float) -> int:
        """Search for nearby POIs across all categories."""
        total = 0

        # Clear existing POIs for this location (full refresh)
        self.sql.execute_non_query(
            "DELETE FROM silver.location_nearby_pois WHERE location_source_id = ?",
            (location_source_id,),
        )

        seen_place_ids = set()  # track duplicates across categories

        for search in POI_SEARCHES:
            places = self._nearby_search(
                lat, lng,
                place_type=search["type"],
                radius=search["radius"],
                max_results=search["max_results"],
            )

            for place in places:
                place_lat = place["geometry"]["location"]["lat"]
                place_lng = place["geometry"]["location"]["lng"]
                distance = _haversine_meters(lat, lng, place_lat, place_lng)

                google_place_id = place["place_id"]

                # Skip if this place was already inserted by a previous category
                if google_place_id in seen_place_ids:
                    continue
                seen_place_ids.add(google_place_id)

                self.sql.execute_non_query("""
                    MERGE silver.location_nearby_pois AS target
                    USING (SELECT ? AS location_source_id, ? AS google_place_id) AS source
                        ON target.location_source_id = source.location_source_id
                       AND target.google_place_id = source.google_place_id
                    WHEN NOT MATCHED THEN INSERT (
                        location_source_id, google_place_id,
                        poi_category, google_primary_type, google_types,
                        name, address,
                        latitude, longitude, distance_meters, walking_minutes,
                        rating, total_ratings, price_level,
                        business_status, opening_hours_text,
                        search_radius_meters, google_data_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    location_source_id, google_place_id,
                    location_source_id,
                    google_place_id,
                    search["category"],
                    place.get("types", [None])[0] if place.get("types") else None,
                    ",".join(place.get("types", [])),
                    place.get("name", "Unknown"),
                    place.get("vicinity") or place.get("formatted_address"),
                    place_lat,
                    place_lng,
                    distance,
                    _walking_minutes(distance),
                    place.get("rating"),
                    place.get("user_ratings_total"),
                    place.get("price_level"),
                    place.get("business_status"),
                    "\n".join(place.get("opening_hours", {}).get("weekday_text", [])) or None,
                    search["radius"],
                    json.dumps(place, default=str, ensure_ascii=False),
                ))
                total += 1

            logger.debug(f"  {search['category']}: {len(places)} found")

        return total

    # ── Transit enrichment ────────────────────────────────────

    def _enrich_transit(self, location_source_id: int, lat: float, lng: float) -> int:
        """Search for nearby transit stations."""
        total = 0

        self.sql.execute_non_query(
            "DELETE FROM silver.location_transit_stations WHERE location_source_id = ?",
            (location_source_id,),
        )

        seen_place_ids = set()

        for search in TRANSIT_SEARCHES:
            places = self._nearby_search(
                lat, lng,
                place_type=search["type"],
                radius=search["radius"],
                max_results=search["max_results"],
            )

            for place in places:
                place_lat = place["geometry"]["location"]["lat"]
                place_lng = place["geometry"]["location"]["lng"]
                distance = _haversine_meters(lat, lng, place_lat, place_lng)

                google_place_id = place["place_id"]
                if google_place_id in seen_place_ids:
                    continue
                seen_place_ids.add(google_place_id)

                self.sql.execute_non_query("""
                    MERGE silver.location_transit_stations AS target
                    USING (SELECT ? AS location_source_id, ? AS google_place_id) AS source
                        ON target.location_source_id = source.location_source_id
                       AND target.google_place_id = source.google_place_id
                    WHEN NOT MATCHED THEN INSERT (
                        location_source_id, google_place_id,
                        transit_type, google_types,
                        name, address,
                        latitude, longitude, distance_meters, walking_minutes,
                        transit_lines,
                        search_radius_meters, google_data_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    location_source_id, google_place_id,
                    location_source_id,
                    google_place_id,
                    search["transit_type"],
                    ",".join(place.get("types", [])),
                    place.get("name", "Unknown"),
                    place.get("vicinity") or place.get("formatted_address"),
                    place_lat,
                    place_lng,
                    distance,
                    _walking_minutes(distance),
                    None,  # transit_lines
                    search["radius"],
                    json.dumps(place, default=str, ensure_ascii=False),
                ))
                total += 1

            logger.debug(f"  {search['transit_type']}: {len(places)} found")

        return total

    # ── Neighborhood enrichment ───────────────────────────────

    def _enrich_neighborhood(self, location_source_id: int, lat: float, lng: float):
        """Reverse geocode to get neighborhood/district info and find nearest landmark."""

        # Reverse geocode for neighborhood
        neighborhood = self._reverse_geocode(lat, lng)

        # Find nearest landmark (tourist_attraction type)
        landmarks = self._nearby_search(lat, lng, place_type="tourist_attraction", radius=2000, max_results=1)
        landmark = landmarks[0] if landmarks else None

        # Find nearest main train station
        stations = self._nearby_search(lat, lng, place_type="train_station", radius=5000, max_results=1)
        station = stations[0] if stations else None

        # Count POIs within 500m (from what we already stored)
        counts = self.sql.execute_query("""
            SELECT
                SUM(CASE WHEN poi_category = 'restaurant' AND distance_meters <= 500 THEN 1 ELSE 0 END) AS restaurants,
                SUM(CASE WHEN poi_category = 'cafe' AND distance_meters <= 500 THEN 1 ELSE 0 END) AS cafes
            FROM silver.location_nearby_pois
            WHERE location_source_id = ?
        """, (location_source_id,))

        transit_count = self.sql.execute_scalar("""
            SELECT COUNT(*) FROM silver.location_transit_stations
            WHERE location_source_id = ? AND distance_meters <= 500
        """, (location_source_id,))

        row = counts[0] if counts else {}

        # Upsert neighborhood
        self.sql.execute_non_query("""
            MERGE silver.location_neighborhoods AS target
            USING (SELECT ? AS location_source_id) AS source
                ON target.location_source_id = source.location_source_id
            WHEN MATCHED THEN UPDATE SET
                neighborhood_name = ?, district_name = ?, city_name = ?, postal_code = ?,
                nearest_landmark_name = ?, nearest_landmark_lat = ?, nearest_landmark_lng = ?,
                landmark_distance_m = ?, landmark_google_place_id = ?,
                nearest_main_station_name = ?, nearest_main_station_lat = ?,
                nearest_main_station_lng = ?, main_station_distance_m = ?,
                main_station_google_place_id = ?,
                total_restaurants_500m = ?, total_cafes_500m = ?, total_transit_500m = ?,
                enriched_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                location_source_id,
                neighborhood_name, district_name, city_name, postal_code,
                nearest_landmark_name, nearest_landmark_lat, nearest_landmark_lng,
                landmark_distance_m, landmark_google_place_id,
                nearest_main_station_name, nearest_main_station_lat,
                nearest_main_station_lng, main_station_distance_m,
                main_station_google_place_id,
                total_restaurants_500m, total_cafes_500m, total_transit_500m
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            # USING
            location_source_id,
            # UPDATE
            neighborhood.get("neighborhood"), neighborhood.get("district"),
            neighborhood.get("city"), neighborhood.get("postal_code"),
            landmark["name"] if landmark else None,
            landmark["geometry"]["location"]["lat"] if landmark else None,
            landmark["geometry"]["location"]["lng"] if landmark else None,
            _haversine_meters(lat, lng, landmark["geometry"]["location"]["lat"], landmark["geometry"]["location"]["lng"]) if landmark else None,
            landmark["place_id"] if landmark else None,
            station["name"] if station else None,
            station["geometry"]["location"]["lat"] if station else None,
            station["geometry"]["location"]["lng"] if station else None,
            _haversine_meters(lat, lng, station["geometry"]["location"]["lat"], station["geometry"]["location"]["lng"]) if station else None,
            station["place_id"] if station else None,
            row.get("restaurants", 0), row.get("cafes", 0), transit_count or 0,
            # INSERT
            location_source_id,
            neighborhood.get("neighborhood"), neighborhood.get("district"),
            neighborhood.get("city"), neighborhood.get("postal_code"),
            landmark["name"] if landmark else None,
            landmark["geometry"]["location"]["lat"] if landmark else None,
            landmark["geometry"]["location"]["lng"] if landmark else None,
            _haversine_meters(lat, lng, landmark["geometry"]["location"]["lat"], landmark["geometry"]["location"]["lng"]) if landmark else None,
            landmark["place_id"] if landmark else None,
            station["name"] if station else None,
            station["geometry"]["location"]["lat"] if station else None,
            station["geometry"]["location"]["lng"] if station else None,
            _haversine_meters(lat, lng, station["geometry"]["location"]["lat"], station["geometry"]["location"]["lng"]) if station else None,
            station["place_id"] if station else None,
            row.get("restaurants", 0), row.get("cafes", 0), transit_count or 0,
        ))

    # ── Google Maps API calls ─────────────────────────────────

    def _nearby_search(
        self, lat: float, lng: float,
        place_type: str, radius: int, max_results: int = 10,
    ) -> list[dict]:
        """Call Google Maps Nearby Search API."""
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            "location": f"{lat},{lng}",
            "radius": radius,
            "type": place_type,
            "key": self.api_key,
        }

        self._rate_limit()
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") not in ("OK", "ZERO_RESULTS"):
            logger.warning(f"Nearby search error: {data.get('status')} — {data.get('error_message')}")
            return []

        results = data.get("results", [])
        return results[:max_results]

    def _reverse_geocode(self, lat: float, lng: float) -> dict:
        """Reverse geocode to extract neighborhood, district, city."""
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "latlng": f"{lat},{lng}",
            "key": self.api_key,
        }

        self._rate_limit()
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        result = {
            "neighborhood": None,
            "district": None,
            "city": None,
            "postal_code": None,
        }

        if data.get("status") != "OK" or not data.get("results"):
            return result

        # Walk through address components of the most detailed result
        for gmaps_result in data["results"]:
            for component in gmaps_result.get("address_components", []):
                types = component.get("types", [])
                name = component.get("long_name")

                if "neighborhood" in types and not result["neighborhood"]:
                    result["neighborhood"] = name
                elif "sublocality" in types and not result["neighborhood"]:
                    result["neighborhood"] = name
                elif "sublocality_level_1" in types and not result["district"]:
                    result["district"] = name
                elif "administrative_area_level_2" in types and not result["district"]:
                    result["district"] = name
                elif "locality" in types and not result["city"]:
                    result["city"] = name
                elif "postal_code" in types and not result["postal_code"]:
                    result["postal_code"] = name

        return result

    def _rate_limit(self):
        """Simple rate limiter: max ~10 requests per second."""
        self._request_count += 1
        if self._request_count % 10 == 0:
            time.sleep(1)

    # ── Database helpers ──────────────────────────────────────

    def _get_locations_to_enrich(self, force: bool = False) -> list[dict]:
        """Get locations that haven't been enriched yet."""
        if force:
            return self.sql.execute_query("""
                SELECT source_id, name, latitude, longitude
                FROM silver.nexudus_locations
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            """)

        return self.sql.execute_query("""
            SELECT l.source_id, l.name, l.latitude, l.longitude
            FROM silver.nexudus_locations l
            LEFT JOIN meta.gmaps_enrichment_log g
                ON g.location_source_id = l.source_id AND g.status = 'success'
            WHERE l.latitude IS NOT NULL
              AND l.longitude IS NOT NULL
              AND g.id IS NULL
        """)

    def _get_location(self, location_source_id: int) -> dict:
        """Get a single location by source_id."""
        rows = self.sql.execute_query(
            "SELECT source_id, name, latitude, longitude FROM silver.nexudus_locations WHERE source_id = ?",
            (location_source_id,),
        )
        if not rows:
            raise ValueError(f"Location {location_source_id} not found in silver")
        return rows[0]

    def _log_enrichment(
        self, location_source_id: int, name: str, status: str,
        pois: int = None, transit: int = None, error: str = None,
        started_at: datetime = None,
    ):
        self.sql.execute_non_query("""
            INSERT INTO meta.gmaps_enrichment_log
                (location_source_id, location_name, status, pois_found, transit_found,
                 error_message, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, GETUTCDATE())
        """, (
            location_source_id, name, status, pois, transit, error,
            started_at or datetime.now(timezone.utc),
        ))