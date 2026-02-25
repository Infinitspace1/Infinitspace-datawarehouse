-- ============================================================
-- silver_location_gmaps_schema.sql
--
-- Google Maps enrichment tables for coworking locations.
-- These are populated on-demand (not scheduled), and only
-- refreshed when a new location is added or manually triggered.
--
-- Tables:
--   silver.location_nearby_pois       — restaurants, cafes, gyms, etc.
--   silver.location_transit_stations   — metro, train, tram, bus stops
--   silver.location_neighborhoods      — area-level context per location
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- 1. Nearby Points of Interest
--
-- Stores places found via Google Maps Nearby Search around
-- each coworking location. Used to answer:
--   "Are there restaurants near X?"
--   "What's near the Herengracht office?"
--   "Is there a gym close to your Berlin space?"
-- ──────────────────────────────────────────────────────────────

CREATE TABLE silver.location_nearby_pois (
    id                      BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Link to coworking location
    location_source_id      BIGINT          NOT NULL,       -- FK → silver.nexudus_locations.source_id

    -- Google Maps identity
    google_place_id         NVARCHAR(512)   NOT NULL,       -- stable Google Place ID
    CONSTRAINT uq_location_poi UNIQUE (location_source_id, google_place_id),

    -- Classification
    poi_category            NVARCHAR(64)    NOT NULL,       -- our category: 'restaurant', 'cafe', 'gym', 'supermarket', etc.
    google_primary_type     NVARCHAR(128)   NULL,           -- Google's primary type: 'italian_restaurant', 'cafe', etc.
    google_types            NVARCHAR(MAX)   NULL,           -- all Google types, comma-separated

    -- Identity
    name                    NVARCHAR(512)   NOT NULL,
    address                 NVARCHAR(1024)  NULL,           -- formatted address

    -- Location
    latitude                FLOAT           NOT NULL,
    longitude               FLOAT           NOT NULL,
    distance_meters         INT             NULL,           -- straight-line distance from coworking
    walking_minutes         INT             NULL,           -- estimated walk time (distance_meters / 80)

    -- Quality signals
    rating                  DECIMAL(2,1)    NULL,           -- Google rating (1.0-5.0)
    total_ratings           INT             NULL,           -- number of reviews
    price_level             TINYINT         NULL,           -- 0=Free, 1=Inexpensive, 2=Moderate, 3=Expensive, 4=Very Expensive

    -- Status
    business_status         NVARCHAR(32)    NULL,           -- OPERATIONAL, CLOSED_TEMPORARILY, CLOSED_PERMANENTLY
    opening_hours_text      NVARCHAR(MAX)   NULL,           -- human-readable hours, newline-separated

    -- Metadata
    search_radius_meters    INT             NOT NULL,       -- radius used for this search (e.g. 500, 1000)
    enriched_at             DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    google_data_json        NVARCHAR(MAX)   NULL            -- full Google response for future use
);

CREATE INDEX ix_location_pois_location   ON silver.location_nearby_pois (location_source_id);
CREATE INDEX ix_location_pois_category   ON silver.location_nearby_pois (poi_category);
CREATE INDEX ix_location_pois_distance   ON silver.location_nearby_pois (distance_meters);
CREATE INDEX ix_location_pois_rating     ON silver.location_nearby_pois (rating DESC);
GO


-- ──────────────────────────────────────────────────────────────
-- 2. Nearby Transit Stations
--
-- Public transport stops/stations near each location.
-- Separated from POIs because these are the most frequently
-- asked about and need specific fields. Used to answer:
--   "How do I get to your Republica office?"
--   "Which spaces are near a metro?"
--   "What's the nearest train station to Herengracht?"
-- ──────────────────────────────────────────────────────────────

CREATE TABLE silver.location_transit_stations (
    id                      BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Link to coworking location
    location_source_id      BIGINT          NOT NULL,       -- FK → silver.nexudus_locations.source_id

    -- Google Maps identity
    google_place_id         NVARCHAR(512)   NOT NULL,
    CONSTRAINT uq_location_transit UNIQUE (location_source_id, google_place_id),

    -- Classification
    transit_type            NVARCHAR(32)    NOT NULL,       -- 'metro', 'train', 'tram', 'bus', 'ferry'
    google_types            NVARCHAR(MAX)   NULL,

    -- Identity
    name                    NVARCHAR(512)   NOT NULL,       -- "Amsterdam Centraal", "Waterlooplein"
    address                 NVARCHAR(1024)  NULL,

    -- Location
    latitude                FLOAT           NOT NULL,
    longitude               FLOAT           NOT NULL,
    distance_meters         INT             NULL,
    walking_minutes         INT             NULL,

    -- Transit details (if available from Google)
    transit_lines           NVARCHAR(MAX)   NULL,           -- lines serving this station, e.g. "M51, M53, M54"

    -- Metadata
    search_radius_meters    INT             NOT NULL,
    enriched_at             DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    google_data_json        NVARCHAR(MAX)   NULL
);

CREATE INDEX ix_location_transit_location ON silver.location_transit_stations (location_source_id);
CREATE INDEX ix_location_transit_type     ON silver.location_transit_stations (transit_type);
CREATE INDEX ix_location_transit_distance ON silver.location_transit_stations (distance_meters);
GO


-- ──────────────────────────────────────────────────────────────
-- 3. Location Neighborhood Context
--
-- One row per location with area-level info. Used to answer:
--   "What's the area like around your Berlin office?"
--   "Is the Herengracht office in the city center?"
--   "What neighborhood is Republica in?"
-- ──────────────────────────────────────────────────────────────

CREATE TABLE silver.location_neighborhoods (
    id                      BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Link to coworking location
    location_source_id      BIGINT          NOT NULL,
    CONSTRAINT uq_location_neighborhood UNIQUE (location_source_id),

    -- Neighborhood info (from Google Geocoding reverse lookup)
    neighborhood_name       NVARCHAR(256)   NULL,           -- e.g. "Grachtengordel", "Shoreditch"
    district_name           NVARCHAR(256)   NULL,           -- e.g. "Amsterdam-Centrum", "Hackney"
    city_name               NVARCHAR(256)   NULL,           -- e.g. "Amsterdam", "London"
    postal_code             NVARCHAR(32)    NULL,

    -- Key landmarks (nearest major landmark for reference)
    nearest_landmark_name   NVARCHAR(512)   NULL,           -- e.g. "Dam Square", "Tower Bridge"
    nearest_landmark_lat    FLOAT           NULL,
    nearest_landmark_lng    FLOAT           NULL,
    landmark_distance_m     INT             NULL,
    landmark_google_place_id NVARCHAR(512)  NULL,

    -- Nearest major train station (for "how to get there" answers)
    nearest_main_station_name       NVARCHAR(256)   NULL,   -- e.g. "Amsterdam Centraal"
    nearest_main_station_lat        FLOAT           NULL,
    nearest_main_station_lng        FLOAT           NULL,
    main_station_distance_m         INT             NULL,
    main_station_google_place_id    NVARCHAR(512)   NULL,

    -- Summary stats from POIs (denormalized for quick chatbot answers)
    total_restaurants_500m  INT             NULL,
    total_cafes_500m        INT             NULL,
    total_transit_500m      INT             NULL,

    -- Metadata
    enriched_at             DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_location_neighborhoods_location ON silver.location_neighborhoods (location_source_id);
GO


-- ──────────────────────────────────────────────────────────────
-- 4. Enrichment tracking
--
-- Tracks which locations have been enriched and when,
-- so we can detect new locations that need enrichment.
-- ──────────────────────────────────────────────────────────────

CREATE TABLE meta.gmaps_enrichment_log (
    id                      BIGINT          IDENTITY(1,1) PRIMARY KEY,
    location_source_id      BIGINT          NOT NULL,
    location_name           NVARCHAR(512)   NULL,
    status                  NVARCHAR(32)    NOT NULL,       -- 'success', 'failed', 'skipped'
    pois_found              INT             NULL,
    transit_found           INT             NULL,
    error_message           NVARCHAR(MAX)   NULL,
    started_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    finished_at             DATETIME2       NULL,
    CONSTRAINT uq_gmaps_enrichment UNIQUE (location_source_id, started_at)
);

CREATE INDEX ix_gmaps_enrichment_location ON meta.gmaps_enrichment_log (location_source_id);
GO

-- improve points of interests because too few

SELECT loc.name,
        pois.*
 FROM silver.location_nearby_pois pois
inner join silver.nexudus_locations loc 
on pois.location_source_id = loc.source_id
WHERE location_source_id = 1420976575


SELECT loc.name,
        neigh.*
 FROM silver.location_neighborhoods neigh
inner join silver.nexudus_locations loc 
on neigh.location_source_id = loc.source_id
WHERE location_source_id = 1420976575

SELECT loc.name,
        transit.*
 FROM silver.location_transit_stations transit
inner join silver.nexudus_locations loc 
on transit.location_source_id = loc.source_id
WHERE location_source_id = 1420976575