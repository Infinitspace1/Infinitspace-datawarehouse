-- Location Scraper bronze schema
-- Run once before deploying the location scraper Azure Function.

CREATE TABLE bronze.n8n_location_scraper_buildings (
    id                   UNIQUEIDENTIFIER  PRIMARY KEY DEFAULT NEWID(),
    source               NVARCHAR(50)      NOT NULL,
    external_id          NVARCHAR(255),
    web_link             NVARCHAR(1000),
    link_to_gmap         NVARCHAR(500),
    latitude             NUMERIC(9,6),
    longitude            NUMERIC(9,6),
    address              NVARCHAR(500),
    postal_code          NVARCHAR(20),
    district             NVARCHAR(255),
    city                 NVARCHAR(100),
    floor                SMALLINT,
    floor_raw            NVARCHAR(50),
    is_exterior          BIT,
    has_lift             BIT,
    has_air_conditioning BIT,
    match_confidence     NVARCHAR(50)      DEFAULT 'inferred',
    updated_at           DATETIME2         DEFAULT GETUTCDATE()
);

CREATE INDEX ix_loc_buildings_city_latlon
    ON bronze.n8n_location_scraper_buildings (city, latitude, longitude);

-- -----------------------------------------------------------------------

CREATE TABLE bronze.n8n_location_scraper_listings (
    id                   UNIQUEIDENTIFIER  PRIMARY KEY DEFAULT NEWID(),
    building_id          UNIQUEIDENTIFIER  NOT NULL
        REFERENCES bronze.n8n_location_scraper_buildings(id),
    run_id               NVARCHAR(100),
    status               NVARCHAR(50),
    surface_m2           DECIMAL(10,2),   -- canonical, always m² (sort/compare/guardrail)
    surface_display      DECIMAL(12,2),   -- value in display unit (sqft for UK/US, else m²)
    surface_unit         NVARCHAR(10),    -- 'sqft' | 'm2'
    price_monthly        DECIMAL(12,2),
    price_per_m2         DECIMAL(10,2),
    currency             NVARCHAR(10),
    energy_class         NVARCHAR(10),
    days_on_market       INT,
    first_listed_date    DATE,
    last_updated_date    DATE,
    first_time_extract   DATE,
    last_seen_date       DATE              NOT NULL
);

CREATE INDEX ix_loc_listings_building_id
    ON bronze.n8n_location_scraper_listings (building_id, last_seen_date DESC);

-- -----------------------------------------------------------------------

CREATE TABLE bronze.n8n_location_scraper_contacts (
    id           UNIQUEIDENTIFIER  PRIMARY KEY DEFAULT NEWID(),
    name         NVARCHAR(255),
    phone        NVARCHAR(50),
    email        NVARCHAR(255)     UNIQUE,
    contact_type NVARCHAR(50),
    title        NVARCHAR(255),
    confidence   DECIMAL(5,2),
    linkedin     NVARCHAR(500),
    source       NVARCHAR(50),     -- 'scraper' | 'lusha'
    updated_at   DATETIME2         DEFAULT GETUTCDATE()
);

-- -----------------------------------------------------------------------

CREATE TABLE bronze.n8n_location_scraper_listing_contacts (
    listing_id  UNIQUEIDENTIFIER  NOT NULL
        REFERENCES bronze.n8n_location_scraper_listings(id),
    contact_id  UNIQUEIDENTIFIER  NOT NULL
        REFERENCES bronze.n8n_location_scraper_contacts(id),
    PRIMARY KEY (listing_id, contact_id)
);

-- -----------------------------------------------------------------------

CREATE TABLE bronze.n8n_location_scraper_logs (
    run_id            NVARCHAR(100)  PRIMARY KEY,
    city              NVARCHAR(100),
    run_date          DATE,
    source            NVARCHAR(50),
    buildings_found   INT            DEFAULT 0,
    buildings_new     INT            DEFAULT 0,
    buildings_updated INT            DEFAULT 0,
    status            NVARCHAR(20)   DEFAULT 'running',  -- running | completed | failed
    created_at        DATETIME2      DEFAULT GETUTCDATE(),
    updated_at        DATETIME2      DEFAULT GETUTCDATE()
);
