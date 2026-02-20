-- ============================================================
-- silver_nexudus_locations.sql
--
-- Two tables:
--   silver.nexudus_locations       — one row per location
--   silver.nexudus_location_hours  — one row per location per day (7 rows per location)
--
-- Source: bronze.nexudus_locations
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- silver.nexudus_locations
-- ──────────────────────────────────────────────────────────────
CREATE TABLE silver.nexudus_locations (
    id                  BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id           BIGINT          NOT NULL,           -- Nexudus Id
    CONSTRAINT uq_silver_nexudus_locations_source_id UNIQUE (source_id),

    -- Traceability back to bronze
    bronze_id           BIGINT          NULL,               -- bronze.nexudus_locations.id
    sync_run_id         UNIQUEIDENTIFIER NULL,

    -- Identity / naming
    nexudus_uuid        NVARCHAR(64)    NULL,               -- UniqueId (GUID from Nexudus)
    name                NVARCHAR(512)   NOT NULL,           -- "Amsterdam - Center - Herengracht"
    web_address         NVARCHAR(256)   NULL,               -- subdomain slug e.g. "beyond"

    -- Location
    address             NVARCHAR(1024)  NULL,
    postal_code         NVARCHAR(32)    NULL,
    city                NVARCHAR(255)   NULL,               -- TownCity
    state               NVARCHAR(255)   NULL,
    country_name        NVARCHAR(128)   NULL,
    country_id          INT             NULL,               -- Nexudus internal country int
    latitude            FLOAT           NULL,
    longitude           FLOAT           NULL,

    -- Contact
    phone               NVARCHAR(64)    NULL,
    email               NVARCHAR(512)   NULL,               -- EmailContact
    web_contact         NVARCHAR(512)   NULL,               -- website URL

    -- Financial
    currency_code       NVARCHAR(8)     NULL,

    -- Content (HTML stripped)
    description         NVARCHAR(MAX)   NULL,               -- AboutUs, tags stripped
    short_intro         NVARCHAR(MAX)   NULL,               -- ShortIntroduction, tags stripped

    -- Nexudus timestamps
    created_on          DATETIME2       NULL,               -- CreatedOn
    updated_on          DATETIME2       NULL,               -- UpdatedOn

    -- Pipeline timestamps
    first_seen_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_locations_source_id ON silver.nexudus_locations (source_id);
CREATE INDEX ix_silver_nexudus_locations_city      ON silver.nexudus_locations (city);
GO


-- ──────────────────────────────────────────────────────────────
-- silver.nexudus_location_hours
-- 7 rows per location (one per day of week)
-- Storing open/close as minutes-since-midnight (matches Nexudus format)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE silver.nexudus_location_hours (
    id                  BIGINT          IDENTITY(1,1) PRIMARY KEY,
    location_source_id  BIGINT          NOT NULL,           -- FK → silver.nexudus_locations.source_id
    day_of_week         TINYINT         NOT NULL,           -- 1=Monday … 7=Sunday
    day_name            NVARCHAR(16)    NOT NULL,           -- 'Monday', 'Tuesday', etc.
    is_closed           BIT             NOT NULL DEFAULT 0,
    open_time           SMALLINT        NULL,               -- minutes since midnight (e.g. 540 = 09:00)
    close_time          SMALLINT        NULL,               -- minutes since midnight (e.g. 1020 = 17:00)
    last_synced_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT uq_silver_nexudus_location_hours UNIQUE (location_source_id, day_of_week)
);

CREATE INDEX ix_silver_nexudus_location_hours_location ON silver.nexudus_location_hours (location_source_id);
GO