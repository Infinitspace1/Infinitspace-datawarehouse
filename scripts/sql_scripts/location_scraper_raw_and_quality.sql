-- Location Scraper: raw Apify payloads + per-run quality metrics
-- Run after location_scraper_schema.sql (or alongside in fresh env).

IF OBJECT_ID(N'bronze.location_scraper_raw', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.location_scraper_raw (
        id            UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        run_id        NVARCHAR(100)    NOT NULL,
        source        NVARCHAR(50)     NOT NULL,
        city          NVARCHAR(100)    NOT NULL,
        item_index    INT              NOT NULL,
        payload_json  NVARCHAR(MAX)    NOT NULL,
        inserted_at   DATETIME2        NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT UQ_location_scraper_raw_run_item UNIQUE (run_id, item_index)
    );
    CREATE INDEX IX_location_scraper_raw_run
        ON bronze.location_scraper_raw (run_id);
    CREATE INDEX IX_location_scraper_raw_src_city
        ON bronze.location_scraper_raw (source, city);
END
GO

IF OBJECT_ID(N'bronze.location_scraper_run_quality', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.location_scraper_run_quality (
        run_id                       NVARCHAR(100)   NOT NULL PRIMARY KEY,
        source                       NVARCHAR(50)    NOT NULL,
        city                         NVARCHAR(100)   NOT NULL,
        raw_item_count               INT             NOT NULL DEFAULT 0,
        normalized_count             INT             NOT NULL DEFAULT 0,
        with_coords_count            INT             NOT NULL DEFAULT 0,
        with_phone_count             INT             NOT NULL DEFAULT 0,
        with_name_or_company_count   INT             NOT NULL DEFAULT 0,
        lusha_email_slots            INT             NOT NULL DEFAULT 0,
        buildings_found              INT             NOT NULL DEFAULT 0,
        buildings_new                INT             NOT NULL DEFAULT 0,
        buildings_updated            INT             NOT NULL DEFAULT 0,
        agencies_total               INT             NOT NULL DEFAULT 0,
        agencies_with_contacts       INT             NOT NULL DEFAULT 0,
        enrichment_diagnostics_json  NVARCHAR(MAX)   NULL,
        finished_at                  DATETIME2       NOT NULL DEFAULT GETUTCDATE()
    );
    CREATE INDEX IX_location_scraper_run_quality_finished
        ON bronze.location_scraper_run_quality (finished_at DESC);
    CREATE INDEX IX_location_scraper_run_quality_city
        ON bronze.location_scraper_run_quality (city, source);
END
GO
