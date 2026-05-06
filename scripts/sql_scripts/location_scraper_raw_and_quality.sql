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

IF OBJECT_ID(N'bronze.location_scraper_lusha_diagnostics', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.location_scraper_lusha_diagnostics (
        id                          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        run_id                      NVARCHAR(100)    NOT NULL,
        source                      NVARCHAR(50)     NOT NULL,
        city                        NVARCHAR(100)    NOT NULL,
        agency_name                 NVARCHAR(255)    NULL,
        first_name                  NVARCHAR(255)    NULL,
        last_name                   NVARCHAR(255)    NULL,
        path                        NVARCHAR(100)    NULL,
        allow_company_fallback      BIT              NULL,
        has_contact                 BIT              NOT NULL DEFAULT 0,
        reason                      NVARCHAR(255)    NULL,
        individual_email_source     NVARCHAR(100)    NULL,
        company_name_cleaned        NVARCHAR(255)    NULL,
        lusha_search_mode           NVARCHAR(100)    NULL,
        domain_used                 NVARCHAR(255)    NULL,
        domains_found               INT              NOT NULL DEFAULT 0,
        google_domains_json         NVARCHAR(MAX)    NULL,
        raw_contacts_found          INT              NOT NULL DEFAULT 0,
        final_contacts_found        INT              NOT NULL DEFAULT 0,
        created_at                  DATETIME2        NOT NULL DEFAULT GETUTCDATE()
    );

    CREATE INDEX IX_location_scraper_lusha_diag_run
        ON bronze.location_scraper_lusha_diagnostics (run_id, source, city);

    CREATE INDEX IX_location_scraper_lusha_diag_reason
        ON bronze.location_scraper_lusha_diagnostics (source, city, reason)
        INCLUDE (run_id, agency_name, path, raw_contacts_found, final_contacts_found);
END
GO

IF OBJECT_ID(N'bronze.location_scraper_lusha_diagnostics', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'bronze.location_scraper_lusha_diagnostics', N'company_name_cleaned') IS NULL
        ALTER TABLE bronze.location_scraper_lusha_diagnostics ADD company_name_cleaned NVARCHAR(255) NULL;
    IF COL_LENGTH(N'bronze.location_scraper_lusha_diagnostics', N'lusha_search_mode') IS NULL
        ALTER TABLE bronze.location_scraper_lusha_diagnostics ADD lusha_search_mode NVARCHAR(100) NULL;
    IF COL_LENGTH(N'bronze.location_scraper_lusha_diagnostics', N'domain_used') IS NULL
        ALTER TABLE bronze.location_scraper_lusha_diagnostics ADD domain_used NVARCHAR(255) NULL;
END
GO
