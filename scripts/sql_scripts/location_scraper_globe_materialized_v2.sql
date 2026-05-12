-- Materialized globe read model table for Location Scraper (v2)
-- Purpose: fast app reads without parsing JSON at query time.
--
-- Note:
--   raw -> silver materialization is handled by Function App activity
--   `ls_materialize_globe` (Python), not by SQL procedures/views.

IF OBJECT_ID(N'silver.location_scraper_globe_v2', N'U') IS NULL
BEGIN
    CREATE TABLE silver.location_scraper_globe_v2 (
        run_id         NVARCHAR(100)   NOT NULL,
        item_index     INT             NOT NULL,
        source         NVARCHAR(50)    NOT NULL,
        run_city       NVARCHAR(100)   NOT NULL,
        inserted_at    DATETIME2       NOT NULL,

        external_id    NVARCHAR(255)   NULL,
        listing_url    NVARCHAR(2000)  NULL,

        latitude       FLOAT           NULL,
        longitude      FLOAT           NULL,

        address        NVARCHAR(1000)  NULL,
        postal_code    NVARCHAR(50)    NULL,
        district       NVARCHAR(255)   NULL,
        city           NVARCHAR(255)   NULL,
        country_code   NVARCHAR(10)    NULL,

        price_monthly  DECIMAL(18,2)   NULL,
        price_per_m2   DECIMAL(18,2)   NULL,
        surface_m2     DECIMAL(18,2)   NULL,
        currency       NVARCHAR(20)    NULL,

        contact_name   NVARCHAR(255)   NULL,
        company_name   NVARCHAR(255)   NULL,
        phone          NVARCHAR(100)   NULL,

        lusha_email_1       NVARCHAR(255)   NULL,
        lusha_contact_1     NVARCHAR(255)   NULL,
        lusha_title_1       NVARCHAR(255)   NULL,
        lusha_confidence_1  DECIMAL(5,2)    NULL,
        lusha_email_2       NVARCHAR(255)   NULL,
        lusha_contact_2     NVARCHAR(255)   NULL,
        lusha_title_2       NVARCHAR(255)   NULL,
        lusha_confidence_2  DECIMAL(5,2)    NULL,
        lusha_email_3       NVARCHAR(255)   NULL,
        lusha_contact_3     NVARCHAR(255)   NULL,
        lusha_title_3       NVARCHAR(255)   NULL,
        lusha_confidence_3  DECIMAL(5,2)    NULL,

        hubspot_exported        BIT           NOT NULL DEFAULT 0,
        hubspot_re_location_id  NVARCHAR(64)  NULL,

        refreshed_at   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),

        CONSTRAINT PK_location_scraper_globe_v2
            PRIMARY KEY (run_id, item_index)
    );
END
GO

IF OBJECT_ID(N'silver.location_scraper_globe_v2', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_email_1') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_email_1 NVARCHAR(255) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_contact_1') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_contact_1 NVARCHAR(255) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_title_1') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_title_1 NVARCHAR(255) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_confidence_1') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_confidence_1 DECIMAL(5,2) NULL;

    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_email_2') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_email_2 NVARCHAR(255) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_contact_2') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_contact_2 NVARCHAR(255) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_title_2') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_title_2 NVARCHAR(255) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_confidence_2') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_confidence_2 DECIMAL(5,2) NULL;

    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_email_3') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_email_3 NVARCHAR(255) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_contact_3') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_contact_3 NVARCHAR(255) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_title_3') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_title_3 NVARCHAR(255) NULL;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'lusha_confidence_3') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD lusha_confidence_3 DECIMAL(5,2) NULL;

    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'hubspot_exported') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD hubspot_exported BIT NOT NULL CONSTRAINT DF_location_scraper_globe_v2_hubspot_exported DEFAULT 0;
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'hubspot_re_location_id') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2 ADD hubspot_re_location_id NVARCHAR(64) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'silver.location_scraper_globe_v2', N'U')
      AND name = N'IX_location_scraper_globe_v2_source_city_inserted'
)
BEGIN
    CREATE INDEX IX_location_scraper_globe_v2_source_city_inserted
        ON silver.location_scraper_globe_v2 (source, run_city, inserted_at DESC);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'silver.location_scraper_globe_v2', N'U')
      AND name = N'IX_location_scraper_globe_v2_geo'
)
BEGIN
    CREATE INDEX IX_location_scraper_globe_v2_geo
        ON silver.location_scraper_globe_v2 (latitude, longitude)
        INCLUDE (source, run_city, external_id, listing_url);
END
GO
