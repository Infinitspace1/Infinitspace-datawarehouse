-- Adds office-rent price breakdown columns to silver.location_scraper_globe_v2.
--
-- Motivation: IS24 office listings ("buero-mieten") set normalized.price.amount = null
-- when the property is divisible and priced "from X EUR/m^2". The actor exposes the
-- per-m^2 rent under adTargetingParameters.obj_rentPerSqM and the per-m^2 Nebenkosten
-- in sections[type=TOP_ATTRIBUTES]. With these columns the dashboard can show the full
-- breakdown and derive a monthly total when needed.
--
-- Backfill strategy: re-materialize each existing run via
-- shared/location_scraper/activities/materialize_globe.py once this DDL is applied.

IF OBJECT_ID(N'silver.location_scraper_globe_v2', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'additional_costs_per_m2') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2
            ADD additional_costs_per_m2 DECIMAL(18,4) NULL;

    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'total_price_per_m2') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2
            ADD total_price_per_m2 DECIMAL(18,4) NULL;

    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'divisible_from_m2') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2
            ADD divisible_from_m2 DECIMAL(18,2) NULL;

    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'price_kind') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2
            ADD price_kind NVARCHAR(50) NULL;

    IF COL_LENGTH(N'silver.location_scraper_globe_v2', N'price_monthly_is_estimated') IS NULL
        ALTER TABLE silver.location_scraper_globe_v2
            ADD price_monthly_is_estimated BIT NOT NULL
                CONSTRAINT DF_location_scraper_globe_v2_price_monthly_is_estimated DEFAULT 0;
END
GO
