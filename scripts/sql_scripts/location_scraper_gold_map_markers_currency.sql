-- Add listing currency to gold.location_scraper_map_markers.
-- The dashboard should format each marker with its source currency instead of
-- assuming EUR for every market.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'gold')
    EXEC(N'CREATE SCHEMA gold');
GO

IF OBJECT_ID(N'gold.location_scraper_map_markers', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'gold.location_scraper_map_markers', N'currency') IS NULL
        ALTER TABLE gold.location_scraper_map_markers ADD currency NVARCHAR(20) NULL;
END
GO

IF OBJECT_ID(N'gold.location_scraper_map_markers', N'U') IS NOT NULL
   AND COL_LENGTH(N'gold.location_scraper_map_markers', N'currency') IS NOT NULL
BEGIN
    ;WITH latest_silver AS (
        SELECT
            source,
            run_city,
            external_id,
            latitude,
            longitude,
            currency,
            ROW_NUMBER() OVER (
                PARTITION BY
                    source,
                    run_city,
                    COALESCE(external_id, CONCAT(latitude, N'|', longitude))
                ORDER BY inserted_at DESC, run_id DESC, item_index DESC
            ) AS rn
        FROM silver.location_scraper_globe_v2
        WHERE currency IS NOT NULL
    )
    UPDATE gold_markers
    SET currency = latest_silver.currency
    FROM gold.location_scraper_map_markers AS gold_markers
    JOIN latest_silver
        ON latest_silver.rn = 1
       AND latest_silver.source = gold_markers.source
       AND latest_silver.run_city = gold_markers.run_city
       AND (
            (latest_silver.external_id IS NOT NULL AND latest_silver.external_id = gold_markers.external_id)
            OR (
                latest_silver.external_id IS NULL
                AND gold_markers.external_id IS NULL
                AND latest_silver.latitude = gold_markers.latitude
                AND latest_silver.longitude = gold_markers.longitude
            )
       );
END
GO
