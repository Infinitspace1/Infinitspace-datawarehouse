-- Propagate the IS24 price breakdown (added to silver.location_scraper_globe_v2
-- in location_scraper_globe_v2_price_breakdown.sql) up to gold so the dashboard
-- map markers can show per-m^2 pricing.
--
-- Adds 8 columns + CREATE OR ALTER on the refresh sproc. Idempotent.
-- After applying: EXEC gold.sp_refresh_location_scraper_map_markers; to rebuild markers.

IF OBJECT_ID(N'gold.location_scraper_map_markers', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'gold.location_scraper_map_markers', N'price_per_m2') IS NULL
        ALTER TABLE gold.location_scraper_map_markers ADD price_per_m2 DECIMAL(18,2) NULL;
    IF COL_LENGTH(N'gold.location_scraper_map_markers', N'min_price_per_m2') IS NULL
        ALTER TABLE gold.location_scraper_map_markers ADD min_price_per_m2 DECIMAL(18,2) NULL;
    IF COL_LENGTH(N'gold.location_scraper_map_markers', N'max_price_per_m2') IS NULL
        ALTER TABLE gold.location_scraper_map_markers ADD max_price_per_m2 DECIMAL(18,2) NULL;
    IF COL_LENGTH(N'gold.location_scraper_map_markers', N'additional_costs_per_m2') IS NULL
        ALTER TABLE gold.location_scraper_map_markers ADD additional_costs_per_m2 DECIMAL(18,4) NULL;
    IF COL_LENGTH(N'gold.location_scraper_map_markers', N'total_price_per_m2') IS NULL
        ALTER TABLE gold.location_scraper_map_markers ADD total_price_per_m2 DECIMAL(18,4) NULL;
    IF COL_LENGTH(N'gold.location_scraper_map_markers', N'divisible_from_m2') IS NULL
        ALTER TABLE gold.location_scraper_map_markers ADD divisible_from_m2 DECIMAL(18,2) NULL;
    IF COL_LENGTH(N'gold.location_scraper_map_markers', N'price_kind') IS NULL
        ALTER TABLE gold.location_scraper_map_markers ADD price_kind NVARCHAR(50) NULL;
    IF COL_LENGTH(N'gold.location_scraper_map_markers', N'price_monthly_is_estimated') IS NULL
        ALTER TABLE gold.location_scraper_map_markers ADD price_monthly_is_estimated BIT NULL;
END
GO

CREATE OR ALTER PROCEDURE gold.sp_refresh_location_scraper_map_markers
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM gold.location_scraper_map_markers;

    WITH run_freshness AS (
        SELECT
            source,
            run_city,
            run_id,
            MAX(inserted_at) AS latest_inserted_at
        FROM silver.location_scraper_globe_v2
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
        GROUP BY source, run_city, run_id
    ),
    latest_runs AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY source, run_city
                   ORDER BY latest_inserted_at DESC, run_id DESC
               ) AS rn
        FROM run_freshness
    ),
    scoped AS (
        SELECT
            g.*,
            CAST(ROUND(g.latitude, 4) AS DECIMAL(9,4)) AS marker_latitude,
            CAST(ROUND(g.longitude, 4) AS DECIMAL(9,4)) AS marker_longitude
        FROM silver.location_scraper_globe_v2 AS g
        JOIN latest_runs AS r
          ON r.source = g.source
         AND r.run_city = g.run_city
         AND r.run_id = g.run_id
         AND r.rn = 1
        WHERE g.latitude IS NOT NULL
          AND g.longitude IS NOT NULL
    ),
    ranked AS (
        SELECT
            s.*,
            COUNT(*) OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
            ) AS listing_count,
            MIN(price_monthly) OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
            ) AS min_price_monthly,
            MAX(price_monthly) OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
            ) AS max_price_monthly,
            MIN(price_per_m2) OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
            ) AS min_price_per_m2_w,
            MAX(price_per_m2) OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
            ) AS max_price_per_m2_w,
            MIN(divisible_from_m2) OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
            ) AS min_divisible_from_m2_w,
            SUM(surface_m2) OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
            ) AS total_surface_m2,
            MIN(surface_m2) OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
            ) AS min_surface_m2,
            MAX(surface_m2) OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
            ) AS max_surface_m2,
            ROW_NUMBER() OVER (
                PARTITION BY source, run_city, run_id, marker_latitude, marker_longitude
                ORDER BY
                    hubspot_exported DESC,
                    CASE WHEN lusha_email_1 IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN listing_url IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN external_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN address IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN company_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN price_monthly IS NOT NULL THEN 1 ELSE 0 END DESC,
                    item_index ASC
            ) AS marker_rank
        FROM scoped AS s
    )
    INSERT INTO gold.location_scraper_map_markers (
        marker_id,
        source, run_city, run_id,
        marker_latitude, marker_longitude,
        representative_item_index, listing_count,
        min_price_monthly, max_price_monthly,
        total_surface_m2, min_surface_m2, max_surface_m2,
        external_id, listing_url,
        latitude, longitude,
        address, postal_code, district, city, country_code,
        contact_name, company_name, phone,
        lusha_email_1, lusha_contact_1, lusha_title_1, lusha_confidence_1,
        hubspot_exported, hubspot_re_location_id,
        currency,
        price_per_m2, min_price_per_m2, max_price_per_m2,
        additional_costs_per_m2, total_price_per_m2,
        divisible_from_m2, price_kind, price_monthly_is_estimated,
        refreshed_at
    )
    SELECT
        CONCAT(source, '|', run_city, '|', run_id, '|', marker_latitude, '|', marker_longitude),
        source, run_city, run_id,
        marker_latitude, marker_longitude,
        item_index, listing_count,
        min_price_monthly, max_price_monthly,
        total_surface_m2, min_surface_m2, max_surface_m2,
        external_id, listing_url,
        latitude, longitude,
        address, postal_code, district, city, country_code,
        contact_name, company_name, phone,
        lusha_email_1, lusha_contact_1, lusha_title_1, lusha_confidence_1,
        hubspot_exported, hubspot_re_location_id,
        currency,
        price_per_m2, min_price_per_m2_w, max_price_per_m2_w,
        additional_costs_per_m2, total_price_per_m2,
        min_divisible_from_m2_w, price_kind, price_monthly_is_estimated,
        GETUTCDATE()
    FROM ranked
    WHERE marker_rank = 1;
END
GO
