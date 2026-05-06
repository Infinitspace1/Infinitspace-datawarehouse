-- Location Scraper: materialized quality metrics for globe reads
-- Purpose:
--   Analyze silver.location_scraper_globe_v2 quality by run / source / city
--   without repeatedly writing ad-hoc aggregate queries.
--
-- Usage:
--   1) Run this script once to create the table + refresh procedure.
--   2) After a scraper run, execute:
--        EXEC silver.sp_refresh_location_scraper_globe_quality @run_id = N'<run_id>';
--      Or refresh all available runs:
--        EXEC silver.sp_refresh_location_scraper_globe_quality;
--   3) Analyze:
--        SELECT TOP 50 *
--        FROM silver.location_scraper_globe_quality
--        ORDER BY refreshed_at DESC;

IF SCHEMA_ID(N'silver') IS NULL
    EXEC(N'CREATE SCHEMA silver');
GO

IF OBJECT_ID(N'silver.location_scraper_globe_quality', N'U') IS NULL
BEGIN
    CREATE TABLE silver.location_scraper_globe_quality (
        run_id                         NVARCHAR(100)   NOT NULL,
        source                         NVARCHAR(50)    NOT NULL,
        run_city                       NVARCHAR(100)   NOT NULL,

        first_inserted_at              DATETIME2       NULL,
        last_inserted_at               DATETIME2       NULL,

        raw_item_count                 INT             NOT NULL DEFAULT 0,
        silver_row_count               INT             NOT NULL DEFAULT 0,
        raw_to_silver_delta            INT             NOT NULL DEFAULT 0,

        rows_with_external_id          INT             NOT NULL DEFAULT 0,
        rows_with_listing_url          INT             NOT NULL DEFAULT 0,
        rows_with_coordinates          INT             NOT NULL DEFAULT 0,
        rows_with_address              INT             NOT NULL DEFAULT 0,
        rows_with_postal_code          INT             NOT NULL DEFAULT 0,
        rows_with_district             INT             NOT NULL DEFAULT 0,
        rows_with_price_monthly        INT             NOT NULL DEFAULT 0,
        rows_with_price_per_m2         INT             NOT NULL DEFAULT 0,
        rows_with_surface_m2           INT             NOT NULL DEFAULT 0,
        rows_with_contact_name         INT             NOT NULL DEFAULT 0,
        rows_with_company_name         INT             NOT NULL DEFAULT 0,
        rows_with_phone                INT             NOT NULL DEFAULT 0,
        rows_with_any_lusha_email      INT             NOT NULL DEFAULT 0,
        rows_with_2plus_lusha_emails   INT             NOT NULL DEFAULT 0,
        rows_with_3_lusha_emails       INT             NOT NULL DEFAULT 0,

        pct_external_id                DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_listing_url                DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_coordinates                DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_address                    DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_postal_code                DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_district                   DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_price_monthly              DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_price_per_m2               DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_surface_m2                 DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_contact_name               DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_company_name               DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_phone                      DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_any_lusha_email            DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_2plus_lusha_emails         DECIMAL(5,2)    NOT NULL DEFAULT 0,
        pct_3_lusha_emails             DECIMAL(5,2)    NOT NULL DEFAULT 0,

        distinct_external_ids          INT             NOT NULL DEFAULT 0,
        duplicate_external_id_rows     INT             NOT NULL DEFAULT 0,
        distinct_geo_points            INT             NOT NULL DEFAULT 0,
        duplicate_geo_rows             INT             NOT NULL DEFAULT 0,
        distinct_companies             INT             NOT NULL DEFAULT 0,
        distinct_contact_names         INT             NOT NULL DEFAULT 0,
        distinct_phones                INT             NOT NULL DEFAULT 0,

        min_price_monthly              DECIMAL(18,2)   NULL,
        avg_price_monthly              DECIMAL(18,2)   NULL,
        max_price_monthly              DECIMAL(18,2)   NULL,
        min_price_per_m2               DECIMAL(18,2)   NULL,
        avg_price_per_m2               DECIMAL(18,2)   NULL,
        max_price_per_m2               DECIMAL(18,2)   NULL,
        min_surface_m2                 DECIMAL(18,2)   NULL,
        avg_surface_m2                 DECIMAL(18,2)   NULL,
        max_surface_m2                 DECIMAL(18,2)   NULL,

        bronze_normalized_count        INT             NULL,
        bronze_with_coords_count       INT             NULL,
        bronze_with_phone_count        INT             NULL,
        bronze_lusha_email_slots       INT             NULL,
        bronze_agencies_total          INT             NULL,
        bronze_agencies_with_contacts  INT             NULL,
        enrichment_diagnostics_json    NVARCHAR(MAX)   NULL,

        refreshed_at                   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),

        CONSTRAINT PK_location_scraper_globe_quality
            PRIMARY KEY (run_id, source, run_city)
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'silver.location_scraper_globe_quality', N'U')
      AND name = N'IX_location_scraper_globe_quality_refreshed'
)
BEGIN
    CREATE INDEX IX_location_scraper_globe_quality_refreshed
        ON silver.location_scraper_globe_quality (refreshed_at DESC)
        INCLUDE (run_id, source, run_city, silver_row_count, pct_any_lusha_email);
END
GO

CREATE OR ALTER PROCEDURE silver.sp_refresh_location_scraper_globe_quality
    @run_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    CREATE TABLE #run_quality (
        run_id                       NVARCHAR(100) NOT NULL,
        source                       NVARCHAR(50)  NOT NULL,
        city                         NVARCHAR(100) NOT NULL,
        normalized_count             INT           NULL,
        with_coords_count            INT           NULL,
        with_phone_count             INT           NULL,
        lusha_email_slots            INT           NULL,
        agencies_total               INT           NULL,
        agencies_with_contacts       INT           NULL,
        enrichment_diagnostics_json  NVARCHAR(MAX) NULL
    );

    IF OBJECT_ID(N'bronze.location_scraper_run_quality', N'U') IS NOT NULL
    BEGIN
        INSERT INTO #run_quality (
            run_id, source, city,
            normalized_count, with_coords_count, with_phone_count,
            lusha_email_slots, agencies_total, agencies_with_contacts,
            enrichment_diagnostics_json
        )
        SELECT
            run_id, source, city,
            normalized_count, with_coords_count, with_phone_count,
            lusha_email_slots, agencies_total, agencies_with_contacts,
            enrichment_diagnostics_json
        FROM bronze.location_scraper_run_quality
        WHERE @run_id IS NULL OR run_id = @run_id;
    END

    ;WITH globe AS (
        SELECT
            g.*,
            CASE WHEN NULLIF(g.external_id, N'') IS NOT NULL THEN 1 ELSE 0 END AS has_external_id,
            CASE WHEN NULLIF(g.listing_url, N'') IS NOT NULL THEN 1 ELSE 0 END AS has_listing_url,
            CASE WHEN g.latitude IS NOT NULL AND g.longitude IS NOT NULL THEN 1 ELSE 0 END AS has_coordinates,
            CASE WHEN NULLIF(g.address, N'') IS NOT NULL THEN 1 ELSE 0 END AS has_address,
            CASE WHEN NULLIF(g.postal_code, N'') IS NOT NULL THEN 1 ELSE 0 END AS has_postal_code,
            CASE WHEN NULLIF(g.district, N'') IS NOT NULL THEN 1 ELSE 0 END AS has_district,
            CASE WHEN g.price_monthly IS NOT NULL THEN 1 ELSE 0 END AS has_price_monthly,
            CASE WHEN g.price_per_m2 IS NOT NULL THEN 1 ELSE 0 END AS has_price_per_m2,
            CASE WHEN g.surface_m2 IS NOT NULL THEN 1 ELSE 0 END AS has_surface_m2,
            CASE WHEN NULLIF(g.contact_name, N'') IS NOT NULL THEN 1 ELSE 0 END AS has_contact_name,
            CASE WHEN NULLIF(g.company_name, N'') IS NOT NULL THEN 1 ELSE 0 END AS has_company_name,
            CASE WHEN NULLIF(g.phone, N'') IS NOT NULL THEN 1 ELSE 0 END AS has_phone,
            (
                CASE WHEN NULLIF(g.lusha_email_1, N'') IS NOT NULL THEN 1 ELSE 0 END
              + CASE WHEN NULLIF(g.lusha_email_2, N'') IS NOT NULL THEN 1 ELSE 0 END
              + CASE WHEN NULLIF(g.lusha_email_3, N'') IS NOT NULL THEN 1 ELSE 0 END
            ) AS lusha_email_count
        FROM silver.location_scraper_globe_v2 AS g
        WHERE @run_id IS NULL OR g.run_id = @run_id
    ),
    grouped AS (
        SELECT
            g.run_id,
            g.source,
            g.run_city,
            MIN(g.inserted_at) AS first_inserted_at,
            MAX(g.inserted_at) AS last_inserted_at,
            COUNT(*) AS silver_row_count,

            SUM(g.has_external_id) AS rows_with_external_id,
            SUM(g.has_listing_url) AS rows_with_listing_url,
            SUM(g.has_coordinates) AS rows_with_coordinates,
            SUM(g.has_address) AS rows_with_address,
            SUM(g.has_postal_code) AS rows_with_postal_code,
            SUM(g.has_district) AS rows_with_district,
            SUM(g.has_price_monthly) AS rows_with_price_monthly,
            SUM(g.has_price_per_m2) AS rows_with_price_per_m2,
            SUM(g.has_surface_m2) AS rows_with_surface_m2,
            SUM(g.has_contact_name) AS rows_with_contact_name,
            SUM(g.has_company_name) AS rows_with_company_name,
            SUM(g.has_phone) AS rows_with_phone,
            SUM(CASE WHEN g.lusha_email_count >= 1 THEN 1 ELSE 0 END) AS rows_with_any_lusha_email,
            SUM(CASE WHEN g.lusha_email_count >= 2 THEN 1 ELSE 0 END) AS rows_with_2plus_lusha_emails,
            SUM(CASE WHEN g.lusha_email_count >= 3 THEN 1 ELSE 0 END) AS rows_with_3_lusha_emails,

            COUNT(DISTINCT NULLIF(g.external_id, N'')) AS distinct_external_ids,
            COUNT(DISTINCT CASE
                WHEN g.latitude IS NOT NULL AND g.longitude IS NOT NULL
                    THEN CONCAT(CONVERT(NVARCHAR(50), ROUND(g.latitude, 4)), N'|', CONVERT(NVARCHAR(50), ROUND(g.longitude, 4)))
                ELSE NULL
            END) AS distinct_geo_points,
            COUNT(DISTINCT NULLIF(g.company_name, N'')) AS distinct_companies,
            COUNT(DISTINCT NULLIF(g.contact_name, N'')) AS distinct_contact_names,
            COUNT(DISTINCT NULLIF(g.phone, N'')) AS distinct_phones,

            MIN(g.price_monthly) AS min_price_monthly,
            CAST(AVG(CAST(g.price_monthly AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS avg_price_monthly,
            MAX(g.price_monthly) AS max_price_monthly,
            MIN(g.price_per_m2) AS min_price_per_m2,
            CAST(AVG(CAST(g.price_per_m2 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS avg_price_per_m2,
            MAX(g.price_per_m2) AS max_price_per_m2,
            MIN(g.surface_m2) AS min_surface_m2,
            CAST(AVG(CAST(g.surface_m2 AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS avg_surface_m2,
            MAX(g.surface_m2) AS max_surface_m2
        FROM globe AS g
        GROUP BY g.run_id, g.source, g.run_city
    ),
    raw_counts AS (
        SELECT
            r.run_id,
            r.source,
            r.city AS run_city,
            COUNT(*) AS raw_item_count
        FROM bronze.location_scraper_raw AS r
        WHERE @run_id IS NULL OR r.run_id = @run_id
        GROUP BY r.run_id, r.source, r.city
    ),
    final AS (
        SELECT
            g.run_id,
            g.source,
            g.run_city,
            g.first_inserted_at,
            g.last_inserted_at,
            ISNULL(r.raw_item_count, 0) AS raw_item_count,
            g.silver_row_count,
            ISNULL(r.raw_item_count, 0) - g.silver_row_count AS raw_to_silver_delta,

            g.rows_with_external_id,
            g.rows_with_listing_url,
            g.rows_with_coordinates,
            g.rows_with_address,
            g.rows_with_postal_code,
            g.rows_with_district,
            g.rows_with_price_monthly,
            g.rows_with_price_per_m2,
            g.rows_with_surface_m2,
            g.rows_with_contact_name,
            g.rows_with_company_name,
            g.rows_with_phone,
            g.rows_with_any_lusha_email,
            g.rows_with_2plus_lusha_emails,
            g.rows_with_3_lusha_emails,

            CAST(100.0 * g.rows_with_external_id / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_external_id,
            CAST(100.0 * g.rows_with_listing_url / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_listing_url,
            CAST(100.0 * g.rows_with_coordinates / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_coordinates,
            CAST(100.0 * g.rows_with_address / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_address,
            CAST(100.0 * g.rows_with_postal_code / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_postal_code,
            CAST(100.0 * g.rows_with_district / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_district,
            CAST(100.0 * g.rows_with_price_monthly / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_price_monthly,
            CAST(100.0 * g.rows_with_price_per_m2 / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_price_per_m2,
            CAST(100.0 * g.rows_with_surface_m2 / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_surface_m2,
            CAST(100.0 * g.rows_with_contact_name / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_contact_name,
            CAST(100.0 * g.rows_with_company_name / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_company_name,
            CAST(100.0 * g.rows_with_phone / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_phone,
            CAST(100.0 * g.rows_with_any_lusha_email / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_any_lusha_email,
            CAST(100.0 * g.rows_with_2plus_lusha_emails / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_2plus_lusha_emails,
            CAST(100.0 * g.rows_with_3_lusha_emails / NULLIF(g.silver_row_count, 0) AS DECIMAL(5,2)) AS pct_3_lusha_emails,

            g.distinct_external_ids,
            g.rows_with_external_id - g.distinct_external_ids AS duplicate_external_id_rows,
            g.distinct_geo_points,
            g.rows_with_coordinates - g.distinct_geo_points AS duplicate_geo_rows,
            g.distinct_companies,
            g.distinct_contact_names,
            g.distinct_phones,

            g.min_price_monthly,
            g.avg_price_monthly,
            g.max_price_monthly,
            g.min_price_per_m2,
            g.avg_price_per_m2,
            g.max_price_per_m2,
            g.min_surface_m2,
            g.avg_surface_m2,
            g.max_surface_m2,

            q.normalized_count AS bronze_normalized_count,
            q.with_coords_count AS bronze_with_coords_count,
            q.with_phone_count AS bronze_with_phone_count,
            q.lusha_email_slots AS bronze_lusha_email_slots,
            q.agencies_total AS bronze_agencies_total,
            q.agencies_with_contacts AS bronze_agencies_with_contacts,
            q.enrichment_diagnostics_json
        FROM grouped AS g
        LEFT JOIN raw_counts AS r
            ON r.run_id = g.run_id
           AND r.source = g.source
           AND r.run_city = g.run_city
        LEFT JOIN #run_quality AS q
            ON q.run_id = g.run_id
           AND q.source = g.source
           AND q.city = g.run_city
    )
    MERGE silver.location_scraper_globe_quality WITH (HOLDLOCK) AS target
    USING final AS src
        ON target.run_id = src.run_id
       AND target.source = src.source
       AND target.run_city = src.run_city
    WHEN MATCHED THEN
        UPDATE SET
            first_inserted_at = src.first_inserted_at,
            last_inserted_at = src.last_inserted_at,
            raw_item_count = src.raw_item_count,
            silver_row_count = src.silver_row_count,
            raw_to_silver_delta = src.raw_to_silver_delta,
            rows_with_external_id = src.rows_with_external_id,
            rows_with_listing_url = src.rows_with_listing_url,
            rows_with_coordinates = src.rows_with_coordinates,
            rows_with_address = src.rows_with_address,
            rows_with_postal_code = src.rows_with_postal_code,
            rows_with_district = src.rows_with_district,
            rows_with_price_monthly = src.rows_with_price_monthly,
            rows_with_price_per_m2 = src.rows_with_price_per_m2,
            rows_with_surface_m2 = src.rows_with_surface_m2,
            rows_with_contact_name = src.rows_with_contact_name,
            rows_with_company_name = src.rows_with_company_name,
            rows_with_phone = src.rows_with_phone,
            rows_with_any_lusha_email = src.rows_with_any_lusha_email,
            rows_with_2plus_lusha_emails = src.rows_with_2plus_lusha_emails,
            rows_with_3_lusha_emails = src.rows_with_3_lusha_emails,
            pct_external_id = src.pct_external_id,
            pct_listing_url = src.pct_listing_url,
            pct_coordinates = src.pct_coordinates,
            pct_address = src.pct_address,
            pct_postal_code = src.pct_postal_code,
            pct_district = src.pct_district,
            pct_price_monthly = src.pct_price_monthly,
            pct_price_per_m2 = src.pct_price_per_m2,
            pct_surface_m2 = src.pct_surface_m2,
            pct_contact_name = src.pct_contact_name,
            pct_company_name = src.pct_company_name,
            pct_phone = src.pct_phone,
            pct_any_lusha_email = src.pct_any_lusha_email,
            pct_2plus_lusha_emails = src.pct_2plus_lusha_emails,
            pct_3_lusha_emails = src.pct_3_lusha_emails,
            distinct_external_ids = src.distinct_external_ids,
            duplicate_external_id_rows = src.duplicate_external_id_rows,
            distinct_geo_points = src.distinct_geo_points,
            duplicate_geo_rows = src.duplicate_geo_rows,
            distinct_companies = src.distinct_companies,
            distinct_contact_names = src.distinct_contact_names,
            distinct_phones = src.distinct_phones,
            min_price_monthly = src.min_price_monthly,
            avg_price_monthly = src.avg_price_monthly,
            max_price_monthly = src.max_price_monthly,
            min_price_per_m2 = src.min_price_per_m2,
            avg_price_per_m2 = src.avg_price_per_m2,
            max_price_per_m2 = src.max_price_per_m2,
            min_surface_m2 = src.min_surface_m2,
            avg_surface_m2 = src.avg_surface_m2,
            max_surface_m2 = src.max_surface_m2,
            bronze_normalized_count = src.bronze_normalized_count,
            bronze_with_coords_count = src.bronze_with_coords_count,
            bronze_with_phone_count = src.bronze_with_phone_count,
            bronze_lusha_email_slots = src.bronze_lusha_email_slots,
            bronze_agencies_total = src.bronze_agencies_total,
            bronze_agencies_with_contacts = src.bronze_agencies_with_contacts,
            enrichment_diagnostics_json = src.enrichment_diagnostics_json,
            refreshed_at = GETUTCDATE()
    WHEN NOT MATCHED THEN
        INSERT (
            run_id, source, run_city,
            first_inserted_at, last_inserted_at,
            raw_item_count, silver_row_count, raw_to_silver_delta,
            rows_with_external_id, rows_with_listing_url, rows_with_coordinates,
            rows_with_address, rows_with_postal_code, rows_with_district,
            rows_with_price_monthly, rows_with_price_per_m2, rows_with_surface_m2,
            rows_with_contact_name, rows_with_company_name, rows_with_phone,
            rows_with_any_lusha_email, rows_with_2plus_lusha_emails, rows_with_3_lusha_emails,
            pct_external_id, pct_listing_url, pct_coordinates,
            pct_address, pct_postal_code, pct_district,
            pct_price_monthly, pct_price_per_m2, pct_surface_m2,
            pct_contact_name, pct_company_name, pct_phone,
            pct_any_lusha_email, pct_2plus_lusha_emails, pct_3_lusha_emails,
            distinct_external_ids, duplicate_external_id_rows,
            distinct_geo_points, duplicate_geo_rows,
            distinct_companies, distinct_contact_names, distinct_phones,
            min_price_monthly, avg_price_monthly, max_price_monthly,
            min_price_per_m2, avg_price_per_m2, max_price_per_m2,
            min_surface_m2, avg_surface_m2, max_surface_m2,
            bronze_normalized_count, bronze_with_coords_count, bronze_with_phone_count,
            bronze_lusha_email_slots, bronze_agencies_total, bronze_agencies_with_contacts,
            enrichment_diagnostics_json
        )
        VALUES (
            src.run_id, src.source, src.run_city,
            src.first_inserted_at, src.last_inserted_at,
            src.raw_item_count, src.silver_row_count, src.raw_to_silver_delta,
            src.rows_with_external_id, src.rows_with_listing_url, src.rows_with_coordinates,
            src.rows_with_address, src.rows_with_postal_code, src.rows_with_district,
            src.rows_with_price_monthly, src.rows_with_price_per_m2, src.rows_with_surface_m2,
            src.rows_with_contact_name, src.rows_with_company_name, src.rows_with_phone,
            src.rows_with_any_lusha_email, src.rows_with_2plus_lusha_emails, src.rows_with_3_lusha_emails,
            src.pct_external_id, src.pct_listing_url, src.pct_coordinates,
            src.pct_address, src.pct_postal_code, src.pct_district,
            src.pct_price_monthly, src.pct_price_per_m2, src.pct_surface_m2,
            src.pct_contact_name, src.pct_company_name, src.pct_phone,
            src.pct_any_lusha_email, src.pct_2plus_lusha_emails, src.pct_3_lusha_emails,
            src.distinct_external_ids, src.duplicate_external_id_rows,
            src.distinct_geo_points, src.duplicate_geo_rows,
            src.distinct_companies, src.distinct_contact_names, src.distinct_phones,
            src.min_price_monthly, src.avg_price_monthly, src.max_price_monthly,
            src.min_price_per_m2, src.avg_price_per_m2, src.max_price_per_m2,
            src.min_surface_m2, src.avg_surface_m2, src.max_surface_m2,
            src.bronze_normalized_count, src.bronze_with_coords_count, src.bronze_with_phone_count,
            src.bronze_lusha_email_slots, src.bronze_agencies_total, src.bronze_agencies_with_contacts,
            src.enrichment_diagnostics_json
        );
END
GO
