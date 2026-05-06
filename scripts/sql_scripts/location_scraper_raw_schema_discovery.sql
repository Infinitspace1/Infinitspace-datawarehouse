-- Location Scraper: discover real JSON schema in bronze.location_scraper_raw
-- Goal: profile payload_json keys before building a stable globe view.
--
-- Usage:
--   1) Set filters below (@source / @city / @run_id) as needed
--   2) Run whole script
--   3) Use output #2 ("JSON path profile") to pick view mappings

SET NOCOUNT ON;

DECLARE @source NVARCHAR(50) = NULL;         -- e.g. 'immobilienscout'
DECLARE @city NVARCHAR(100) = NULL;          -- e.g. 'berlin'
DECLARE @run_id NVARCHAR(100) = NULL;        -- exact run_id if you want one run
DECLARE @sample_rows INT = 2000;             -- cap for profiling cost
DECLARE @max_depth INT = 6;                  -- max JSON traversal depth

-- 1) Recent runs overview (quality + raw volume)
SELECT TOP (50)
    q.finished_at,
    q.run_id,
    q.source,
    q.city,
    q.raw_item_count,
    q.normalized_count,
    q.with_coords_count,
    q.with_phone_count,
    q.with_name_or_company_count,
    q.lusha_email_slots
FROM bronze.location_scraper_run_quality AS q
WHERE (@source IS NULL OR q.source = @source)
  AND (@city IS NULL OR q.city = @city)
  AND (@run_id IS NULL OR q.run_id = @run_id)
ORDER BY q.finished_at DESC;

-- 2) JSON path profile
--    Shows every observed path, type consistency, coverage, and sample scalar value.
IF OBJECT_ID('tempdb..#sample_rows') IS NOT NULL DROP TABLE #sample_rows;
IF OBJECT_ID('tempdb..#json_nodes') IS NOT NULL DROP TABLE #json_nodes;

CREATE TABLE #sample_rows (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    payload_json NVARCHAR(MAX) NOT NULL
);

INSERT INTO #sample_rows (id, payload_json)
SELECT TOP (@sample_rows)
    r.id,
    r.payload_json
FROM bronze.location_scraper_raw AS r
WHERE (@source IS NULL OR r.source = @source)
  AND (@city IS NULL OR r.city = @city)
  AND (@run_id IS NULL OR r.run_id = @run_id)
ORDER BY r.inserted_at DESC, r.item_index DESC;

CREATE TABLE #json_nodes (
    id UNIQUEIDENTIFIER NOT NULL,
    json_path NVARCHAR(4000) NOT NULL,
    json_value NVARCHAR(MAX) NULL,
    json_type INT NOT NULL,
    depth INT NOT NULL,
    processed BIT NOT NULL DEFAULT(0)
);

INSERT INTO #json_nodes (id, json_path, json_value, json_type, depth, processed)
SELECT
    s.id,
    N'$',
    s.payload_json,
    5,
    0,
    0
FROM #sample_rows AS s;

WHILE 1 = 1
BEGIN
    INSERT INTO #json_nodes (id, json_path, json_value, json_type, depth, processed)
    SELECT
        n.id,
        LEFT(
            CASE
                WHEN n.json_type = 4
                    THEN n.json_path + N'[' + CAST(j.[key] AS NVARCHAR(100)) + N']'
                ELSE
                    n.json_path + N'.' + CAST(j.[key] AS NVARCHAR(4000))
            END,
            4000
        ) AS json_path,
        CAST(j.[value] AS NVARCHAR(MAX)) AS json_value,
        CAST(j.[type] AS INT) AS json_type,
        n.depth + 1 AS depth,
        0
    FROM #json_nodes AS n
    CROSS APPLY OPENJSON(n.json_value) AS j
    WHERE n.processed = 0
      AND n.json_type IN (4, 5)
      AND n.depth < @max_depth;

    IF @@ROWCOUNT = 0 BREAK;

    UPDATE n
    SET n.processed = 1
    FROM #json_nodes AS n
    WHERE n.processed = 0
      AND n.json_type IN (4, 5)
      AND n.depth < @max_depth;
END;

;WITH path_stats AS (
    SELECT
        n.json_path,
        COUNT(*) AS path_occurrences,
        COUNT(DISTINCT n.id) AS distinct_rows_with_path,
        CAST(
            100.0 * COUNT(DISTINCT n.id)
            / NULLIF((SELECT COUNT(*) FROM #sample_rows), 0)
            AS DECIMAL(5,2)
        ) AS row_coverage_pct,
        MAX(CASE WHEN n.json_type NOT IN (4, 5) THEN LEFT(n.json_value, 200) END) AS sample_scalar_value
    FROM #json_nodes AS n
    WHERE n.depth > 0
    GROUP BY n.json_path
)
SELECT
    ps.json_path,
    STUFF((
        SELECT DISTINCT
            N', ' + CASE n2.json_type
                WHEN 0 THEN N'null'
                WHEN 1 THEN N'string'
                WHEN 2 THEN N'number'
                WHEN 3 THEN N'boolean'
                WHEN 4 THEN N'array'
                WHEN 5 THEN N'object'
                ELSE N'type_' + CAST(n2.json_type AS NVARCHAR(20))
            END
        FROM #json_nodes AS n2
        WHERE n2.json_path = ps.json_path
          AND n2.depth > 0
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, N'') AS observed_types,
    ps.path_occurrences,
    ps.distinct_rows_with_path,
    ps.row_coverage_pct,
    ps.sample_scalar_value
FROM path_stats AS ps
ORDER BY ps.distinct_rows_with_path DESC, ps.json_path;

DROP TABLE #json_nodes;
DROP TABLE #sample_rows;

-- 3) Candidate globe-field coverage (quick health check)
--    These paths come from current adapters and typical Apify outputs.
WITH sample_rows AS (
    SELECT TOP (@sample_rows)
        r.id,
        r.payload_json,
        r.source
    FROM bronze.location_scraper_raw AS r
    WHERE (@source IS NULL OR r.source = @source)
      AND (@city IS NULL OR r.city = @city)
      AND (@run_id IS NULL OR r.run_id = @run_id)
    ORDER BY r.inserted_at DESC, r.item_index DESC
),
candidates AS (
    SELECT 'external_id' AS field_name, '$.adid' AS json_path UNION ALL
    SELECT 'external_id', '$.id' UNION ALL
    SELECT 'listing_url', '$.detailWebLink' UNION ALL
    SELECT 'listing_url', '$.propertyUrl' UNION ALL
    SELECT 'listing_url', '$.normalized.url' UNION ALL
    SELECT 'latitude', '$.ubication.latitude' UNION ALL
    SELECT 'latitude', '$.latitude' UNION ALL
    SELECT 'latitude', '$.normalized.address.latitude' UNION ALL
    SELECT 'longitude', '$.ubication.longitude' UNION ALL
    SELECT 'longitude', '$.longitude' UNION ALL
    SELECT 'longitude', '$.normalized.address.longitude' UNION ALL
    SELECT 'address', '$.ubication.title' UNION ALL
    SELECT 'address', '$.street' UNION ALL
    SELECT 'address', '$.normalized.address.formatted' UNION ALL
    SELECT 'district', '$.basicInfo.district' UNION ALL
    SELECT 'district', '$.district' UNION ALL
    SELECT 'district', '$.geo_quarter' UNION ALL
    SELECT 'price_monthly', '$.basicInfo.price' UNION ALL
    SELECT 'price_monthly', '$.price' UNION ALL
    SELECT 'price_monthly', '$.normalized.price.amount' UNION ALL
    SELECT 'surface_m2', '$.basicInfo.size' UNION ALL
    SELECT 'surface_m2', '$.area' UNION ALL
    SELECT 'surface_m2', '$.normalized.area.livingSpace' UNION ALL
    SELECT 'contact_name', '$.contactInfo.commercialName' UNION ALL
    SELECT 'contact_name', '$.sellerName' UNION ALL
    SELECT 'contact_name', '$.normalized.contact.name' UNION ALL
    SELECT 'company_name', '$.agencyName' UNION ALL
    SELECT 'company_name', '$.normalized.contact.company' UNION ALL
    SELECT 'phone', '$.contactInfo.phone1.phoneNumberForMobileDialing' UNION ALL
    SELECT 'phone', '$.sellerPhone' UNION ALL
    SELECT 'phone', '$.normalized.contact.phone'
)
SELECT
    c.field_name,
    c.json_path,
    COUNT(*) AS rows_checked,
    SUM(CASE WHEN NULLIF(JSON_VALUE(s.payload_json, c.json_path), '') IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_value,
    CAST(
        100.0 * SUM(CASE WHEN NULLIF(JSON_VALUE(s.payload_json, c.json_path), '') IS NOT NULL THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,2)
    ) AS fill_rate_pct
FROM sample_rows AS s
CROSS JOIN candidates AS c
GROUP BY c.field_name, c.json_path
ORDER BY c.field_name, fill_rate_pct DESC, c.json_path;
