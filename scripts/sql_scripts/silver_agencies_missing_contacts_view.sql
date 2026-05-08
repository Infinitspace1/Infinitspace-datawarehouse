-- View: silver.vw_agencies_missing_contacts
-- Ranks real estate agencies that never yielded a Lusha contact,
-- ordered by how many distinct buildings they appear on in the globe table.
-- Useful to prioritise manual outreach or improve enrichment coverage.

CREATE OR ALTER VIEW silver.vw_agencies_missing_contacts AS
WITH agency_globe AS (
    SELECT
        company_name,
        source,
        country_code,
        external_id,
        run_id,
        inserted_at,
        CASE
            WHEN lusha_email_1 IS NOT NULL
              OR lusha_email_2 IS NOT NULL
              OR lusha_email_3 IS NOT NULL
            THEN 1 ELSE 0
        END AS has_lusha_contact
    FROM silver.location_scraper_globe_v2
    WHERE company_name IS NOT NULL
      AND LTRIM(RTRIM(company_name)) <> ''
),
agency_summary AS (
    SELECT
        company_name,
        source,
        country_code,
        COUNT(DISTINCT external_id) AS building_count,
        COUNT(DISTINCT run_id)      AS run_count,   -- how many scrape runs this agency appeared in
        MAX(inserted_at)            AS last_seen_at,
        MAX(has_lusha_contact)      AS ever_had_contact
    FROM agency_globe
    GROUP BY company_name, source, country_code
),
-- Latest failure diagnostic per (agency_name, source)
latest_diag AS (
    SELECT
        agency_name,
        source,
        reason          AS last_failure_reason,
        domain_used,
        domains_found,
        lusha_search_mode,
        company_name_cleaned,
        ROW_NUMBER() OVER (
            PARTITION BY agency_name, source
            ORDER BY created_at DESC
        ) AS rn
    FROM bronze.location_scraper_lusha_diagnostics
    WHERE has_contact = 0
)
SELECT
    a.company_name,
    a.source,
    a.country_code,
    a.building_count,
    a.run_count,
    a.last_seen_at,
    d.last_failure_reason,
    d.domain_used,
    d.domains_found,
    d.lusha_search_mode,
    d.company_name_cleaned,
    RANK() OVER (ORDER BY a.building_count DESC) AS rank_by_buildings
FROM agency_summary a
LEFT JOIN latest_diag d
    ON  a.company_name = d.agency_name
    AND a.source       = d.source
    AND d.rn           = 1
WHERE a.ever_had_contact = 0;
