-- =============================================================================
-- core_finance_dashboard_schema.sql
--
-- Finance dashboard production model — Nexudus-only.
--
-- Design:
--   - meta.finance_dashboard_location_settings
--       Manual per-location finance email settings.
--   - gold.finance_dashboard_user_access
--       Materialized user/location access table.
--   - gold.finance_dashboard_invoice_worklist
--       Materialized invoice worklist table for the website (Nexudus-primary).
--   - gold.sp_refresh_finance_dashboard
--       Rebuilds both gold tables from silver/meta data.
--
-- Why gold tables instead of views:
--   The website should read indexed production tables, not recompute the
--   invoice logic live on every request.
--
-- Workflow type classification:
--   An invoice is "recurrent" if any of its line items in
--   silver.nexudus_coworker_invoice_lines has a financial_account_name
--   containing "Membership Fees". Everything else is "one_off".
--
-- Amsterdam exception:
--   BambooHR location "beyond Republica Campus" grants access to Republica,
--   Herengracht, and Zuidtoren.
-- =============================================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'gold'
)
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA gold';
END
GO

-- ── Location settings (replaces tenant settings keyed on xero_tenant_id) ────

IF OBJECT_ID('meta.finance_dashboard_location_settings', 'U') IS NULL
BEGIN
    CREATE TABLE meta.finance_dashboard_location_settings (
        location_source_id       BIGINT              NOT NULL PRIMARY KEY,
        location_name            NVARCHAR(512)       NULL,
        finance_location_email   NVARCHAR(512)       NULL,
        notes                    NVARCHAR(1024)      NULL,
        created_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        updated_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

-- ── User access table ───────────────────────────────────────────────────────

DROP TABLE IF EXISTS gold.finance_dashboard_user_access;
GO

CREATE TABLE gold.finance_dashboard_user_access (
    employee_source_id          INT                 NOT NULL,
    first_name                  NVARCHAR(100)       NULL,
    last_name                   NVARCHAR(100)       NULL,
    employee_name               NVARCHAR(201)       NULL,
    work_email                  NVARCHAR(255)       NULL,
    job_title                   NVARCHAR(200)       NULL,
    division                    NVARCHAR(200)       NULL,
    bamboohr_location           NVARCHAR(200)       NULL,
    access_rule                 NVARCHAR(64)        NOT NULL,
    location_source_id          BIGINT              NULL,
    location_nexudus_uuid       NVARCHAR(64)        NULL,
    location_name               NVARCHAR(512)       NULL,
    location_city               NVARCHAR(255)       NULL,
    location_country_name       NVARCHAR(128)       NULL,
    location_email              NVARCHAR(512)       NULL
);
GO

CREATE INDEX ix_gold_finance_dashboard_user_access_work_email
    ON gold.finance_dashboard_user_access (work_email);
GO

CREATE INDEX ix_gold_finance_dashboard_user_access_location
    ON gold.finance_dashboard_user_access (location_source_id);
GO

-- ── Invoice worklist table (Nexudus-primary) ────────────────────────────────

DROP TABLE IF EXISTS gold.finance_dashboard_invoice_worklist;
GO

CREATE TABLE gold.finance_dashboard_invoice_worklist (
    nexudus_invoice_source_id   BIGINT              NOT NULL PRIMARY KEY,
    location_source_id          BIGINT              NULL,
    location_name               NVARCHAR(512)       NULL,
    location_city               NVARCHAR(255)       NULL,
    location_country_name       NVARCHAR(128)       NULL,
    location_finance_email      NVARCHAR(512)       NULL,
    invoice_number              NVARCHAR(128)       NULL,
    payment_reference           NVARCHAR(128)       NULL,
    coworker_id                 BIGINT              NULL,
    coworker_name               NVARCHAR(512)       NULL,
    coworker_billing_email      NVARCHAR(512)       NULL,
    company_display_name        NVARCHAR(512)       NULL,
    company_email               NVARCHAR(512)       NULL,
    currency_code               NVARCHAR(8)         NULL,
    invoice_date                DATE                NULL,
    due_date                    DATE                NULL,
    as_of_date_utc              DATE                NOT NULL,
    days_until_due              INT                 NULL,
    days_overdue                INT                 NULL,
    due_state                   NVARCHAR(32)        NOT NULL,
    total_amount                DECIMAL(12,2)       NULL,
    due_amount                  DECIMAL(12,2)       NULL,
    paid_amount                 DECIMAL(12,2)       NULL,
    workflow_type               NVARCHAR(32)        NOT NULL,
    pdf_blob_path               NVARCHAR(1024)      NULL,
    last_synced_at              DATETIME2           NULL
);
GO

CREATE INDEX ix_gold_finance_dashboard_invoice_worklist_due_date
    ON gold.finance_dashboard_invoice_worklist (due_date);
GO

CREATE INDEX ix_gold_finance_dashboard_invoice_worklist_location_due
    ON gold.finance_dashboard_invoice_worklist (location_source_id, due_date);
GO

CREATE INDEX ix_gold_finance_dashboard_invoice_worklist_workflow_due
    ON gold.finance_dashboard_invoice_worklist (workflow_type, due_date);
GO

-- ── Stored procedure ────────────────────────────────────────────────────────

CREATE OR ALTER PROCEDURE gold.sp_refresh_finance_dashboard
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Seed / update location settings from silver.nexudus_locations
    MERGE meta.finance_dashboard_location_settings AS target
    USING (
        SELECT DISTINCT
            source_id AS location_source_id,
            name AS location_name
        FROM silver.nexudus_locations
    ) AS source
        ON target.location_source_id = source.location_source_id
    WHEN MATCHED THEN UPDATE SET
        location_name = source.location_name,
        updated_at = CASE
            WHEN ISNULL(target.location_name, N'') <> ISNULL(source.location_name, N'')
                THEN GETUTCDATE()
            ELSE target.updated_at
        END
    WHEN NOT MATCHED THEN INSERT (
        location_source_id,
        location_name
    ) VALUES (
        source.location_source_id,
        source.location_name
    );

    -- Seed known finance emails by location_source_id
    MERGE meta.finance_dashboard_location_settings AS target
    USING (
        SELECT *
        FROM (VALUES
            (CAST(1376491118 AS BIGINT), N'finance.aldgatetower@infinitspace.com'),
            (CAST(1420976475 AS BIGINT), N'finance.c29@infinitspace.com'),
            (CAST(1420962233 AS BIGINT), N'finance.qh@infinitspace.com'),
            (CAST(1415499547 AS BIGINT), N'finance.thebower@infinitspace.com'),
            (CAST(1414964752 AS BIGINT), N'finance.kingsbournehouse@infinitspace.com'),
            (CAST(1420976575 AS BIGINT), N'finance.foxcourt@infinitspace.com'),
            (CAST(1415079491 AS BIGINT), N'finance.republica@infinitspace.com'),
            (CAST(1420951935 AS BIGINT), N'finance.goudenbocht@infinitspace.com'),
            (CAST(1414964753 AS BIGINT), N'finance.zuidtoren@infinitspace.com')
        ) AS seed(location_source_id, finance_location_email)
    ) AS source
        ON target.location_source_id = source.location_source_id
    WHEN MATCHED THEN UPDATE SET
        finance_location_email = source.finance_location_email,
        updated_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        location_source_id,
        finance_location_email
    ) VALUES (
        source.location_source_id,
        source.finance_location_email
    );

    -- Step 2: Rebuild user access
    TRUNCATE TABLE gold.finance_dashboard_user_access;

    WITH cm_roster AS (
        SELECT
            source_id AS employee_source_id,
            first_name,
            last_name,
            LTRIM(RTRIM(CONCAT(COALESCE(first_name, N''), N' ', COALESCE(last_name, N'')))) AS employee_name,
            work_email,
            job_title,
            division,
            location AS bamboohr_location,
            LTRIM(RTRIM(
                REPLACE(
                    REPLACE(COALESCE(location, N''), NCHAR(223), N'ss'),
                    NCHAR(7838), N'SS'
                )
            )) AS bamboohr_location_normalized
        FROM silver.bamboohr_employees
        WHERE job_title IN (N'Community Manager', N'Assistant Community Manager')
    ),
    location_access_map AS (
        SELECT *
        FROM (VALUES
            (N'beyond Aldgate Tower',         CAST(1376491118 AS BIGINT), N'exact_location'),
            (N'beyond Chausseestrasse',       CAST(1420976475 AS BIGINT), N'exact_location'),
            (N'beyond Quartier Heidestrasse', CAST(1420962233 AS BIGINT), N'exact_location'),
            (N'beyond The Bower',             CAST(1415499547 AS BIGINT), N'exact_location'),
            (N'beyond Kingsbourne House',     CAST(1414964752 AS BIGINT), N'exact_location'),
            (N'beyond Fox Court',             CAST(1420976575 AS BIGINT), N'exact_location'),
            (N'beyond Republica Campus',      CAST(1415079491 AS BIGINT), N'exact_location'),
            (N'beyond Herengracht',           CAST(1420951935 AS BIGINT), N'exact_location'),
            (N'beyond Zuidtoren',             CAST(1414964753 AS BIGINT), N'exact_location'),
            (N'beyond Republica Campus',      CAST(1420951935 AS BIGINT), N'amsterdam_shared_access'),
            (N'beyond Republica Campus',      CAST(1414964753 AS BIGINT), N'amsterdam_shared_access')
        ) AS map(bamboohr_location_normalized, location_source_id, access_rule)
    )
    INSERT INTO gold.finance_dashboard_user_access (
        employee_source_id,
        first_name,
        last_name,
        employee_name,
        work_email,
        job_title,
        division,
        bamboohr_location,
        access_rule,
        location_source_id,
        location_nexudus_uuid,
        location_name,
        location_city,
        location_country_name,
        location_email
    )
    SELECT DISTINCT
        cm.employee_source_id,
        cm.first_name,
        cm.last_name,
        cm.employee_name,
        cm.work_email,
        cm.job_title,
        cm.division,
        cm.bamboohr_location,
        map.access_rule,
        loc.source_id AS location_source_id,
        loc.nexudus_uuid AS location_nexudus_uuid,
        loc.name AS location_name,
        loc.city AS location_city,
        loc.country_name AS location_country_name,
        loc.email AS location_email
    FROM cm_roster cm
    INNER JOIN location_access_map map
        ON map.bamboohr_location_normalized = cm.bamboohr_location_normalized
    LEFT JOIN silver.nexudus_locations loc
        ON loc.source_id = map.location_source_id;

    -- Step 3: Rebuild invoice worklist (Nexudus-only)
    TRUNCATE TABLE gold.finance_dashboard_invoice_worklist;

    WITH unpaid_nexudus_invoices AS (
        SELECT
            nci.source_id,
            nci.invoice_number,
            nci.payment_reference,
            nci.coworker_id,
            nci.coworker_name,
            nci.coworker_billing_email,
            nci.coworker_company_name,
            nci.bill_to_name,
            nci.location_source_id,
            nci.location_name,
            nci.currency_code,
            nci.invoice_from_date,
            nci.due_date,
            nci.total_amount,
            nci.due_amount,
            nci.paid_amount,
            nci.pdf_blob_path,
            nci.last_synced_at
        FROM silver.nexudus_coworker_invoices nci
        WHERE nci.due_amount > 0
          AND nci.void = 0
          AND nci.draft = 0
          AND nci.paid = 0
          AND nci.credit_note = 0
          AND nci.due_date >= '2026-03-01'
    ),
    invoice_account_flags AS (
        SELECT
            ncil.invoice_source_id,
            MAX(
                CASE
                    WHEN UPPER(ISNULL(ncil.financial_account_name, N'')) LIKE N'%MEMBERSHIP FEES%'
                        THEN 1
                    ELSE 0
                END
            ) AS has_recurrent_account
        FROM silver.nexudus_coworker_invoice_lines ncil
        INNER JOIN unpaid_nexudus_invoices inv
            ON inv.source_id = ncil.invoice_source_id
        GROUP BY ncil.invoice_source_id
    )
    INSERT INTO gold.finance_dashboard_invoice_worklist (
        nexudus_invoice_source_id,
        location_source_id,
        location_name,
        location_city,
        location_country_name,
        location_finance_email,
        invoice_number,
        payment_reference,
        coworker_id,
        coworker_name,
        coworker_billing_email,
        company_display_name,
        company_email,
        currency_code,
        invoice_date,
        due_date,
        as_of_date_utc,
        days_until_due,
        days_overdue,
        due_state,
        total_amount,
        due_amount,
        paid_amount,
        workflow_type,
        pdf_blob_path,
        last_synced_at
    )
    SELECT
        inv.source_id AS nexudus_invoice_source_id,
        inv.location_source_id,
        COALESCE(loc.name, inv.location_name) AS location_name,
        loc.city AS location_city,
        loc.country_name AS location_country_name,
        ls.finance_location_email AS location_finance_email,
        inv.invoice_number,
        inv.payment_reference,
        inv.coworker_id,
        COALESCE(nc.full_name, inv.coworker_name) AS coworker_name,
        COALESCE(nc.billing_email, nc.email, inv.coworker_billing_email) AS coworker_billing_email,
        COALESCE(
            nc.billing_name,
            nc.company_name,
            inv.coworker_company_name,
            inv.bill_to_name
        ) AS company_display_name,
        COALESCE(
            nc.billing_email,
            nc.email,
            inv.coworker_billing_email
        ) AS company_email,
        inv.currency_code,
        CAST(inv.invoice_from_date AS DATE) AS invoice_date,
        CAST(inv.due_date AS DATE) AS due_date,
        CAST(GETUTCDATE() AS DATE) AS as_of_date_utc,
        DATEDIFF(DAY, CAST(GETUTCDATE() AS DATE), CAST(inv.due_date AS DATE)) AS days_until_due,
        CASE
            WHEN CAST(inv.due_date AS DATE) < CAST(GETUTCDATE() AS DATE)
                THEN DATEDIFF(DAY, CAST(inv.due_date AS DATE), CAST(GETUTCDATE() AS DATE))
            ELSE 0
        END AS days_overdue,
        CASE
            WHEN CAST(inv.due_date AS DATE) < CAST(GETUTCDATE() AS DATE) THEN N'overdue'
            WHEN CAST(inv.due_date AS DATE) = CAST(GETUTCDATE() AS DATE) THEN N'due_today'
            ELSE N'upcoming'
        END AS due_state,
        inv.total_amount,
        inv.due_amount,
        inv.paid_amount,
        CASE
            WHEN ISNULL(af.has_recurrent_account, 0) = 1 THEN N'recurrent'
            ELSE N'one_off'
        END AS workflow_type,
        inv.pdf_blob_path,
        inv.last_synced_at
    FROM unpaid_nexudus_invoices inv
    LEFT JOIN silver.nexudus_locations loc
        ON loc.source_id = inv.location_source_id
    LEFT JOIN silver.nexudus_coworkers nc
        ON nc.source_id = inv.coworker_id
    LEFT JOIN meta.finance_dashboard_location_settings ls
        ON ls.location_source_id = inv.location_source_id
    LEFT JOIN invoice_account_flags af
        ON af.invoice_source_id = inv.source_id;
END
GO

EXEC gold.sp_refresh_finance_dashboard;
GO

-- Example queries
SELECT TOP 20 *
FROM gold.finance_dashboard_invoice_worklist
ORDER BY due_date ASC, invoice_number ASC;

SELECT *
FROM gold.finance_dashboard_user_access
ORDER BY location_name, job_title, employee_name;


select * from bronze.nexudus_coworker_invoices