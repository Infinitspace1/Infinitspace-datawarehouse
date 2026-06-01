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
--   - gold.finance_dashboard_revenue_occupancy
--       Materialized location-level contracted MRR and physical occupancy.
--   - gold.vw_finance_dashboard_membership_schedule
--       Contract-level membership schedule for reporting/export.
--   - gold.sp_refresh_finance_dashboard
--       Rebuilds the materialized gold tables from silver/meta data.
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
    invoice_status              NVARCHAR(64)        NULL,
    processing                  BIT                 NOT NULL DEFAULT 0,
    payment_failure_count       INT                 NULL,
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

IF OBJECT_ID('silver.nexudus_coworker_invoices', 'U') IS NOT NULL
   AND COL_LENGTH('silver.nexudus_coworker_invoices', 'invoice_status') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworker_invoices
    ADD invoice_status NVARCHAR(64) NULL;
END
GO

IF OBJECT_ID('silver.nexudus_coworker_invoices', 'U') IS NOT NULL
   AND COL_LENGTH('silver.nexudus_coworker_invoices', 'processing') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworker_invoices
    ADD processing BIT NULL;
END
GO

IF OBJECT_ID('silver.nexudus_coworker_invoices', 'U') IS NOT NULL
   AND COL_LENGTH('silver.nexudus_coworker_invoices', 'payment_failure_count') IS NULL
BEGIN
    ALTER TABLE silver.nexudus_coworker_invoices
    ADD payment_failure_count INT NULL;
END
GO

-- Revenue and occupancy snapshot

DROP TABLE IF EXISTS gold.finance_dashboard_revenue_occupancy;
GO

CREATE TABLE gold.finance_dashboard_revenue_occupancy (
    as_of_date_utc              DATE                NOT NULL,
    location_source_id          BIGINT              NOT NULL,
    location_name               NVARCHAR(512)       NULL,
    location_city               NVARCHAR(255)       NULL,
    location_country_name       NVARCHAR(128)       NULL,
    currency_code               NVARCHAR(8)         NULL,
    active_contract_count       INT                 NOT NULL,
    active_member_count         INT                 NOT NULL,
    occupied_workstations       INT                 NOT NULL,
    total_workstation_capacity  INT                 NOT NULL,
    vacant_workstations         INT                 NOT NULL,
    occupancy_pct               DECIMAL(9,4)        NULL,
    contracted_monthly_revenue  DECIMAL(18,2)       NOT NULL,
    monthly_revenue_per_occupied_workstation DECIMAL(18,2) NULL,
    last_refreshed_at           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_gold_finance_dashboard_revenue_occupancy
        PRIMARY KEY (as_of_date_utc, location_source_id)
);
GO

CREATE INDEX ix_gold_finance_dashboard_revenue_occupancy_location
    ON gold.finance_dashboard_revenue_occupancy (location_source_id);
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
        WHERE is_deleted = 0
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
    DELETE FROM gold.finance_dashboard_user_access;

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
          AND is_deleted = 0
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
        ON loc.source_id = map.location_source_id
       AND loc.is_deleted = 0;

    -- Step 3: Rebuild invoice worklist (Nexudus-only)
    DELETE FROM gold.finance_dashboard_invoice_worklist;

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
            nci.invoice_status,
            ISNULL(nci.processing, 0) AS processing,
            nci.payment_failure_count,
            nci.invoice_from_date,
            nci.due_date,
            CAST((nci.due_date AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time') AS DATE) AS due_date_local,
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
          -- Nexudus's CreditNote flag is unreliable — it gets set on normal
          -- invoices that have received credits from a prior invoice (e.g.
          -- ADP INV-2026.05-0645). Real credit notes are paid/zero-balance
          -- so the due_amount > 0 AND paid = 0 gates above already exclude
          -- them.
          AND nci.is_deleted = 0
          AND ISNULL(nci.processing, 0) = 0
          AND UPPER(ISNULL(nci.invoice_status, N'')) NOT LIKE N'%PROCESSING%'
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
        WHERE ncil.is_deleted = 0
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
        invoice_status,
        processing,
        payment_failure_count,
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
        inv.invoice_status,
        inv.processing,
        inv.payment_failure_count,
        CAST(inv.invoice_from_date AS DATE) AS invoice_date,
        inv.due_date_local AS due_date,
        CAST(GETUTCDATE() AS DATE) AS as_of_date_utc,
        DATEDIFF(DAY, CAST(GETUTCDATE() AS DATE), inv.due_date_local) AS days_until_due,
        CASE
            WHEN inv.due_date_local < CAST(GETUTCDATE() AS DATE)
                THEN DATEDIFF(DAY, inv.due_date_local, CAST(GETUTCDATE() AS DATE))
            ELSE 0
        END AS days_overdue,
        CASE
            WHEN inv.due_date_local < CAST(GETUTCDATE() AS DATE) THEN N'overdue'
            WHEN inv.due_date_local = CAST(GETUTCDATE() AS DATE) THEN N'due_today'
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
       AND loc.is_deleted = 0
    LEFT JOIN silver.nexudus_coworkers nc
        ON nc.source_id = inv.coworker_id
       AND nc.is_deleted = 0
    LEFT JOIN meta.finance_dashboard_location_settings ls
        ON ls.location_source_id = inv.location_source_id
    LEFT JOIN invoice_account_flags af
        ON af.invoice_source_id = inv.source_id;

    -- Step 4: Rebuild contracted revenue and occupancy snapshot.
    DELETE FROM gold.finance_dashboard_revenue_occupancy
    WHERE as_of_date_utc = CAST(GETUTCDATE() AS DATE);

    WITH physical_products AS (
        SELECT
            p.location_source_id,
            SUM(
                CASE
                    WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                    WHEN p.item_type IN (2, 3) THEN 1
                    ELSE 0
                END
            ) AS total_workstation_capacity
        FROM silver.nexudus_products p
        INNER JOIN silver.nexudus_locations loc
            ON loc.source_id = p.location_source_id
           AND loc.is_deleted = 0
        WHERE p.item_type IN (1, 2, 3)
          AND p.is_available = 1
          AND p.is_deleted = 0
          -- TODO: remove once floor 2 refurbishment is complete and products are re-enabled in Nexudus
          AND NOT (loc.name = 'Amsterdam - Hoofddorp - Taurusavenue 3' AND p.name LIKE '2-%')
        GROUP BY p.location_source_id
    ),
    active_contracts AS (
        SELECT
            c.source_id,
            c.coworker_id,
            c.location_source_id,
            c.currency_code,
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS monthly_fee
        FROM silver.nexudus_contracts c
        WHERE c.active = 1
          AND c.in_paused_period = 0
          AND c.is_deleted = 0
          AND (c.start_date IS NULL OR CAST(c.start_date AS DATE) <= CAST(GETUTCDATE() AS DATE))
          -- Per AVA end-date semantics: cancellation_date is the hard end when set.
          -- contract_term alone (no cancellation_date) = rolling month-to-month → always include.
          -- cancelled = 1 with a future cancellation_date = notice period → still occupied, include.
          AND (c.cancellation_date IS NULL OR CAST(c.cancellation_date AS DATE) >= CAST(GETUTCDATE() AS DATE))
          AND (
              c.cancelled = 0
              OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL
                  AND CAST(c.cancellation_date AS DATE) >= CAST(GETUTCDATE() AS DATE))
          )
    ),
    contract_product_capacity AS (
        SELECT
            ac.source_id AS contract_source_id,
            SUM(
                CASE
                    WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                    WHEN p.item_type IN (2, 3) THEN 1
                    ELSE 0
                END
            ) AS workstation_capacity
        FROM active_contracts ac
        INNER JOIN silver.nexudus_contracts c
            ON c.source_id = ac.source_id
        CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
        INNER JOIN silver.nexudus_products p
            ON p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
           AND p.is_deleted = 0
        WHERE TRIM(s.value) <> N''
        GROUP BY ac.source_id
    ),
    contract_facts AS (
        SELECT
            ac.source_id,
            ac.coworker_id,
            ac.location_source_id,
            ac.currency_code,
            ac.monthly_fee,
            ISNULL(cpc.workstation_capacity, 0) AS workstation_capacity
        FROM active_contracts ac
        LEFT JOIN contract_product_capacity cpc
            ON cpc.contract_source_id = ac.source_id
    ),
    location_facts AS (
        SELECT
            cf.location_source_id,
            MAX(cf.currency_code) AS currency_code,
            COUNT(1) AS active_contract_count,
            COUNT(DISTINCT cf.coworker_id) AS active_member_count,
            SUM(cf.workstation_capacity) AS occupied_workstations,
            SUM(cf.monthly_fee) AS contracted_monthly_revenue
        FROM contract_facts cf
        GROUP BY cf.location_source_id
    )
    INSERT INTO gold.finance_dashboard_revenue_occupancy (
        as_of_date_utc,
        location_source_id,
        location_name,
        location_city,
        location_country_name,
        currency_code,
        active_contract_count,
        active_member_count,
        occupied_workstations,
        total_workstation_capacity,
        vacant_workstations,
        occupancy_pct,
        contracted_monthly_revenue,
        monthly_revenue_per_occupied_workstation,
        last_refreshed_at
    )
    SELECT
        CAST(GETUTCDATE() AS DATE) AS as_of_date_utc,
        loc.source_id AS location_source_id,
        loc.name AS location_name,
        loc.city AS location_city,
        loc.country_name AS location_country_name,
        lf.currency_code,
        ISNULL(lf.active_contract_count, 0) AS active_contract_count,
        ISNULL(lf.active_member_count, 0) AS active_member_count,
        ISNULL(lf.occupied_workstations, 0) AS occupied_workstations,
        ISNULL(pp.total_workstation_capacity, 0) AS total_workstation_capacity,
        CASE
            WHEN ISNULL(pp.total_workstation_capacity, 0) - ISNULL(lf.occupied_workstations, 0) < 0
                THEN 0
            ELSE ISNULL(pp.total_workstation_capacity, 0) - ISNULL(lf.occupied_workstations, 0)
        END AS vacant_workstations,
        CAST(
            100.0 * ISNULL(lf.occupied_workstations, 0)
            / NULLIF(ISNULL(pp.total_workstation_capacity, 0), 0)
            AS DECIMAL(9,4)
        ) AS occupancy_pct,
        ISNULL(lf.contracted_monthly_revenue, 0) AS contracted_monthly_revenue,
        CAST(
            ISNULL(lf.contracted_monthly_revenue, 0)
            / NULLIF(ISNULL(lf.occupied_workstations, 0), 0)
            AS DECIMAL(18,2)
        ) AS monthly_revenue_per_occupied_workstation,
        GETUTCDATE() AS last_refreshed_at
    FROM silver.nexudus_locations loc
    LEFT JOIN location_facts lf
        ON lf.location_source_id = loc.source_id
    LEFT JOIN physical_products pp
        ON pp.location_source_id = loc.source_id
    WHERE loc.is_deleted = 0;
END
GO

CREATE OR ALTER VIEW gold.vw_finance_dashboard_membership_schedule
AS
WITH contract_product_capacity AS (
    SELECT
        c.source_id AS contract_source_id,
        SUM(
            CASE
                WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                WHEN p.item_type IN (2, 3) THEN 1
                ELSE 0
            END
        ) AS workstation_capacity
    FROM silver.nexudus_contracts c
    CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
    INNER JOIN silver.nexudus_products p
        ON p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
       AND p.is_deleted = 0
    WHERE TRIM(s.value) <> N''
      AND c.is_deleted = 0
    GROUP BY c.source_id
),
-- AVA end-date semantics:
--   1. cancellation_date set            → use cancellation_date (explicit hard end)
--   2. contract_term in the future      → use contract_term (real fixed end)
--   3. contract_term in the past,
--      cancellation_date NULL           → NULL (rolled into month-to-month)
contract_eff_end AS (
    SELECT
        c.source_id,
        CAST(
            CASE
                WHEN c.cancellation_date IS NOT NULL
                    THEN c.cancellation_date
                WHEN c.contract_term IS NOT NULL
                     AND CAST(c.contract_term AS DATE) >= CAST(GETUTCDATE() AS DATE)
                    THEN c.contract_term
                ELSE NULL
            END
        AS DATE) AS eff_end_date
    FROM silver.nexudus_contracts c
    WHERE c.is_deleted = 0
),
membership AS (
    SELECT
        c.source_id AS contract_source_id,
        c.coworker_id,
        c.coworker_name,
        COALESCE(NULLIF(c.coworker_company, N''), c.coworker_billing_name, c.coworker_name) AS member_company_name,
        c.coworker_email,
        c.location_source_id,
        COALESCE(loc.name, c.location_name) AS location_name,
        loc.city AS location_city,
        loc.country_name AS location_country_name,
        c.tariff_id,
        c.tariff_name,
        c.next_tariff_id,
        c.next_tariff_name,
        c.floor_plan_desk_ids,
        c.floor_plan_desk_names,
        ISNULL(cpc.workstation_capacity, 0) AS capacity,
        c.currency_code,
        COALESCE(c.price_with_products, c.price, c.tariff_price, 0) AS latest_monthly_fee,
        CAST(
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0)
            / NULLIF(ISNULL(cpc.workstation_capacity, 0), 0)
            AS DECIMAL(18,2)
        ) AS latest_monthly_fee_per_workstation,
        c.start_date,
        cee.eff_end_date AS end_date,
        CASE
            WHEN c.start_date IS NULL THEN NULL
            WHEN cee.eff_end_date IS NULL
                THEN DATEDIFF(MONTH, CAST(c.start_date AS DATE), CAST(GETUTCDATE() AS DATE))
            ELSE DATEDIFF(MONTH, CAST(c.start_date AS DATE), cee.eff_end_date)
        END AS term_months,
        CAST(CEILING(ISNULL(c.cancellation_limit_days, 0) / 30.0) AS INT) AS notice_period_months,
        CAST(
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0)
            * CASE
                WHEN c.start_date IS NULL THEN 0
                WHEN cee.eff_end_date IS NULL
                    THEN CAST(CEILING(ISNULL(c.cancellation_limit_days, 0) / 30.0) AS INT)
                ELSE DATEDIFF(MONTH, CAST(c.start_date AS DATE), cee.eff_end_date)
              END
            AS DECIMAL(18,2)
        ) AS contract_value,
        CAST(
            COALESCE(c.price_with_products, c.price, c.tariff_price, 0)
            * CASE
                WHEN cee.eff_end_date IS NULL
                    THEN CAST(CEILING(ISNULL(c.cancellation_limit_days, 0) / 30.0) AS INT)
                WHEN cee.eff_end_date < CAST(GETUTCDATE() AS DATE)
                    THEN 0
                ELSE DATEDIFF(MONTH, CAST(GETUTCDATE() AS DATE), cee.eff_end_date)
              END
            AS DECIMAL(18,2)
        ) AS remaining_contract_value,
        c.active,
        c.cancelled,
        c.in_paused_period,
        c.coworker_active,
        c.created_on,
        c.updated_on,
        c.last_synced_at
    FROM silver.nexudus_contracts c
    LEFT JOIN contract_product_capacity cpc
        ON cpc.contract_source_id = c.source_id
    LEFT JOIN contract_eff_end cee
        ON cee.source_id = c.source_id
    LEFT JOIN silver.nexudus_locations loc
        ON loc.source_id = c.location_source_id
       AND loc.is_deleted = 0
    WHERE c.is_deleted = 0
)
SELECT *
FROM membership;
GO

EXEC gold.sp_refresh_finance_dashboard;
GO


-- SELECT 
-- location_name,occupied_workstations, 
-- total_workstation_capacity, 
-- occupancy_pct 
-- FROM gold.finance_dashboard_revenue_occupancy

-- -- SELECT *
-- -- FROM gold.vw_finance_dashboard_membership_schedule
-- -- WHERE location_name = 'Amsterdam - Hoofddorp - Taurusavenue 3'
-- --   AND capacity != 0
-- --   -- contract started on or before the last day of this month
-- --   AND CAST(start_date AS DATE) <= EOMONTH(GETUTCDATE())
-- --   -- and either ongoing (end_date IS NULL) or ends on/after the first day of this month
-- --   AND (
-- --       end_date IS NULL
-- --       OR CAST(end_date AS DATE) >= DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)
-- --   )


-- SELECT
--     member_company_name,
--     SUM(capacity)                                           AS total_capacity,
--     SUM(latest_monthly_fee)                                 AS total_monthly_fee,
--     STRING_AGG(floor_plan_desk_names, ', ')
--         WITHIN GROUP (ORDER BY contract_source_id)          AS all_desk_names
-- FROM gold.vw_finance_dashboard_membership_schedule
-- WHERE location_name = 'Amsterdam - Noord - Papaverhof 59'
-- AND capacity != 0
-- AND CAST(start_date AS DATE) <= EOMONTH(GETUTCDATE())
-- AND (
--     end_date IS NULL
--     OR CAST(end_date AS DATE) >= DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)
-- )
-- GROUP BY member_company_name

-- -- --241
-- --Crescode Group B.V. DD04, Prabakaran DD02



-- SELECT *
-- FROM gold.vw_finance_dashboard_membership_schedule
-- WHERE location_name = 'Amsterdam - Hoofddorp - Taurusavenue 3'
--   AND capacity != 0
--   -- contract started on or before the last day of this month
--   AND CAST(start_date AS DATE) <= EOMONTH(GETUTCDATE())
--   -- and either ongoing (end_date IS NULL) or ends on/after the first day of this month
--   AND (
--       end_date IS NULL
--       OR CAST(end_date AS DATE) >= DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)
--   )
--   AND floor_plan_desk_names = 'DD02'

-- SELECT
--     contract_source_id,
--     member_company_name,
--     tariff_name,
--     capacity,
--     latest_monthly_fee,
--     start_date,
--     end_date,
--     active,
--     cancelled,
--     in_paused_period,
--     floor_plan_desk_names,
--     floor_plan_desk_ids
-- FROM gold.vw_finance_dashboard_membership_schedule
-- WHERE location_name = 'Amsterdam - Hoofddorp - Taurusavenue 3'
--   AND capacity != 0
--   AND CAST(start_date AS DATE) <= EOMONTH(GETUTCDATE())
--   AND (
--       end_date IS NULL
--       OR CAST(end_date AS DATE) >= DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)
--   )
-- ORDER BY capacity DESC, member_company_name;


-- SELECT
--     item_type,
--     p.name,
--     p.capacity,
--     CASE
--         WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
--         WHEN p.item_type IN (2, 3) THEN 1
--         ELSE 0
--     END AS counted_as,
--     p.is_available
-- FROM silver.nexudus_products p
-- INNER JOIN silver.nexudus_locations loc
--     ON loc.source_id = p.location_source_id
-- WHERE loc.name = 'Amsterdam - Hoofddorp - Taurusavenue 3'
--   AND p.item_type IN (1, 2, 3)
--   AND p.is_available = 1
--   AND p.is_deleted = 0
-- ORDER BY item_type, p.name;

EXEC gold.sp_refresh_invoice_worklist;
EXEC gold.sp_refresh_finance_dashboard;

SELECT invoice_number, due_date, invoice_status, processing
FROM silver.nexudus_coworker_invoices
WHERE invoice_number IN ('GB-INV-2026.05-0173', 'GB-INV-2026.05-0175');

SELECT invoice_number, due_date, workflow_type, invoice_status, processing
FROM gold.finance_dashboard_invoice_worklist
WHERE invoice_number IN ('GB-INV-2026.05-0185', 'GB-INV-2026.05-0186','GB-INV-2026.05-0188');



-- How many future-signed contracts are out there?
SELECT active, cancelled, COUNT(*)
FROM silver.nexudus_contracts
WHERE is_deleted = 0 AND start_date > GETUTCDATE()
GROUP BY active, cancelled;

-- Total impact of negative adjustments on current-month revenue
SELECT SUM(sold_monthly_fee) AS net_adjustment_value, COUNT(*) AS adjustment_count
FROM gold.vw_landlord_current_contracts
WHERE is_negative_adjustment = 1;
