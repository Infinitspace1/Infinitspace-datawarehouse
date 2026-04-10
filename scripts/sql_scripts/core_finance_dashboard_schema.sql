-- =============================================================================
-- core_finance_dashboard_schema.sql
--
-- Finance dashboard production model.
--
-- Design:
--   - meta.finance_dashboard_tenant_settings
--       Manual per-tenant finance email settings.
--   - meta.finance_dashboard_xero_account_map
--       Optional per-tenant override map for Xero account classification.
--   - gold.finance_dashboard_user_access
--       Materialized user/location/tenant access table.
--   - gold.finance_dashboard_invoice_worklist
--       Materialized invoice worklist table for the website.
--   - gold.sp_refresh_finance_dashboard
--       Rebuilds both gold tables from silver/meta data.
--
-- Why gold tables instead of views:
--   The website should read indexed production tables, not recompute the
--   invoice matching logic live on every request.
--
-- Xero account classification:
--   Classification now prefers silver.xero_accounts.account_label /
--   silver.xero_accounts.account_name. The dashboard marks an invoice as
--   recurrent when any linked Xero invoice line resolves to an account whose
--   label/name contains "Membership Fees". The manual table
--   meta.finance_dashboard_xero_account_map is kept as an override path.
--
-- Fallback:
--   If account metadata is not yet available for a line item, the old
--   account_code = 200 heuristic is kept as a temporary fallback.
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

IF OBJECT_ID('meta.finance_dashboard_tenant_settings', 'U') IS NULL
BEGIN
    CREATE TABLE meta.finance_dashboard_tenant_settings (
        xero_tenant_id           NVARCHAR(128)       NOT NULL PRIMARY KEY,
        tenant_name              NVARCHAR(512)       NULL,
        finance_tenant_email     NVARCHAR(512)       NULL,
        notes                    NVARCHAR(1024)      NULL,
        created_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        updated_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF OBJECT_ID('meta.finance_dashboard_xero_account_map', 'U') IS NULL
BEGIN
    CREATE TABLE meta.finance_dashboard_xero_account_map (
        xero_tenant_id           NVARCHAR(128)       NOT NULL,
        account_code             NVARCHAR(64)        NOT NULL,
        account_label            NVARCHAR(512)       NULL,
        workflow_type            NVARCHAR(32)        NULL,
        notes                    NVARCHAR(1024)      NULL,
        created_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        updated_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_meta_finance_dashboard_xero_account_map
            PRIMARY KEY (xero_tenant_id, account_code),
        CONSTRAINT ck_meta_finance_dashboard_xero_account_map_workflow
            CHECK (workflow_type IS NULL OR workflow_type IN (N'recurrent', N'one_off'))
    );
END
GO

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
    location_email              NVARCHAR(512)       NULL,
    xero_connection_id          BIGINT              NULL,
    xero_tenant_id              NVARCHAR(128)       NULL,
    tenant_name                 NVARCHAR(512)       NULL,
    location_match_rule         NVARCHAR(128)       NULL,
    has_xero_tenant             BIT                 NOT NULL
);
GO

CREATE INDEX ix_gold_finance_dashboard_user_access_work_email
    ON gold.finance_dashboard_user_access (work_email);
GO

CREATE INDEX ix_gold_finance_dashboard_user_access_location
    ON gold.finance_dashboard_user_access (location_source_id);
GO

CREATE INDEX ix_gold_finance_dashboard_user_access_tenant
    ON gold.finance_dashboard_user_access (xero_tenant_id);
GO

DROP TABLE IF EXISTS gold.finance_dashboard_invoice_worklist;
GO

CREATE TABLE gold.finance_dashboard_invoice_worklist (
    xero_tenant_id              NVARCHAR(128)       NOT NULL,
    tenant_name                 NVARCHAR(512)       NULL,
    location_source_id          BIGINT              NULL,
    location_name               NVARCHAR(512)       NULL,
    location_city               NVARCHAR(255)       NULL,
    location_country_name       NVARCHAR(128)       NULL,
    tenant_finance_email        NVARCHAR(512)       NULL,
    xero_invoice_source_id      NVARCHAR(128)       NOT NULL,
    invoice_number              NVARCHAR(64)        NULL,
    invoice_status              NVARCHAR(32)        NULL,
    contact_id                  NVARCHAR(128)       NULL,
    contact_name                NVARCHAR(512)       NULL,
    company_display_name        NVARCHAR(512)       NULL,
    company_email               NVARCHAR(512)       NULL,
    nexudus_invoice_source_id   BIGINT              NULL,
    nexudus_coworker_id         BIGINT              NULL,
    coworker_name               NVARCHAR(512)       NULL,
    reference                   NVARCHAR(512)       NULL,
    currency_code               NVARCHAR(8)         NULL,
    invoice_date                DATE                NULL,
    due_date                    DATE                NULL,
    as_of_date_utc              DATE                NOT NULL,
    days_until_due              INT                 NULL,
    days_overdue                INT                 NULL,
    due_state                   NVARCHAR(32)        NOT NULL,
    total                       DECIMAL(12,2)       NULL,
    amount_due                  DECIMAL(12,2)       NULL,
    amount_paid                 DECIMAL(12,2)       NULL,
    workflow_type               NVARCHAR(32)        NOT NULL,
    last_synced_at              DATETIME2           NULL,
    CONSTRAINT pk_gold_finance_dashboard_invoice_worklist
        PRIMARY KEY (xero_tenant_id, xero_invoice_source_id)
);
GO

CREATE INDEX ix_gold_finance_dashboard_invoice_worklist_due_date
    ON gold.finance_dashboard_invoice_worklist (due_date);
GO

CREATE INDEX ix_gold_finance_dashboard_invoice_worklist_tenant_due
    ON gold.finance_dashboard_invoice_worklist (xero_tenant_id, due_date);
GO

CREATE INDEX ix_gold_finance_dashboard_invoice_worklist_workflow_due
    ON gold.finance_dashboard_invoice_worklist (workflow_type, due_date);
GO

CREATE OR ALTER PROCEDURE gold.sp_refresh_finance_dashboard
AS
BEGIN
    SET NOCOUNT ON;

    MERGE meta.finance_dashboard_tenant_settings AS target
    USING (
        SELECT DISTINCT
            xero_tenant_id,
            tenant_name
        FROM silver.xero_tenants
    ) AS source
        ON target.xero_tenant_id = source.xero_tenant_id
    WHEN MATCHED THEN UPDATE SET
        tenant_name = source.tenant_name,
        updated_at = CASE
            WHEN ISNULL(target.tenant_name, N'') <> ISNULL(source.tenant_name, N'')
                THEN GETUTCDATE()
            ELSE target.updated_at
        END
    WHEN NOT MATCHED THEN INSERT (
        xero_tenant_id,
        tenant_name
    ) VALUES (
        source.xero_tenant_id,
        source.tenant_name
    );

    MERGE meta.finance_dashboard_tenant_settings AS target
    USING (
        SELECT DISTINCT
            xt.xero_tenant_id,
            xt.tenant_name,
            seed.finance_tenant_email
        FROM silver.xero_tenants xt
        INNER JOIN (
            VALUES
                (CAST(1376491118 AS BIGINT), N'finance.aldgatetower@infinitspace.com'),
                (CAST(1420976475 AS BIGINT), N'finance.c29@infinitspace.com'),
                (CAST(1420962233 AS BIGINT), N'finance.qh@infinitspace.com'),
                (CAST(1415499547 AS BIGINT), N'finance.thebower@infinitspace.com'),
                (CAST(1414964752 AS BIGINT), N'finance.kingsbournehouse@infinitspace.com'),
                (CAST(1420976575 AS BIGINT), N'finance.foxcourt@infinitspace.com'),
                (CAST(1415079491 AS BIGINT), N'finance.republica@infinitspace.com'),
                (CAST(1420951935 AS BIGINT), N'finance.goudenbocht@infinitspace.com'),
                (CAST(1414964753 AS BIGINT), N'finance.zuidtoren@infinitspace.com')
        ) AS seed(location_source_id, finance_tenant_email)
            ON seed.location_source_id = xt.location_source_id
    ) AS source
        ON target.xero_tenant_id = source.xero_tenant_id
    WHEN MATCHED THEN UPDATE SET
        tenant_name = source.tenant_name,
        finance_tenant_email = source.finance_tenant_email,
        updated_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        xero_tenant_id,
        tenant_name,
        finance_tenant_email
    ) VALUES (
        source.xero_tenant_id,
        source.tenant_name,
        source.finance_tenant_email
    );

    MERGE meta.finance_dashboard_xero_account_map AS target
    USING (
        SELECT DISTINCT
            li.xero_tenant_id,
            COALESCE(xa.account_code, li.account_code) AS account_code,
            xa.account_label
        FROM silver.xero_invoice_line_items li
        INNER JOIN silver.xero_invoices inv
            ON inv.xero_tenant_id = li.xero_tenant_id
           AND inv.source_id = li.invoice_source_id
        OUTER APPLY (
            SELECT TOP 1
                xa.account_code,
                xa.account_label
            FROM silver.xero_accounts xa
            WHERE xa.xero_tenant_id = li.xero_tenant_id
              AND (
                    (li.account_id IS NOT NULL AND xa.source_id = li.account_id)
                 OR (li.account_id IS NULL AND li.account_code IS NOT NULL AND xa.account_code = li.account_code)
              )
            ORDER BY
                CASE
                    WHEN li.account_id IS NOT NULL AND xa.source_id = li.account_id THEN 0
                    ELSE 1
                END,
                xa.last_synced_at DESC,
                xa.id DESC
        ) xa
        WHERE inv.url LIKE '%nexudus%'
          AND COALESCE(xa.account_code, li.account_code) IS NOT NULL
    ) AS source
        ON target.xero_tenant_id = source.xero_tenant_id
       AND target.account_code = source.account_code
    WHEN MATCHED
     AND ISNULL(target.account_label, N'') <> ISNULL(source.account_label, N'') THEN UPDATE SET
        account_label = source.account_label,
        updated_at = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT (
        xero_tenant_id,
        account_code,
        account_label,
        workflow_type,
        notes
    ) VALUES (
        source.xero_tenant_id,
        source.account_code,
        source.account_label,
        CASE
            WHEN UPPER(ISNULL(source.account_label, N'')) LIKE N'%MEMBERSHIP FEES%' THEN N'recurrent'
            ELSE NULL
        END,
        CASE
            WHEN UPPER(ISNULL(source.account_label, N'')) LIKE N'%MEMBERSHIP FEES%'
                THEN N'autoseeded from silver.xero_accounts'
            ELSE NULL
        END
    );

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
        location_email,
        xero_connection_id,
        xero_tenant_id,
        tenant_name,
        location_match_rule,
        has_xero_tenant
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
        loc.email AS location_email,
        xt.xero_connection_id,
        xt.xero_tenant_id,
        xt.tenant_name,
        xt.location_match_rule,
        CASE
            WHEN xt.xero_tenant_id IS NULL THEN CAST(0 AS BIT)
            ELSE CAST(1 AS BIT)
        END AS has_xero_tenant
    FROM cm_roster cm
    INNER JOIN location_access_map map
        ON map.bamboohr_location_normalized = cm.bamboohr_location_normalized
    LEFT JOIN silver.nexudus_locations loc
        ON loc.source_id = map.location_source_id
    LEFT JOIN silver.xero_tenants xt
        ON xt.location_source_id = map.location_source_id;

    TRUNCATE TABLE gold.finance_dashboard_invoice_worklist;

    WITH unpaid_xero_invoices AS (
        SELECT
            inv.xero_connection_id,
            inv.xero_tenant_id,
            inv.source_id,
            inv.invoice_number,
            inv.invoice_status,
            inv.contact_id,
            inv.contact_name,
            inv.reference,
            inv.currency_code,
            inv.invoice_date,
            inv.due_date,
            inv.total,
            inv.amount_due,
            inv.amount_paid,
            inv.last_synced_at
        FROM silver.xero_invoices inv
        WHERE inv.url LIKE '%nexudus%'
          AND inv.invoice_status = 'AUTHORISED'
          AND inv.amount_due > 0
          AND COALESCE(inv.invoice_type, N'ACCREC') = N'ACCREC'
    ),
    invoice_base AS (
        SELECT
            inv.xero_connection_id,
            inv.xero_tenant_id,
            inv.source_id AS xero_invoice_source_id,
            inv.invoice_number,
            inv.invoice_status,
            inv.contact_id,
            inv.contact_name,
            inv.reference,
            inv.currency_code,
            inv.invoice_date,
            inv.due_date,
            inv.total,
            inv.amount_due,
            inv.amount_paid,
            inv.last_synced_at,
            COALESCE(xt.tenant_name, inv.xero_tenant_id) AS tenant_name,
            xt.location_source_id,
            xt.location_name,
            xt.location_city,
            xt.location_country_name,
            tenant_settings.finance_tenant_email AS tenant_finance_email
        FROM unpaid_xero_invoices inv
        LEFT JOIN silver.xero_tenants xt
            ON xt.xero_connection_id = inv.xero_connection_id
           AND xt.xero_tenant_id = inv.xero_tenant_id
        LEFT JOIN meta.finance_dashboard_tenant_settings tenant_settings
            ON tenant_settings.xero_tenant_id = inv.xero_tenant_id
    ),
    invoice_match_keys AS (
        SELECT
            ib.xero_tenant_id,
            ib.xero_invoice_source_id,
            ib.location_source_id,
            ib.invoice_number AS match_key,
            0 AS invoice_key_rank
        FROM invoice_base ib
        WHERE ib.invoice_number IS NOT NULL
          AND LTRIM(RTRIM(ib.invoice_number)) <> N''

        UNION ALL

        SELECT
            ib.xero_tenant_id,
            ib.xero_invoice_source_id,
            ib.location_source_id,
            ib.reference AS match_key,
            2 AS invoice_key_rank
        FROM invoice_base ib
        WHERE ib.reference IS NOT NULL
          AND LTRIM(RTRIM(ib.reference)) <> N''
    ),
    nexudus_match_keys AS (
        SELECT
            nci.source_id AS nexudus_invoice_source_id,
            nci.coworker_id AS nexudus_coworker_id,
            nci.coworker_name,
            nci.coworker_billing_email,
            nci.coworker_company_name,
            nci.bill_to_name,
            nci.location_source_id AS nexudus_location_source_id,
            nci.last_synced_at,
            nci.invoice_number AS match_key,
            0 AS nexudus_key_rank
        FROM silver.nexudus_coworker_invoices nci
        WHERE nci.invoice_number IS NOT NULL
          AND LTRIM(RTRIM(nci.invoice_number)) <> N''

        UNION ALL

        SELECT
            nci.source_id AS nexudus_invoice_source_id,
            nci.coworker_id AS nexudus_coworker_id,
            nci.coworker_name,
            nci.coworker_billing_email,
            nci.coworker_company_name,
            nci.bill_to_name,
            nci.location_source_id AS nexudus_location_source_id,
            nci.last_synced_at,
            nci.payment_reference AS match_key,
            1 AS nexudus_key_rank
        FROM silver.nexudus_coworker_invoices nci
        WHERE nci.payment_reference IS NOT NULL
          AND LTRIM(RTRIM(nci.payment_reference)) <> N''
    ),
    ranked_matches AS (
        SELECT
            imk.xero_tenant_id,
            imk.xero_invoice_source_id,
            nmk.nexudus_invoice_source_id,
            nmk.nexudus_coworker_id,
            nmk.coworker_name,
            nmk.coworker_billing_email,
            nmk.coworker_company_name,
            nmk.bill_to_name,
            ROW_NUMBER() OVER (
                PARTITION BY imk.xero_tenant_id, imk.xero_invoice_source_id
                ORDER BY
                    CASE WHEN nmk.nexudus_location_source_id = imk.location_source_id THEN 0 ELSE 1 END,
                    imk.invoice_key_rank + nmk.nexudus_key_rank,
                    nmk.last_synced_at DESC,
                    nmk.nexudus_invoice_source_id DESC
            ) AS rn
        FROM invoice_match_keys imk
        INNER JOIN nexudus_match_keys nmk
            ON nmk.match_key = imk.match_key
    ),
    best_match AS (
        SELECT
            xero_tenant_id,
            xero_invoice_source_id,
            nexudus_invoice_source_id,
            nexudus_coworker_id,
            coworker_name,
            coworker_billing_email,
            coworker_company_name,
            bill_to_name
        FROM ranked_matches
        WHERE rn = 1
    ),
    invoice_account_flags AS (
        SELECT
            li.xero_tenant_id,
            li.invoice_source_id AS xero_invoice_source_id,
            MAX(
                CASE
                    WHEN COALESCE(
                        account_map.workflow_type,
                        CASE
                            WHEN UPPER(COALESCE(xa.account_label, xa.account_name, N'')) LIKE N'%MEMBERSHIP FEES%'
                                THEN N'recurrent'
                            WHEN li.account_code = N'200'
                                THEN N'recurrent'
                            ELSE N'one_off'
                        END
                    ) = N'recurrent'
                    THEN 1
                    ELSE 0
                END
            ) AS has_recurrent_account
        FROM silver.xero_invoice_line_items li
        INNER JOIN unpaid_xero_invoices inv
            ON inv.xero_tenant_id = li.xero_tenant_id
           AND inv.source_id = li.invoice_source_id
        OUTER APPLY (
            SELECT TOP 1
                xa.account_code,
                xa.account_name,
                xa.account_label
            FROM silver.xero_accounts xa
            WHERE xa.xero_tenant_id = li.xero_tenant_id
              AND (
                    (li.account_id IS NOT NULL AND xa.source_id = li.account_id)
                 OR (li.account_id IS NULL AND li.account_code IS NOT NULL AND xa.account_code = li.account_code)
              )
            ORDER BY
                CASE
                    WHEN li.account_id IS NOT NULL AND xa.source_id = li.account_id THEN 0
                    ELSE 1
                END,
                xa.last_synced_at DESC,
                xa.id DESC
        ) xa
        LEFT JOIN meta.finance_dashboard_xero_account_map account_map
            ON account_map.xero_tenant_id = li.xero_tenant_id
           AND account_map.account_code = COALESCE(xa.account_code, li.account_code)
        GROUP BY
            li.xero_tenant_id,
            li.invoice_source_id
    )
    INSERT INTO gold.finance_dashboard_invoice_worklist (
        xero_tenant_id,
        tenant_name,
        location_source_id,
        location_name,
        location_city,
        location_country_name,
        tenant_finance_email,
        xero_invoice_source_id,
        invoice_number,
        invoice_status,
        contact_id,
        contact_name,
        company_display_name,
        company_email,
        nexudus_invoice_source_id,
        nexudus_coworker_id,
        coworker_name,
        reference,
        currency_code,
        invoice_date,
        due_date,
        as_of_date_utc,
        days_until_due,
        days_overdue,
        due_state,
        total,
        amount_due,
        amount_paid,
        workflow_type,
        last_synced_at
    )
    SELECT
        ib.xero_tenant_id,
        ib.tenant_name,
        ib.location_source_id,
        ib.location_name,
        ib.location_city,
        ib.location_country_name,
        ib.tenant_finance_email,
        ib.xero_invoice_source_id,
        ib.invoice_number,
        ib.invoice_status,
        ib.contact_id,
        ib.contact_name,
        COALESCE(
            nc.billing_name,
            bm.bill_to_name,
            nc.company_name,
            bm.coworker_company_name,
            ib.contact_name
        ) AS company_display_name,
        COALESCE(
            nc.billing_email,
            nc.email,
            bm.coworker_billing_email
        ) AS company_email,
        bm.nexudus_invoice_source_id,
        bm.nexudus_coworker_id,
        COALESCE(nc.full_name, bm.coworker_name) AS coworker_name,
        ib.reference,
        ib.currency_code,
        ib.invoice_date,
        ib.due_date,
        CAST(GETUTCDATE() AS DATE) AS as_of_date_utc,
        DATEDIFF(DAY, CAST(GETUTCDATE() AS DATE), ib.due_date) AS days_until_due,
        CASE
            WHEN ib.due_date IS NULL THEN NULL
            WHEN ib.due_date < CAST(GETUTCDATE() AS DATE)
                THEN DATEDIFF(DAY, ib.due_date, CAST(GETUTCDATE() AS DATE))
            ELSE 0
        END AS days_overdue,
        CASE
            WHEN ib.due_date IS NULL THEN N'unknown'
            WHEN ib.due_date < CAST(GETUTCDATE() AS DATE) THEN N'overdue'
            WHEN ib.due_date = CAST(GETUTCDATE() AS DATE) THEN N'due_today'
            ELSE N'upcoming'
        END AS due_state,
        ib.total,
        ib.amount_due,
        ib.amount_paid,
        CASE
            WHEN ISNULL(account_flags.has_recurrent_account, 0) = 1 THEN N'recurrent'
            ELSE N'one_off'
        END AS workflow_type,
        ib.last_synced_at
    FROM invoice_base ib
    LEFT JOIN invoice_account_flags account_flags
        ON account_flags.xero_tenant_id = ib.xero_tenant_id
       AND account_flags.xero_invoice_source_id = ib.xero_invoice_source_id
    LEFT JOIN best_match bm
        ON bm.xero_tenant_id = ib.xero_tenant_id
       AND bm.xero_invoice_source_id = ib.xero_invoice_source_id
    LEFT JOIN silver.nexudus_coworkers nc
        ON nc.source_id = bm.nexudus_coworker_id;
END
GO

EXEC gold.sp_refresh_finance_dashboard;
GO

-- Manual maintenance examples
-- 1. Fill the tenant finance email used by the dashboard / reminder flow.
--    Known location emails are auto-seeded by this procedure.
-- UPDATE meta.finance_dashboard_tenant_settings
-- SET finance_tenant_email = 'finance@example.com',
--     updated_at = GETUTCDATE()
-- WHERE xero_tenant_id = '<xero_tenant_id>';
--
-- 2. Review discovered Xero account codes and optional overrides.
-- SELECT *
-- FROM meta.finance_dashboard_xero_account_map
-- ORDER BY xero_tenant_id, account_code;
--
-- UPDATE meta.finance_dashboard_xero_account_map
-- SET account_label = '200 - Sales: Membership Fees',
--     workflow_type = 'recurrent',
--     updated_at = GETUTCDATE()
-- WHERE xero_tenant_id = '<xero_tenant_id>'
--   AND account_code = '200';
--
-- 3. Rebuild the gold tables after changing settings / mappings.
-- EXEC gold.sp_refresh_finance_dashboard;
--
-- Example queries
SELECT TOP 20 *
FROM gold.finance_dashboard_invoice_worklist
ORDER BY due_date ASC, invoice_number ASC;

SELECT *
FROM gold.finance_dashboard_user_access
ORDER BY location_name, job_title, employee_name;


EXEC gold.sp_refresh_finance_dashboard;