-- =============================================================================
-- finance_invoice_worklist_sp.sql
--
-- gold.sp_refresh_invoice_worklist
--
-- Focused variant of gold.sp_refresh_finance_dashboard that rebuilds ONLY
-- the invoice worklist table. Used by the on-demand HTTP endpoint called
-- from the finance dashboard, so user_access (BambooHR) is intentionally
-- skipped — it changes daily at most and is already current from the
-- morning BambooHR sync.
--
-- Silver tables read:
--   silver.nexudus_coworker_invoices      (paid/unpaid status)
--   silver.nexudus_coworker_invoice_lines (workflow_type classification)
--   silver.nexudus_coworkers              (billing email / full name)
--   silver.nexudus_locations              (city, country, name enrichment)
--   meta.finance_dashboard_location_settings (finance email per location)
-- =============================================================================

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

IF OBJECT_ID('gold.finance_dashboard_invoice_worklist', 'U') IS NOT NULL
   AND COL_LENGTH('gold.finance_dashboard_invoice_worklist', 'invoice_status') IS NULL
BEGIN
    ALTER TABLE gold.finance_dashboard_invoice_worklist
    ADD invoice_status NVARCHAR(64) NULL;
END
GO

IF OBJECT_ID('gold.finance_dashboard_invoice_worklist', 'U') IS NOT NULL
   AND COL_LENGTH('gold.finance_dashboard_invoice_worklist', 'processing') IS NULL
BEGIN
    ALTER TABLE gold.finance_dashboard_invoice_worklist
    ADD processing BIT NOT NULL
        CONSTRAINT df_gold_finance_dashboard_invoice_worklist_processing DEFAULT 0;
END
GO

IF OBJECT_ID('gold.finance_dashboard_invoice_worklist', 'U') IS NOT NULL
   AND COL_LENGTH('gold.finance_dashboard_invoice_worklist', 'payment_failure_count') IS NULL
BEGIN
    ALTER TABLE gold.finance_dashboard_invoice_worklist
    ADD payment_failure_count INT NULL;
END
GO

CREATE OR ALTER PROCEDURE gold.sp_refresh_invoice_worklist
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Seed / update location settings from silver.nexudus_locations
    -- (same as sp_refresh_finance_dashboard — keeps meta table current)
    MERGE meta.finance_dashboard_location_settings AS target
    USING (
        SELECT DISTINCT
            source_id AS location_source_id,
            name      AS location_name
        FROM silver.nexudus_locations
        WHERE is_deleted = 0
    ) AS source
        ON target.location_source_id = source.location_source_id
    WHEN MATCHED THEN UPDATE SET
        location_name = source.location_name,
        updated_at    = CASE
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

    -- Step 2: Rebuild invoice worklist only
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
          AND nci.void        = 0
          AND nci.draft       = 0
          AND nci.paid        = 0
          -- Nexudus's CreditNote flag is unreliable — it gets set on normal
          -- invoices that have received credits from a prior invoice (e.g.
          -- ADP INV-2026.05-0645). Real credit notes are paid/zero-balance
          -- so the due_amount > 0 AND paid = 0 gates above already exclude
          -- them.
          AND nci.is_deleted  = 0
          AND ISNULL(nci.processing, 0) = 0
          AND UPPER(ISNULL(nci.invoice_status, N'')) NOT LIKE N'%PROCESSING%'
          AND nci.due_date   >= '2026-03-01'
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
        inv.source_id                                                      AS nexudus_invoice_source_id,
        inv.location_source_id,
        COALESCE(loc.name,       inv.location_name)                        AS location_name,
        loc.city                                                           AS location_city,
        loc.country_name                                                   AS location_country_name,
        ls.finance_location_email                                          AS location_finance_email,
        inv.invoice_number,
        inv.payment_reference,
        inv.coworker_id,
        COALESCE(nc.full_name,   inv.coworker_name)                        AS coworker_name,
        COALESCE(nc.billing_email, nc.email, inv.coworker_billing_email)   AS coworker_billing_email,
        COALESCE(nc.billing_name, nc.company_name,
                 inv.coworker_company_name, inv.bill_to_name)              AS company_display_name,
        COALESCE(nc.billing_email, nc.email, inv.coworker_billing_email)   AS company_email,
        inv.currency_code,
        inv.invoice_status,
        inv.processing,
        inv.payment_failure_count,
        CAST(inv.invoice_from_date AS DATE)                                AS invoice_date,
        inv.due_date_local                                                 AS due_date,
        CAST(GETUTCDATE()          AS DATE)                                AS as_of_date_utc,
        DATEDIFF(DAY, CAST(GETUTCDATE() AS DATE), inv.due_date_local)      AS days_until_due,
        CASE
            WHEN inv.due_date_local < CAST(GETUTCDATE() AS DATE)
                THEN DATEDIFF(DAY, inv.due_date_local, CAST(GETUTCDATE() AS DATE))
            ELSE 0
        END                                                                AS days_overdue,
        CASE
            WHEN inv.due_date_local < CAST(GETUTCDATE() AS DATE) THEN N'overdue'
            WHEN inv.due_date_local = CAST(GETUTCDATE() AS DATE) THEN N'due_today'
            ELSE N'upcoming'
        END                                                                AS due_state,
        inv.total_amount,
        inv.due_amount,
        inv.paid_amount,
        CASE
            WHEN ISNULL(af.has_recurrent_account, 0) = 1 THEN N'recurrent'
            ELSE N'one_off'
        END                                                                AS workflow_type,
        inv.pdf_blob_path,
        inv.last_synced_at
    FROM unpaid_nexudus_invoices inv
    LEFT JOIN silver.nexudus_locations loc
        ON loc.source_id  = inv.location_source_id
       AND loc.is_deleted = 0
    LEFT JOIN silver.nexudus_coworkers nc
        ON nc.source_id   = inv.coworker_id
       AND nc.is_deleted  = 0
    LEFT JOIN meta.finance_dashboard_location_settings ls
        ON ls.location_source_id = inv.location_source_id
    LEFT JOIN invoice_account_flags af
        ON af.invoice_source_id = inv.source_id;
END
GO
