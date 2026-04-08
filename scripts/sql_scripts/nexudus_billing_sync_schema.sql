-- =============================================================================
-- nexudus_billing_sync_schema.sql
--
-- Adds Nexudus billing/customer entities needed to link Xero overdue invoices
-- back to Nexudus customer contact data.
-- =============================================================================

IF OBJECT_ID('bronze.nexudus_coworker_invoices', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.nexudus_coworker_invoices (
        id              BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id     UNIQUEIDENTIFIER    NOT NULL,
        source_id       BIGINT              NOT NULL,
        location_id     BIGINT              NULL,
        coworker_id     BIGINT              NULL,
        raw_json        NVARCHAR(MAX)       NOT NULL,
        synced_at       DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_bronze_nexudus_coworker_invoices_source UNIQUE (source_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_bronze_nexudus_coworker_invoices_location'
      AND object_id = OBJECT_ID('bronze.nexudus_coworker_invoices')
)
BEGIN
    CREATE INDEX ix_bronze_nexudus_coworker_invoices_location
        ON bronze.nexudus_coworker_invoices (location_id);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_bronze_nexudus_coworker_invoices_coworker'
      AND object_id = OBJECT_ID('bronze.nexudus_coworker_invoices')
)
BEGIN
    CREATE INDEX ix_bronze_nexudus_coworker_invoices_coworker
        ON bronze.nexudus_coworker_invoices (coworker_id);
END
GO

IF OBJECT_ID('bronze.nexudus_coworkers', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.nexudus_coworkers (
        id              BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id     UNIQUEIDENTIFIER    NOT NULL,
        source_id       BIGINT              NOT NULL,
        location_id     BIGINT              NULL,
        raw_json        NVARCHAR(MAX)       NOT NULL,
        synced_at       DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_bronze_nexudus_coworkers_source UNIQUE (source_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_bronze_nexudus_coworkers_location'
      AND object_id = OBJECT_ID('bronze.nexudus_coworkers')
)
BEGIN
    CREATE INDEX ix_bronze_nexudus_coworkers_location
        ON bronze.nexudus_coworkers (location_id);
END
GO

IF OBJECT_ID('silver.nexudus_coworker_invoices', 'U') IS NULL
BEGIN
    CREATE TABLE silver.nexudus_coworker_invoices (
        id                          BIGINT              IDENTITY(1,1) PRIMARY KEY,
        source_id                   BIGINT              NOT NULL,
        CONSTRAINT uq_silver_nexudus_coworker_invoices_source UNIQUE (source_id),
        unique_id                   NVARCHAR(64)        NULL,
        bronze_id                   BIGINT              NULL,
        sync_run_id                 UNIQUEIDENTIFIER    NULL,
        coworker_id                 BIGINT              NULL,
        coworker_name               NVARCHAR(512)       NULL,
        coworker_billing_email      NVARCHAR(512)       NULL,
        coworker_company_name       NVARCHAR(512)       NULL,
        coworker_team_names         NVARCHAR(512)       NULL,
        location_source_id          BIGINT              NULL,
        location_name               NVARCHAR(512)       NULL,
        invoice_number              NVARCHAR(128)       NULL,
        payment_reference           NVARCHAR(128)       NULL,
        bill_to_name                NVARCHAR(512)       NULL,
        bill_to_address             NVARCHAR(1024)      NULL,
        bill_to_city                NVARCHAR(255)       NULL,
        bill_to_post_code           NVARCHAR(64)        NULL,
        bill_to_state               NVARCHAR(255)       NULL,
        bill_to_country_name        NVARCHAR(128)       NULL,
        bill_to_tax_id_number       NVARCHAR(128)       NULL,
        description                 NVARCHAR(MAX)       NULL,
        currency_code               NVARCHAR(8)         NULL,
        due_date                    DATETIME2           NULL,
        invoice_from_date           DATETIME2           NULL,
        invoice_to_date             DATETIME2           NULL,
        sent_on                     DATETIME2           NULL,
        paid_on                     DATETIME2           NULL,
        refunded_on                 DATETIME2           NULL,
        total_amount                DECIMAL(12,2)       NULL,
        paid_amount                 DECIMAL(12,2)       NULL,
        due_amount                  DECIMAL(12,2)       NULL,
        received_amount             DECIMAL(12,2)       NULL,
        credited_amount             DECIMAL(12,2)       NULL,
        refunded_amount             DECIMAL(12,2)       NULL,
        tax_amount                  DECIMAL(12,2)       NULL,
        draft                       BIT                 NULL,
        void                        BIT                 NULL,
        paid                        BIT                 NULL,
        sent                        BIT                 NULL,
        refunded                    BIT                 NULL,
        credit_note                 BIT                 NULL,
        is_due                      BIT                 NULL,
        xero_invoice_transferred    BIT                 NULL,
        xero_payment_transferred    BIT                 NULL,
        created_on                  DATETIME2           NULL,
        updated_on                  DATETIME2           NULL,
        first_seen_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at              DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworker_invoices_invoice_number'
      AND object_id = OBJECT_ID('silver.nexudus_coworker_invoices')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworker_invoices_invoice_number
        ON silver.nexudus_coworker_invoices (invoice_number);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworker_invoices_location'
      AND object_id = OBJECT_ID('silver.nexudus_coworker_invoices')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworker_invoices_location
        ON silver.nexudus_coworker_invoices (location_source_id);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworker_invoices_coworker'
      AND object_id = OBJECT_ID('silver.nexudus_coworker_invoices')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworker_invoices_coworker
        ON silver.nexudus_coworker_invoices (coworker_id);
END
GO

IF OBJECT_ID('silver.nexudus_coworkers', 'U') IS NULL
BEGIN
    CREATE TABLE silver.nexudus_coworkers (
        id                                      BIGINT              IDENTITY(1,1) PRIMARY KEY,
        source_id                               BIGINT              NOT NULL,
        CONSTRAINT uq_silver_nexudus_coworkers_source UNIQUE (source_id),
        unique_id                               NVARCHAR(64)        NULL,
        bronze_id                               BIGINT              NULL,
        sync_run_id                             UNIQUEIDENTIFIER    NULL,
        coworker_type                           TINYINT             NULL,
        full_name                               NVARCHAR(512)       NULL,
        email                                   NVARCHAR(512)       NULL,
        billing_email                           NVARCHAR(512)       NULL,
        billing_name                            NVARCHAR(512)       NULL,
        company_name                            NVARCHAR(512)       NULL,
        team_name                               NVARCHAR(512)       NULL,
        team_names                              NVARCHAR(1024)      NULL,
        team_ids                                NVARCHAR(1024)      NULL,
        business_ids                            NVARCHAR(1024)      NULL,
        location_source_id                      BIGINT              NULL,
        location_name                           NVARCHAR(512)       NULL,
        mobile_phone                            NVARCHAR(128)       NULL,
        land_line                               NVARCHAR(128)       NULL,
        address                                 NVARCHAR(1024)      NULL,
        post_code                               NVARCHAR(64)        NULL,
        city_name                               NVARCHAR(255)       NULL,
        state                                   NVARCHAR(255)       NULL,
        billing_address                         NVARCHAR(1024)      NULL,
        billing_post_code                       NVARCHAR(64)        NULL,
        billing_city_name                       NVARCHAR(255)       NULL,
        billing_state                           NVARCHAR(255)       NULL,
        tax_id_number                           NVARCHAR(128)       NULL,
        billing_day                             INT                 NULL,
        tariff_id                               BIGINT              NULL,
        tariff_name                             NVARCHAR(512)       NULL,
        next_tariff_id                          BIGINT              NULL,
        next_tariff_name                        NVARCHAR(512)       NULL,
        coworker_contract_ids                   NVARCHAR(MAX)       NULL,
        coworker_contract_tariff_names          NVARCHAR(MAX)       NULL,
        active                                  BIT                 NULL,
        archived                                BIT                 NULL,
        user_active                             BIT                 NULL,
        notify_on_new_invoice                   BIT                 NULL,
        notify_on_new_payment                   BIT                 NULL,
        notify_on_failed_payment                BIT                 NULL,
        do_not_process_invoices_automatically   BIT                 NULL,
        user_last_access                        DATETIME2           NULL,
        registration_date                       DATETIME2           NULL,
        renewal_date                            DATETIME2           NULL,
        start_date                              DATETIME2           NULL,
        cancellation_date                       DATETIME2           NULL,
        created_on                              DATETIME2           NULL,
        updated_on                              DATETIME2           NULL,
        first_seen_at                           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at                          DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworkers_location'
      AND object_id = OBJECT_ID('silver.nexudus_coworkers')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworkers_location
        ON silver.nexudus_coworkers (location_source_id);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworkers_email'
      AND object_id = OBJECT_ID('silver.nexudus_coworkers')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworkers_email
        ON silver.nexudus_coworkers (email);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworkers_billing_email'
      AND object_id = OBJECT_ID('silver.nexudus_coworkers')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworkers_billing_email
        ON silver.nexudus_coworkers (billing_email);
END
GO

EXEC sp_executesql N'
CREATE OR ALTER VIEW silver.xero_overdue_invoice_contacts AS
SELECT
    xi.xero_connection_id,
    xi.xero_tenant_id,
    xt.location_source_id AS xero_location_source_id,
    xi.source_id AS xero_invoice_source_id,
    xi.invoice_number,
    xi.reference,
    xi.contact_name,
    xi.due_date,
    xi.amount_due,
    ni.source_id AS nexudus_invoice_source_id,
    ni.coworker_id AS nexudus_coworker_id,
    ni.location_source_id AS nexudus_location_source_id,
    ni.invoice_number AS nexudus_invoice_number,
    ni.payment_reference AS nexudus_payment_reference,
    COALESCE(nc.billing_email, nc.email, ni.coworker_billing_email) AS recipient_email,
    nc.full_name AS coworker_full_name,
    nc.billing_name AS coworker_billing_name,
    CASE
        WHEN ni.source_id IS NULL THEN ''unmatched''
        WHEN ni.invoice_number = xi.invoice_number THEN ''invoice_number''
        WHEN ni.payment_reference = xi.invoice_number THEN ''payment_reference''
        WHEN ni.invoice_number = xi.reference THEN ''xero_reference''
        WHEN ni.payment_reference = xi.reference THEN ''xero_reference_payment_reference''
        ELSE ''matched''
    END AS match_reason
FROM silver.xero_invoices xi
LEFT JOIN silver.xero_tenants xt
  ON xt.xero_connection_id = xi.xero_connection_id
 AND xt.xero_tenant_id = xi.xero_tenant_id
OUTER APPLY (
    SELECT TOP 1 *
    FROM silver.nexudus_coworker_invoices ni
    WHERE ni.invoice_number = xi.invoice_number
       OR ni.payment_reference = xi.invoice_number
       OR (xi.reference IS NOT NULL AND ni.invoice_number = xi.reference)
       OR (xi.reference IS NOT NULL AND ni.payment_reference = xi.reference)
    ORDER BY
        CASE WHEN ni.location_source_id = xt.location_source_id THEN 0 ELSE 1 END,
        CASE
            WHEN ni.invoice_number = xi.invoice_number THEN 0
            WHEN ni.payment_reference = xi.invoice_number THEN 1
            WHEN ni.invoice_number = xi.reference THEN 2
            WHEN ni.payment_reference = xi.reference THEN 3
            ELSE 9
        END,
        ni.last_synced_at DESC
) ni
LEFT JOIN silver.nexudus_coworkers nc
  ON nc.source_id = ni.coworker_id
WHERE xi.invoice_status = ''AUTHORISED''
  AND xi.amount_due > 0
  AND xi.due_date < CAST(GETUTCDATE() AS DATE);
';
GO


-- How many records landed?
SELECT COUNT(*) AS invoice_count FROM bronze.nexudus_coworker_invoices;
SELECT COUNT(*) AS coworker_count FROM bronze.nexudus_coworkers;
SELECT COUNT(*) AS silver_invoices FROM silver.nexudus_coworker_invoices;
SELECT COUNT(*) AS silver_coworkers FROM silver.nexudus_coworkers;

-- Sample some invoices with customer data
SELECT TOP 10
    invoice_number, coworker_name, coworker_billing_email,
    location_name, due_date, total_amount, paid, is_due
FROM silver.nexudus_coworker_invoices
ORDER BY due_date DESC;

-- Check the Xero link view — overdue invoices with customer emails
SELECT TOP 20
    invoice_number, contact_name, due_date, amount_due,
    recipient_email, coworker_full_name, match_reason
FROM silver.xero_overdue_invoice_contacts
ORDER BY due_date ASC;

-- How many overdue invoices have a matched email vs unmatched?
SELECT match_reason, COUNT(*) AS cnt
FROM silver.xero_overdue_invoice_contacts
GROUP BY match_reason
ORDER BY cnt DESC;
