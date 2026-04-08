-- =============================================================================
-- xero_invoices_schema.sql
--
-- Creates the bronze and silver tables for Xero invoice sync.
--
--   bronze.xero_invoices            Raw invoice payloads (upsert on tenant+source_id)
--   silver.xero_invoices            Typed invoice rows (upsert on tenant+source_id)
--   silver.xero_invoice_line_items  Line items (DELETE + INSERT per invoice)
--   bronze.xero_invoice_pdfs        Cached PDF bytes (upsert on tenant+invoice_source_id)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.xero_invoices
-- -----------------------------------------------------------------------------
IF OBJECT_ID('bronze.xero_invoices', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.xero_invoices (
        id                  BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id         UNIQUEIDENTIFIER    NOT NULL,
        xero_connection_id  BIGINT              NOT NULL,
        xero_tenant_id      NVARCHAR(128)       NOT NULL,
        source_id           NVARCHAR(128)       NOT NULL,
        raw_json            NVARCHAR(MAX)       NOT NULL,
        synced_at           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_bronze_xero_invoices_tenant_source
            UNIQUE (xero_tenant_id, source_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_bronze_xero_invoices_tenant_id'
      AND object_id = OBJECT_ID('bronze.xero_invoices')
)
BEGIN
    CREATE INDEX ix_bronze_xero_invoices_tenant_id
        ON bronze.xero_invoices (xero_tenant_id);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_bronze_xero_invoices_synced_at'
      AND object_id = OBJECT_ID('bronze.xero_invoices')
)
BEGIN
    CREATE INDEX ix_bronze_xero_invoices_synced_at
        ON bronze.xero_invoices (synced_at);
END
GO

-- -----------------------------------------------------------------------------
-- silver.xero_invoices
-- -----------------------------------------------------------------------------
IF OBJECT_ID('silver.xero_invoices', 'U') IS NULL
BEGIN
    CREATE TABLE silver.xero_invoices (
        id                      BIGINT              IDENTITY(1,1) PRIMARY KEY,
        bronze_id               BIGINT              NULL,
        sync_run_id             UNIQUEIDENTIFIER    NULL,
        xero_connection_id      BIGINT              NOT NULL,
        xero_tenant_id          NVARCHAR(128)       NOT NULL,
        source_id               NVARCHAR(128)       NOT NULL,
        CONSTRAINT uq_silver_xero_invoices_tenant_source
            UNIQUE (xero_tenant_id, source_id),
        invoice_number          NVARCHAR(64)        NULL,
        invoice_type            NVARCHAR(32)        NULL,
        invoice_status          NVARCHAR(32)        NULL,
        contact_id              NVARCHAR(128)       NULL,
        contact_name            NVARCHAR(512)       NULL,
        reference               NVARCHAR(512)       NULL,
        url                     NVARCHAR(2048)      NULL,
        currency_code           NVARCHAR(8)         NULL,
        currency_rate           DECIMAL(18,6)       NULL,
        line_amount_types       NVARCHAR(32)        NULL,
        invoice_date            DATE                NULL,
        due_date                DATE                NULL,
        planned_payment_date    DATE                NULL,
        expected_payment_date   DATE                NULL,
        fully_paid_on_date      DATE                NULL,
        updated_date_utc        DATETIME2           NULL,
        sub_total               DECIMAL(12,2)       NULL,
        total_tax               DECIMAL(12,2)       NULL,
        total                   DECIMAL(12,2)       NULL,
        amount_due              DECIMAL(12,2)       NULL,
        amount_paid             DECIMAL(12,2)       NULL,
        amount_credited         DECIMAL(12,2)       NULL,
        has_attachments         BIT                 NOT NULL DEFAULT 0,
        is_discounted           BIT                 NOT NULL DEFAULT 0,
        has_errors              BIT                 NOT NULL DEFAULT 0,
        pdf_cached_at           DATETIME2           NULL,
        pdf_content_type        NVARCHAR(128)       NULL,
        pdf_file_name           NVARCHAR(512)       NULL,
        pdf_blob_path           NVARCHAR(512)       NULL,
        first_seen_at           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at          DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_invoices_tenant_id'
      AND object_id = OBJECT_ID('silver.xero_invoices')
)
BEGIN
    CREATE INDEX ix_silver_xero_invoices_tenant_id
        ON silver.xero_invoices (xero_tenant_id);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_invoices_invoice_date'
      AND object_id = OBJECT_ID('silver.xero_invoices')
)
BEGIN
    CREATE INDEX ix_silver_xero_invoices_invoice_date
        ON silver.xero_invoices (invoice_date DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_invoices_contact_id'
      AND object_id = OBJECT_ID('silver.xero_invoices')
)
BEGIN
    CREATE INDEX ix_silver_xero_invoices_contact_id
        ON silver.xero_invoices (contact_id);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_invoices_status'
      AND object_id = OBJECT_ID('silver.xero_invoices')
)
BEGIN
    CREATE INDEX ix_silver_xero_invoices_status
        ON silver.xero_invoices (invoice_status);
END
GO

-- -----------------------------------------------------------------------------
-- silver.xero_invoice_line_items
-- -----------------------------------------------------------------------------
IF OBJECT_ID('silver.xero_invoice_line_items', 'U') IS NULL
BEGIN
    CREATE TABLE silver.xero_invoice_line_items (
        id                  BIGINT              IDENTITY(1,1) PRIMARY KEY,
        xero_tenant_id      NVARCHAR(128)       NOT NULL,
        invoice_source_id   NVARCHAR(128)       NOT NULL,
        line_item_index     INT                 NOT NULL,
        description         NVARCHAR(MAX)       NULL,
        item_code           NVARCHAR(255)       NULL,
        account_code        NVARCHAR(64)        NULL,
        tax_type            NVARCHAR(64)        NULL,
        tracking_json       NVARCHAR(MAX)       NULL,
        quantity            DECIMAL(18,4)       NULL,
        unit_amount         DECIMAL(18,4)       NULL,
        line_amount         DECIMAL(12,2)       NULL,
        tax_amount          DECIMAL(12,2)       NULL,
        discount_rate       DECIMAL(18,4)       NULL,
        raw_json            NVARCHAR(MAX)       NOT NULL
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_invoice_line_items_invoice'
      AND object_id = OBJECT_ID('silver.xero_invoice_line_items')
)
BEGIN
    CREATE INDEX ix_silver_xero_invoice_line_items_invoice
        ON silver.xero_invoice_line_items (xero_tenant_id, invoice_source_id);
END
GO

-- -----------------------------------------------------------------------------
-- bronze.xero_invoice_pdfs
-- -----------------------------------------------------------------------------
IF OBJECT_ID('bronze.xero_invoice_pdfs', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.xero_invoice_pdfs (
        id                  BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id         UNIQUEIDENTIFIER    NOT NULL,
        xero_connection_id  BIGINT              NOT NULL,
        xero_tenant_id      NVARCHAR(128)       NOT NULL,
        invoice_source_id   NVARCHAR(128)       NOT NULL,
        content_type        NVARCHAR(128)       NOT NULL,
        file_name           NVARCHAR(512)       NULL,
        pdf_bytes           VARBINARY(MAX)      NOT NULL,
        synced_at           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_bronze_xero_invoice_pdfs_tenant_invoice
            UNIQUE (xero_tenant_id, invoice_source_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_bronze_xero_invoice_pdfs_tenant_id'
      AND object_id = OBJECT_ID('bronze.xero_invoice_pdfs')
)
BEGIN
    CREATE INDEX ix_bronze_xero_invoice_pdfs_tenant_id
        ON bronze.xero_invoice_pdfs (xero_tenant_id);
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_invoice_sync_started_at') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_invoice_sync_started_at DATETIME2 NULL;
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_invoice_sync_completed_at') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_invoice_sync_completed_at DATETIME2 NULL;
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_invoice_modified_utc') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_invoice_modified_utc DATETIME2 NULL;
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_invoice_sync_error') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_invoice_sync_error NVARCHAR(1024) NULL;
END
GO


SELECT
    xero_tenant_id,
    tenant_name,
    last_invoice_sync_started_at,
    last_invoice_sync_completed_at,
    last_invoice_modified_utc,
    last_invoice_sync_error
FROM meta.xero_tenants
ORDER BY tenant_name;

SELECT * FROM silver.xero_invoices
ORDER BY last_synced_at DESC;