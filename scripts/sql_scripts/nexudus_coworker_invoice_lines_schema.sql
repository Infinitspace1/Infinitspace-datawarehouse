-- =============================================================================
-- nexudus_coworker_invoice_lines_schema.sql
--
-- Bronze and silver tables for Nexudus coworker invoice line items.
-- Source: GET /api/billing/coworkerinvoicelines?CoworkerInvoiceLine_CoworkerInvoice={id}
--
-- Also adds pdf_blob_path to silver.nexudus_coworker_invoices for the
-- Nexudus invoice PDF caching feature.
-- =============================================================================

-- ── Bronze ──────────────────────────────────────────────────────────────────

IF OBJECT_ID('bronze.nexudus_coworker_invoice_lines', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.nexudus_coworker_invoice_lines (
        id                  BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id         UNIQUEIDENTIFIER    NOT NULL,
        source_id           BIGINT              NOT NULL,
        invoice_source_id   BIGINT              NULL,
        raw_json            NVARCHAR(MAX)       NOT NULL,
        synced_at           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_bronze_nexudus_coworker_invoice_lines_source UNIQUE (source_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_bronze_nexudus_coworker_invoice_lines_invoice'
      AND object_id = OBJECT_ID('bronze.nexudus_coworker_invoice_lines')
)
BEGIN
    CREATE INDEX ix_bronze_nexudus_coworker_invoice_lines_invoice
        ON bronze.nexudus_coworker_invoice_lines (invoice_source_id);
END
GO

-- ── Silver ──────────────────────────────────────────────────────────────────

IF OBJECT_ID('silver.nexudus_coworker_invoice_lines', 'U') IS NULL
BEGIN
    CREATE TABLE silver.nexudus_coworker_invoice_lines (
        id                          BIGINT              IDENTITY(1,1) PRIMARY KEY,
        source_id                   BIGINT              NOT NULL,
        CONSTRAINT uq_silver_nexudus_coworker_invoice_lines_source UNIQUE (source_id),
        unique_id                   NVARCHAR(64)        NULL,
        bronze_id                   BIGINT              NULL,
        sync_run_id                 UNIQUEIDENTIFIER    NULL,
        invoice_source_id           BIGINT              NULL,
        coworker_id                 BIGINT              NULL,
        location_source_id          BIGINT              NULL,
        description                 NVARCHAR(MAX)       NULL,
        financial_account_code      NVARCHAR(64)        NULL,
        financial_account_name      NVARCHAR(512)       NULL,
        quantity                    DECIMAL(12,4)       NULL,
        unit_price                  DECIMAL(12,2)       NULL,
        sub_total                   DECIMAL(12,2)       NULL,
        tax_amount                  DECIMAL(12,2)       NULL,
        total_amount                DECIMAL(12,2)       NULL,
        currency_code               NVARCHAR(8)         NULL,
        tax_category_name           NVARCHAR(256)       NULL,
        product_name                NVARCHAR(512)       NULL,
        created_on                  DATETIME2           NULL,
        updated_on                  DATETIME2           NULL,
        first_seen_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at              DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworker_invoice_lines_invoice'
      AND object_id = OBJECT_ID('silver.nexudus_coworker_invoice_lines')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworker_invoice_lines_invoice
        ON silver.nexudus_coworker_invoice_lines (invoice_source_id);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_coworker_invoice_lines_account'
      AND object_id = OBJECT_ID('silver.nexudus_coworker_invoice_lines')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_coworker_invoice_lines_account
        ON silver.nexudus_coworker_invoice_lines (financial_account_code);
END
GO

-- ── Add pdf_blob_path to silver.nexudus_coworker_invoices ───────────────────

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('silver.nexudus_coworker_invoices')
      AND name = 'pdf_blob_path'
)
BEGIN
    ALTER TABLE silver.nexudus_coworker_invoices
        ADD pdf_blob_path NVARCHAR(1024) NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('silver.nexudus_coworker_invoices')
      AND name = 'pdf_cached_at'
)
BEGIN
    ALTER TABLE silver.nexudus_coworker_invoices
        ADD pdf_cached_at DATETIME2 NULL;
END
GO

-- Verification
SELECT COUNT(*) AS bronze_invoice_lines FROM bronze.nexudus_coworker_invoice_lines;
SELECT COUNT(*) AS silver_invoice_lines FROM silver.nexudus_coworker_invoice_lines;
