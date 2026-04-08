-- =============================================================================
-- xero_pdf_blob_migration.sql
--
-- Migrates bronze.xero_invoice_pdfs from storing raw bytes in SQL to storing
-- a blob path reference (the PDF lives in Azure Blob Storage xero-invoice-pdfs).
-- Also adds pdf_blob_path to silver.xero_invoices.
-- =============================================================================

-- Step 1: Migrate bronze.xero_invoice_pdfs
--   The table currently has pdf_bytes VARBINARY(MAX) NOT NULL.
--   We drop that column and add blob_path NVARCHAR(512) instead.
--   Safe to run even if the table is empty.

IF COL_LENGTH('bronze.xero_invoice_pdfs', 'pdf_bytes') IS NOT NULL
BEGIN
    -- Remove the NOT NULL constraint first (required before DROP COLUMN)
    ALTER TABLE bronze.xero_invoice_pdfs
    ALTER COLUMN pdf_bytes VARBINARY(MAX) NULL;

    ALTER TABLE bronze.xero_invoice_pdfs
    DROP COLUMN pdf_bytes;
END
GO

IF COL_LENGTH('bronze.xero_invoice_pdfs', 'blob_path') IS NULL
BEGIN
    ALTER TABLE bronze.xero_invoice_pdfs
    ADD blob_path NVARCHAR(512) NULL;
END
GO

-- Step 2: Add pdf_blob_path to silver.xero_invoices

IF COL_LENGTH('silver.xero_invoices', 'pdf_blob_path') IS NULL
BEGIN
    ALTER TABLE silver.xero_invoices
    ADD pdf_blob_path NVARCHAR(512) NULL;
END
GO

-- Verify
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'xero_invoice_pdfs'
ORDER BY ORDINAL_POSITION;

SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'xero_invoices'
  AND COLUMN_NAME IN ('pdf_cached_at', 'pdf_content_type', 'pdf_file_name', 'pdf_blob_path')
ORDER BY ORDINAL_POSITION;
