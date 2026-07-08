-- =============================================================================
-- xero_profit_loss_schema.sql
--
-- Creates the bronze and silver tables for monthly Xero Profit & Loss reports.
--
--   bronze.xero_profit_loss_reports      Raw monthly report payloads
--   silver.xero_profit_loss_accounts     Flattened account + summary rows
--   meta.xero_tenants                    + per-tenant P&L sync observability
--
-- Idempotent; apply with:
--   python scripts/python_scripts/apply_schema_script.py scripts/sql_scripts/xero_profit_loss_schema.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.xero_profit_loss_reports
-- -----------------------------------------------------------------------------
IF OBJECT_ID('bronze.xero_profit_loss_reports', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.xero_profit_loss_reports (
        id                      BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id             UNIQUEIDENTIFIER    NOT NULL,
        xero_connection_id      BIGINT              NOT NULL,
        xero_tenant_id          NVARCHAR(128)       NOT NULL,
        period_month            DATE                NOT NULL,
        from_date               DATE                NOT NULL,
        to_date                 DATE                NOT NULL,
        currency_code           NVARCHAR(8)         NULL,
        report_updated_utc      DATETIME2           NULL,
        report_titles_json      NVARCHAR(MAX)       NULL,
        raw_json                NVARCHAR(MAX)       NOT NULL,
        synced_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_bronze_xero_profit_loss_tenant_month
            UNIQUE (xero_tenant_id, period_month)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_bronze_xero_profit_loss_synced_at'
      AND object_id = OBJECT_ID('bronze.xero_profit_loss_reports')
)
BEGIN
    CREATE INDEX ix_bronze_xero_profit_loss_synced_at
        ON bronze.xero_profit_loss_reports (synced_at);
END
GO

-- -----------------------------------------------------------------------------
-- silver.xero_profit_loss_accounts
-- -----------------------------------------------------------------------------
IF OBJECT_ID('silver.xero_profit_loss_accounts', 'U') IS NULL
BEGIN
    CREATE TABLE silver.xero_profit_loss_accounts (
        id                      BIGINT              IDENTITY(1,1) PRIMARY KEY,
        bronze_id               BIGINT              NULL,
        sync_run_id             UNIQUEIDENTIFIER    NOT NULL,
        xero_connection_id      BIGINT              NOT NULL,
        xero_tenant_id          NVARCHAR(128)       NOT NULL,
        period_month            DATE                NOT NULL,
        from_date               DATE                NOT NULL,
        to_date                 DATE                NOT NULL,
        currency_code           NVARCHAR(8)         NULL,
        section                 NVARCHAR(255)       NULL,
        row_type                NVARCHAR(32)        NOT NULL,
        is_summary              BIT                 NOT NULL DEFAULT 0,
        row_order               INT                 NOT NULL,
        account_id              NVARCHAR(128)       NULL,
        account_code            NVARCHAR(64)        NULL,
        account_name            NVARCHAR(512)       NULL,
        amount                  DECIMAL(18,2)       NULL,
        report_updated_utc      DATETIME2           NULL,
        raw_row_json            NVARCHAR(MAX)       NOT NULL,
        last_synced_at          DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_profit_loss_tenant_month'
      AND object_id = OBJECT_ID('silver.xero_profit_loss_accounts')
)
BEGIN
    CREATE INDEX ix_silver_xero_profit_loss_tenant_month
        ON silver.xero_profit_loss_accounts (xero_tenant_id, period_month);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_profit_loss_account'
      AND object_id = OBJECT_ID('silver.xero_profit_loss_accounts')
)
BEGIN
    CREATE INDEX ix_silver_xero_profit_loss_account
        ON silver.xero_profit_loss_accounts (xero_tenant_id, account_id, period_month);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_profit_loss_section'
      AND object_id = OBJECT_ID('silver.xero_profit_loss_accounts')
)
BEGIN
    CREATE INDEX ix_silver_xero_profit_loss_section
        ON silver.xero_profit_loss_accounts (section, period_month);
END
GO

-- -----------------------------------------------------------------------------
-- meta.xero_tenants: per-tenant P&L sync observability
-- -----------------------------------------------------------------------------
IF COL_LENGTH('meta.xero_tenants', 'last_profit_loss_sync_started_at') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_profit_loss_sync_started_at DATETIME2 NULL;
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_profit_loss_sync_completed_at') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_profit_loss_sync_completed_at DATETIME2 NULL;
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_profit_loss_period_month') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_profit_loss_period_month DATE NULL;
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_profit_loss_sync_error') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_profit_loss_sync_error NVARCHAR(1024) NULL;
END
GO

-- -----------------------------------------------------------------------------
-- silver.vw_xero_profit_loss_monthly_accounts
--
-- Budget-tool friendly shape requested by finance. Includes account rows and
-- summary rows; summary rows have account_id/account_code NULL and is_summary=1.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW silver.vw_xero_profit_loss_monthly_accounts AS
SELECT
    xero_tenant_id,
    period_month,
    account_id,
    account_code,
    account_name,
    section,
    amount,
    currency_code AS currency,
    row_type,
    is_summary,
    row_order,
    from_date,
    to_date,
    report_updated_utc,
    last_synced_at
FROM silver.xero_profit_loss_accounts;
GO
