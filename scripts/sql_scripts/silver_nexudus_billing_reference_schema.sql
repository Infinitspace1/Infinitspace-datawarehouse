-- =============================================================================
-- silver_nexudus_billing_reference_schema.sql
--
-- Bronze + silver tables for two Nexudus reference entities:
--   1. tariffs            — silver.nexudus_contracts.tariff_id links here
--   2. financial_accounts — silver.nexudus_extra_services.financial_account_id
--                           AND every invoice line's financial_account_id link here
--
-- These are required by Phase 2 of the landlord dashboard rework: future-
-- revenue and future-occupancy views need to filter contracts through
-- tariff → financial_account.name LIKE '%membership fee%' so the same rule
-- as the invoice-based past revenue (Phase 1) applies on the contract side.
--
-- Both tables are small (typically <500 rows each — one row per defined
-- price plan / accounting category) so we don't bother with watermarking
-- on the silver side; the worker re-merges all rows every run.
--
-- Source endpoints (assumed — verify with Nexudus docs):
--   GET /api/billing/tariffs
--   GET /api/billing/financialaccounts
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC sp_executesql N'CREATE SCHEMA bronze';
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC sp_executesql N'CREATE SCHEMA silver';
GO


-- ── BRONZE: tariffs ─────────────────────────────────────────────────────────

IF OBJECT_ID('bronze.nexudus_tariffs', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.nexudus_tariffs (
        id              BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id     UNIQUEIDENTIFIER    NOT NULL,
        source_id       BIGINT              NOT NULL,
        raw_json        NVARCHAR(MAX)       NOT NULL,
        payload_hash    CHAR(64)            NULL,
        synced_at       DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_bronze_nexudus_tariffs_source UNIQUE (source_id)
    );
END
GO


-- ── BRONZE: financial_accounts ──────────────────────────────────────────────

IF OBJECT_ID('bronze.nexudus_financial_accounts', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.nexudus_financial_accounts (
        id              BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id     UNIQUEIDENTIFIER    NOT NULL,
        source_id       BIGINT              NOT NULL,
        raw_json        NVARCHAR(MAX)       NOT NULL,
        payload_hash    CHAR(64)            NULL,
        synced_at       DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_bronze_nexudus_financial_accounts_source UNIQUE (source_id)
    );
END
GO


-- ── SILVER: tariffs ─────────────────────────────────────────────────────────
-- Tariff = a named price plan in Nexudus. Each contract references one.
-- Connects to financial_accounts via financial_account_id so the dashboard
-- can ask "is this contract booked against a membership-fee account?".

IF OBJECT_ID('silver.nexudus_tariffs', 'U') IS NULL
BEGIN
    CREATE TABLE silver.nexudus_tariffs (
        id                          BIGINT              IDENTITY(1,1) PRIMARY KEY,
        source_id                   BIGINT              NOT NULL,
        CONSTRAINT uq_silver_nexudus_tariffs_source UNIQUE (source_id),
        unique_id                   NVARCHAR(64)        NULL,           -- UniqueId GUID
        bronze_id                   BIGINT              NULL,
        sync_run_id                 UNIQUEIDENTIFIER    NULL,

        -- Identity
        name                        NVARCHAR(512)       NOT NULL,
        description                 NVARCHAR(MAX)       NULL,
        location_source_id          BIGINT              NULL,           -- BusinessId (tariffs can be scoped to one location)

        -- Pricing
        price                       DECIMAL(12,2)       NULL,
        currency_code               NVARCHAR(8)         NULL,
        signup_fee                  DECIMAL(12,2)       NULL,
        deposit                     DECIMAL(12,2)       NULL,
        included_credit_amount      DECIMAL(12,2)       NULL,
        time_credit_minutes         INT                 NULL,

        -- Billing cadence
        charge_period               INT                 NULL,           -- 0=monthly, 1=weekly, etc (Nexudus enum)
        billing_day                 TINYINT             NULL,
        term_duration_months        INT                 NULL,
        notice_period_days          INT                 NULL,

        -- Financial account linkage — THIS is the join key Phase 2 uses
        financial_account_id        BIGINT              NULL,

        -- Flags
        active                      BIT                 NOT NULL DEFAULT 1,
        visible                     BIT                 NULL,
        is_team_plan                BIT                 NULL,
        is_default                  BIT                 NULL,
        apply_pro_rating            BIT                 NULL,
        pro_rate_cancellation       BIT                 NULL,
        is_deleted                  BIT                 NOT NULL DEFAULT 0,

        -- Audit
        updated_by                  NVARCHAR(512)       NULL,
        created_on                  DATETIME2           NULL,
        updated_on                  DATETIME2           NULL,
        first_seen_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at              DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );

    CREATE INDEX ix_silver_nexudus_tariffs_financial_account
        ON silver.nexudus_tariffs (financial_account_id);
    CREATE INDEX ix_silver_nexudus_tariffs_location
        ON silver.nexudus_tariffs (location_source_id);
END
GO


-- ── SILVER: financial_accounts ──────────────────────────────────────────────
-- Financial account = a Nexudus accounting category, e.g. "Sales — Membership
-- Fee — Private Office" or "Sales — Parking". Tariffs reference one of these,
-- and invoice lines copy the name onto the line (financial_account_name).
-- The dashboard filters revenue/occupancy by whether the name matches
-- '%membership fee%'.

IF OBJECT_ID('silver.nexudus_financial_accounts', 'U') IS NULL
BEGIN
    CREATE TABLE silver.nexudus_financial_accounts (
        id                          BIGINT              IDENTITY(1,1) PRIMARY KEY,
        source_id                   BIGINT              NOT NULL,
        CONSTRAINT uq_silver_nexudus_financial_accounts_source UNIQUE (source_id),
        unique_id                   NVARCHAR(64)        NULL,
        bronze_id                   BIGINT              NULL,
        sync_run_id                 UNIQUEIDENTIFIER    NULL,

        -- Identity
        name                        NVARCHAR(512)       NOT NULL,
        code                        NVARCHAR(128)       NULL,           -- accounting code (e.g. "SALES-MF-PO")
        description                 NVARCHAR(MAX)       NULL,
        location_source_id          BIGINT              NULL,           -- BusinessId if scoped

        -- Classification
        account_type                NVARCHAR(64)        NULL,           -- "Revenue", "Expense", etc
        currency_code               NVARCHAR(8)         NULL,

        -- Flags
        active                      BIT                 NOT NULL DEFAULT 1,
        is_deleted                  BIT                 NOT NULL DEFAULT 0,

        -- Audit
        updated_by                  NVARCHAR(512)       NULL,
        created_on                  DATETIME2           NULL,
        updated_on                  DATETIME2           NULL,
        first_seen_at               DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at              DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );

    -- Frequently filtered on name (case-insensitive LIKE %membership fee%)
    CREATE INDEX ix_silver_nexudus_financial_accounts_name
        ON silver.nexudus_financial_accounts (name);
END
GO


-- ── Verification ────────────────────────────────────────────────────────────
SELECT COUNT(*) AS bronze_tariffs              FROM bronze.nexudus_tariffs;
SELECT COUNT(*) AS bronze_financial_accounts   FROM bronze.nexudus_financial_accounts;
SELECT COUNT(*) AS silver_tariffs              FROM silver.nexudus_tariffs;
SELECT COUNT(*) AS silver_financial_accounts   FROM silver.nexudus_financial_accounts;


-- Aggregate classification across ALL active contracts — the headline number
SELECT
    CASE
        WHEN LOWER(fa.name) LIKE '%membership fee%' THEN 'MATCH (counts for revenue)'
        WHEN fa.name IS NOT NULL                    THEN 'other account (excluded — parking/admin/etc)'
        WHEN t.source_id IS NULL                    THEN 'unresolved tariff'
        ELSE                                              'tariff without financial account'
    END                                        AS classification,
    COUNT(*)                                   AS contract_count,
    SUM(COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0)) AS monthly_total
FROM silver.nexudus_contracts c
LEFT JOIN silver.nexudus_tariffs t              ON t.source_id  = c.tariff_id
LEFT JOIN silver.nexudus_financial_accounts fa  ON fa.source_id = t.financial_account_id
WHERE c.active = 1
  AND c.is_deleted = 0
GROUP BY
    CASE
        WHEN LOWER(fa.name) LIKE '%membership fee%' THEN 'MATCH (counts for revenue)'
        WHEN fa.name IS NOT NULL                    THEN 'other account (excluded — parking/admin/etc)'
        WHEN t.source_id IS NULL                    THEN 'unresolved tariff'
        ELSE                                              'tariff without financial account'
    END
ORDER BY contract_count DESC;