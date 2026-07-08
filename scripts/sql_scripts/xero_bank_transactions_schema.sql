-- =============================================================================
-- xero_bank_transactions_schema.sql
--
-- Creates the bronze and silver tables for the Xero bank transactions sync
-- (spend/receive money — bank fees, Revolut merchant fees, direct debits...
-- these never appear on ACCPAY invoices, so the invoice sync alone leaves a
-- structural hole in P&L actuals).
--
--   bronze.xero_bank_transactions              Raw payloads (upsert on tenant+source_id)
--   silver.xero_bank_transactions              Typed headers (upsert on tenant+source_id)
--   silver.xero_bank_transaction_line_items    Line items (DELETE + INSERT per transaction)
--   silver.vw_xero_bank_transaction_pnl_lines  P&L serving view — the budget-tool shape:
--                                              tenant, date, account_code, net-of-tax amount,
--                                              status, type (SPEND/RECEIVE only)
--   meta.xero_tenants                          + per-tenant bank-transaction sync watermarks
--
-- Mirrors xero_invoices_schema.sql. Idempotent; apply with:
--   python scripts/python_scripts/apply_schema_script.py scripts/sql_scripts/xero_bank_transactions_schema.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.xero_bank_transactions
-- -----------------------------------------------------------------------------
IF OBJECT_ID('bronze.xero_bank_transactions', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.xero_bank_transactions (
        id                  BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id         UNIQUEIDENTIFIER    NOT NULL,
        xero_connection_id  BIGINT              NOT NULL,
        xero_tenant_id      NVARCHAR(128)       NOT NULL,
        source_id           NVARCHAR(128)       NOT NULL,
        raw_json            NVARCHAR(MAX)       NOT NULL,
        synced_at           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_bronze_xero_bank_transactions_tenant_source
            UNIQUE (xero_tenant_id, source_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_bronze_xero_bank_transactions_synced_at'
      AND object_id = OBJECT_ID('bronze.xero_bank_transactions')
)
BEGIN
    CREATE INDEX ix_bronze_xero_bank_transactions_synced_at
        ON bronze.xero_bank_transactions (synced_at);
END
GO

-- -----------------------------------------------------------------------------
-- silver.xero_bank_transactions (headers)
-- -----------------------------------------------------------------------------
IF OBJECT_ID('silver.xero_bank_transactions', 'U') IS NULL
BEGIN
    CREATE TABLE silver.xero_bank_transactions (
        id                      BIGINT              IDENTITY(1,1) PRIMARY KEY,
        bronze_id               BIGINT              NULL,
        sync_run_id             UNIQUEIDENTIFIER    NULL,
        xero_connection_id      BIGINT              NOT NULL,
        xero_tenant_id          NVARCHAR(128)       NOT NULL,
        source_id               NVARCHAR(128)       NOT NULL,
        CONSTRAINT uq_silver_xero_bank_transactions_tenant_source
            UNIQUE (xero_tenant_id, source_id),
        -- SPEND / RECEIVE / SPEND-PREPAYMENT / SPEND-OVERPAYMENT /
        -- RECEIVE-PREPAYMENT / RECEIVE-OVERPAYMENT / SPEND-TRANSFER / RECEIVE-TRANSFER
        transaction_type        NVARCHAR(32)        NULL,
        -- AUTHORISED / DELETED (deletions arrive as a status flip, like invoices)
        transaction_status      NVARCHAR(32)        NULL,
        is_reconciled           BIT                 NOT NULL DEFAULT 0,
        bank_account_id         NVARCHAR(128)       NULL,
        bank_account_code       NVARCHAR(64)        NULL,
        bank_account_name       NVARCHAR(255)       NULL,
        contact_id              NVARCHAR(128)       NULL,
        contact_name            NVARCHAR(512)       NULL,
        reference               NVARCHAR(512)       NULL,
        url                     NVARCHAR(2048)      NULL,
        currency_code           NVARCHAR(8)         NULL,
        currency_rate           DECIMAL(18,6)       NULL,
        line_amount_types       NVARCHAR(32)        NULL,
        transaction_date        DATE                NULL,
        updated_date_utc        DATETIME2           NULL,
        sub_total               DECIMAL(12,2)       NULL,
        total_tax               DECIMAL(12,2)       NULL,
        total                   DECIMAL(12,2)       NULL,
        has_attachments         BIT                 NOT NULL DEFAULT 0,
        first_seen_at           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at          DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_bank_transactions_tenant_id'
      AND object_id = OBJECT_ID('silver.xero_bank_transactions')
)
BEGIN
    CREATE INDEX ix_silver_xero_bank_transactions_tenant_id
        ON silver.xero_bank_transactions (xero_tenant_id);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_bank_transactions_date'
      AND object_id = OBJECT_ID('silver.xero_bank_transactions')
)
BEGIN
    CREATE INDEX ix_silver_xero_bank_transactions_date
        ON silver.xero_bank_transactions (transaction_date DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_bank_transactions_status_type'
      AND object_id = OBJECT_ID('silver.xero_bank_transactions')
)
BEGIN
    CREATE INDEX ix_silver_xero_bank_transactions_status_type
        ON silver.xero_bank_transactions (transaction_status, transaction_type);
END
GO

-- -----------------------------------------------------------------------------
-- silver.xero_bank_transaction_line_items (same shape as xero_invoice_line_items)
-- -----------------------------------------------------------------------------
IF OBJECT_ID('silver.xero_bank_transaction_line_items', 'U') IS NULL
BEGIN
    CREATE TABLE silver.xero_bank_transaction_line_items (
        id                          BIGINT              IDENTITY(1,1) PRIMARY KEY,
        xero_tenant_id              NVARCHAR(128)       NOT NULL,
        bank_transaction_source_id  NVARCHAR(128)       NOT NULL,
        line_item_index             INT                 NOT NULL,
        description                 NVARCHAR(MAX)       NULL,
        item_code                   NVARCHAR(255)       NULL,
        account_id                  NVARCHAR(128)       NULL,
        account_code                NVARCHAR(64)        NULL,
        tax_type                    NVARCHAR(64)        NULL,
        tracking_json               NVARCHAR(MAX)       NULL,
        quantity                    DECIMAL(18,4)       NULL,
        unit_amount                 DECIMAL(18,4)       NULL,
        line_amount                 DECIMAL(12,2)       NULL,
        tax_amount                  DECIMAL(12,2)       NULL,
        discount_rate               DECIMAL(18,4)       NULL,
        raw_json                    NVARCHAR(MAX)       NOT NULL
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_bank_tx_line_items_txn'
      AND object_id = OBJECT_ID('silver.xero_bank_transaction_line_items')
)
BEGIN
    CREATE INDEX ix_silver_xero_bank_tx_line_items_txn
        ON silver.xero_bank_transaction_line_items (xero_tenant_id, bank_transaction_source_id);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_bank_tx_line_items_account_code'
      AND object_id = OBJECT_ID('silver.xero_bank_transaction_line_items')
)
BEGIN
    CREATE INDEX ix_silver_xero_bank_tx_line_items_account_code
        ON silver.xero_bank_transaction_line_items (xero_tenant_id, account_code);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_silver_xero_bank_tx_line_items_account_id'
      AND object_id = OBJECT_ID('silver.xero_bank_transaction_line_items')
)
BEGIN
    CREATE INDEX ix_silver_xero_bank_tx_line_items_account_id
        ON silver.xero_bank_transaction_line_items (xero_tenant_id, account_id);
END
GO

-- -----------------------------------------------------------------------------
-- meta.xero_tenants: per-tenant bank-transaction sync watermarks
-- -----------------------------------------------------------------------------
IF COL_LENGTH('meta.xero_tenants', 'last_bank_transaction_sync_started_at') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_bank_transaction_sync_started_at DATETIME2 NULL;
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_bank_transaction_sync_completed_at') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_bank_transaction_sync_completed_at DATETIME2 NULL;
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_bank_transaction_modified_utc') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_bank_transaction_modified_utc DATETIME2 NULL;
END
GO

IF COL_LENGTH('meta.xero_tenants', 'last_bank_transaction_sync_error') IS NULL
BEGIN
    ALTER TABLE meta.xero_tenants
    ADD last_bank_transaction_sync_error NVARCHAR(1024) NULL;
END
GO

-- -----------------------------------------------------------------------------
-- silver.vw_xero_bank_transaction_pnl_lines
--
-- P&L serving view for the budget tool. One row per line item, with the exact
-- requested shape: xero_tenant_id, [date], account_code, line_amount_net
-- (net of tax), [status], [type].
--
-- Rules:
--   * Only SPEND / RECEIVE — prepayment/overpayment variants are excluded
--     (they hit balance-sheet accounts and are later allocated to invoices:
--     summing them alongside ACCPAY invoice lines would double-count), and
--     *-TRANSFER types are excluded (bank-to-bank moves, no P&L impact).
--   * Only AUTHORISED — DELETED transactions drop out automatically.
--   * line_amount_net: Xero's LineAmount includes tax when the transaction is
--     LineAmountTypes='Inclusive' (the common case for bank-feed entries), so
--     the net amount subtracts tax_amount in that case.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW silver.vw_xero_bank_transaction_pnl_lines AS
SELECT
    li.xero_tenant_id,
    bt.transaction_date                             AS [date],
    li.account_code,
    li.account_id,
    COALESCE(ac.account_name, ai.account_name)     AS account_name,
    COALESCE(ac.account_type, ai.account_type)     AS account_type,
    COALESCE(ac.account_class, ai.account_class)   AS account_class,
    CASE WHEN bt.line_amount_types = 'Inclusive'
         THEN li.line_amount - COALESCE(li.tax_amount, 0)
         ELSE li.line_amount
    END                                             AS line_amount_net,
    li.line_amount,
    li.tax_amount,
    li.tax_type,
    li.description,
    li.quantity,
    li.unit_amount,
    bt.transaction_status                           AS [status],
    bt.transaction_type                             AS [type],
    bt.is_reconciled,
    bt.contact_name,
    bt.bank_account_code,
    bt.bank_account_name,
    bt.currency_code,
    bt.currency_rate,
    bt.line_amount_types,
    bt.reference,
    bt.source_id                                    AS bank_transaction_source_id,
    li.line_item_index,
    bt.updated_date_utc
FROM silver.xero_bank_transaction_line_items li
JOIN silver.xero_bank_transactions bt
  ON bt.xero_tenant_id = li.xero_tenant_id
 AND bt.source_id = li.bank_transaction_source_id
LEFT JOIN silver.xero_accounts ac
  ON ac.xero_tenant_id = li.xero_tenant_id
 AND ac.account_code = li.account_code
LEFT JOIN silver.xero_accounts ai
  ON ai.xero_tenant_id = li.xero_tenant_id
 AND ai.source_id = li.account_id
WHERE bt.transaction_status = 'AUTHORISED'
  AND bt.transaction_type IN ('SPEND', 'RECEIVE');
GO
