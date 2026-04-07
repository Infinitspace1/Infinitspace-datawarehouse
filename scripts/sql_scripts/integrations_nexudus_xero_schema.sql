-- =============================================================================
-- integrations_nexudus_xero_schema.sql
--
-- Extends the existing warehouse schemas using the current project conventions:
--   - bronze: raw third-party payloads
--   - silver: typed, queryable Nexudus entities
--   - meta: operational integration state / OAuth persistence
--
-- This script intentionally reuses silver.nexudus_locations rather than
-- creating a duplicate locations table.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.nexudus_coworkers
-- Raw coworker payloads fetched from GET /api/spaces/coworkers/{id}
-- -----------------------------------------------------------------------------
IF OBJECT_ID('bronze.nexudus_coworkers', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.nexudus_coworkers (
        id              BIGINT              IDENTITY(1,1) PRIMARY KEY,
        sync_run_id     UNIQUEIDENTIFIER    NOT NULL,
        source_id       BIGINT              NOT NULL,
        raw_json        NVARCHAR(MAX)       NOT NULL,
        synced_at       DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_bronze_nexudus_coworkers_source_id'
      AND object_id = OBJECT_ID('bronze.nexudus_coworkers')
)
BEGIN
    CREATE INDEX ix_bronze_nexudus_coworkers_source_id
        ON bronze.nexudus_coworkers (source_id);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_bronze_nexudus_coworkers_sync_run'
      AND object_id = OBJECT_ID('bronze.nexudus_coworkers')
)
BEGIN
    CREATE INDEX ix_bronze_nexudus_coworkers_sync_run
        ON bronze.nexudus_coworkers (sync_run_id);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_bronze_nexudus_coworkers_synced_at'
      AND object_id = OBJECT_ID('bronze.nexudus_coworkers')
)
BEGIN
    CREATE INDEX ix_bronze_nexudus_coworkers_synced_at
        ON bronze.nexudus_coworkers (synced_at);
END
GO

-- -----------------------------------------------------------------------------
-- silver.nexudus_colleagues
-- Normalized coworker rows. Uses source_id like the rest of silver.*.
-- -----------------------------------------------------------------------------
IF OBJECT_ID('silver.nexudus_colleagues', 'U') IS NULL
BEGIN
    CREATE TABLE silver.nexudus_colleagues (
        id                  BIGINT              IDENTITY(1,1) PRIMARY KEY,
        source_id           BIGINT              NOT NULL,
        CONSTRAINT uq_silver_nexudus_colleagues_source_id UNIQUE (source_id),
        bronze_id           BIGINT              NULL,
        sync_run_id         UNIQUEIDENTIFIER    NULL,
        team_business_id    BIGINT              NULL,
        full_name           NVARCHAR(512)       NULL,
        first_name          NVARCHAR(255)       NULL,
        last_name           NVARCHAR(255)       NULL,
        email               NVARCHAR(512)       NULL,
        status              NVARCHAR(128)       NULL,
        first_seen_at       DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at      DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_colleagues_email'
      AND object_id = OBJECT_ID('silver.nexudus_colleagues')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_colleagues_email
        ON silver.nexudus_colleagues (email);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_colleagues_team_business_id'
      AND object_id = OBJECT_ID('silver.nexudus_colleagues')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_colleagues_team_business_id
        ON silver.nexudus_colleagues (team_business_id);
END
GO

-- -----------------------------------------------------------------------------
-- silver.nexudus_colleague_location_access
-- Soft link table:
--   colleague_source_id -> silver.nexudus_colleagues.source_id
--   location_source_id  -> silver.nexudus_locations.source_id
-- -----------------------------------------------------------------------------
IF OBJECT_ID('silver.nexudus_colleague_location_access', 'U') IS NULL
BEGIN
    CREATE TABLE silver.nexudus_colleague_location_access (
        id                      BIGINT              IDENTITY(1,1) PRIMARY KEY,
        colleague_source_id     BIGINT              NOT NULL,
        location_source_id      BIGINT              NOT NULL,
        source                  NVARCHAR(128)       NOT NULL
            CONSTRAINT df_silver_nexudus_colleague_location_access_source
            DEFAULT 'nexudus_coworker_business_access',
        first_seen_at           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at          DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_silver_nexudus_colleague_location_access
            UNIQUE (colleague_source_id, location_source_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_colleague_location_access_location'
      AND object_id = OBJECT_ID('silver.nexudus_colleague_location_access')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_colleague_location_access_location
        ON silver.nexudus_colleague_location_access (location_source_id);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_nexudus_colleague_location_access_colleague'
      AND object_id = OBJECT_ID('silver.nexudus_colleague_location_access')
)
BEGIN
    CREATE INDEX ix_silver_nexudus_colleague_location_access_colleague
        ON silver.nexudus_colleague_location_access (colleague_source_id);
END
GO

-- -----------------------------------------------------------------------------
-- meta.xero_connections
-- Operational app metadata, not warehouse facts.
-- -----------------------------------------------------------------------------
IF OBJECT_ID('meta.xero_connections', 'U') IS NULL
BEGIN
    CREATE TABLE meta.xero_connections (
        id                          BIGINT              IDENTITY(1,1) PRIMARY KEY,
        owner_type                  NVARCHAR(64)        NULL,
        owner_id                    NVARCHAR(128)       NULL,
        xero_user_id                NVARCHAR(128)       NULL,
        access_token_encrypted      NVARCHAR(MAX)       NOT NULL,
        refresh_token_encrypted     NVARCHAR(MAX)       NOT NULL,
        id_token_encrypted          NVARCHAR(MAX)       NULL,
        scope                       NVARCHAR(1024)      NULL,
        token_type                  NVARCHAR(64)        NULL,
        expires_at                  DATETIME2           NOT NULL,
        selected_xero_tenant_id     NVARCHAR(128)       NULL,
        is_connected                BIT                 NOT NULL DEFAULT 1,
        last_error                  NVARCHAR(1024)      NULL,
        raw_token_payload           NVARCHAR(MAX)       NULL,
        created_at                  DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        updated_at                  DATETIME2           NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'uq_meta_xero_connections_owner'
      AND object_id = OBJECT_ID('meta.xero_connections')
)
BEGIN
    CREATE UNIQUE INDEX uq_meta_xero_connections_owner
        ON meta.xero_connections (owner_type, owner_id)
        WHERE owner_type IS NOT NULL AND owner_id IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_meta_xero_connections_selected_tenant'
      AND object_id = OBJECT_ID('meta.xero_connections')
)
BEGIN
    CREATE INDEX ix_meta_xero_connections_selected_tenant
        ON meta.xero_connections (selected_xero_tenant_id);
END
GO

-- -----------------------------------------------------------------------------
-- meta.xero_tenants
-- -----------------------------------------------------------------------------
IF OBJECT_ID('meta.xero_tenants', 'U') IS NULL
BEGIN
    CREATE TABLE meta.xero_tenants (
        id                      BIGINT              IDENTITY(1,1) PRIMARY KEY,
        xero_connection_id      BIGINT              NOT NULL,
        xero_tenant_id          NVARCHAR(128)       NOT NULL,
        xero_tenant_type        NVARCHAR(64)        NULL,
        tenant_name             NVARCHAR(512)       NULL,
        connection_id           NVARCHAR(128)       NULL,
        auth_event_id           NVARCHAR(128)       NULL,
        raw_payload             NVARCHAR(MAX)       NULL,
        created_at              DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        updated_at              DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT fk_meta_xero_tenants_connection
            FOREIGN KEY (xero_connection_id) REFERENCES meta.xero_connections(id),
        CONSTRAINT uq_meta_xero_tenants_connection_tenant
            UNIQUE (xero_connection_id, xero_tenant_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_meta_xero_tenants_tenant_name'
      AND object_id = OBJECT_ID('meta.xero_tenants')
)
BEGIN
    CREATE INDEX ix_meta_xero_tenants_tenant_name
        ON meta.xero_tenants (tenant_name);
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

-- -----------------------------------------------------------------------------
-- silver.xero_tenants
-- Refreshable tenant directory that combines Xero tenant metadata with the
-- best matching silver.nexudus_locations row.
-- -----------------------------------------------------------------------------
IF OBJECT_ID('silver.xero_tenants', 'U') IS NULL
BEGIN
    CREATE TABLE silver.xero_tenants (
        id                      BIGINT              IDENTITY(1,1) PRIMARY KEY,
        xero_connection_id      BIGINT              NOT NULL,
        xero_tenant_id          NVARCHAR(128)       NOT NULL,
        xero_tenant_type        NVARCHAR(64)        NULL,
        tenant_name             NVARCHAR(512)       NULL,
        tenant_connection_id    NVARCHAR(128)       NULL,
        auth_event_id           NVARCHAR(128)       NULL,
        raw_tenant_payload      NVARCHAR(MAX)       NULL,
        location_source_id      BIGINT              NULL,
        location_nexudus_uuid   NVARCHAR(64)        NULL,
        location_name           NVARCHAR(512)       NULL,
        location_web_address    NVARCHAR(256)       NULL,
        location_address        NVARCHAR(1024)      NULL,
        location_postal_code    NVARCHAR(32)        NULL,
        location_city           NVARCHAR(255)       NULL,
        location_state          NVARCHAR(255)       NULL,
        location_country_name   NVARCHAR(128)       NULL,
        location_currency_code  NVARCHAR(8)         NULL,
        location_email          NVARCHAR(512)       NULL,
        location_web_contact    NVARCHAR(512)       NULL,
        community_manager_name  NVARCHAR(255)       NULL,
        location_match_rule     NVARCHAR(128)       NULL,
        first_seen_at           DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        last_synced_at          DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_silver_xero_tenants_connection_tenant
            UNIQUE (xero_connection_id, xero_tenant_id),
        CONSTRAINT fk_silver_xero_tenants_connection
            FOREIGN KEY (xero_connection_id) REFERENCES meta.xero_connections(id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_xero_tenants_location_source_id'
      AND object_id = OBJECT_ID('silver.xero_tenants')
)
BEGIN
    CREATE INDEX ix_silver_xero_tenants_location_source_id
        ON silver.xero_tenants (location_source_id);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_xero_tenants_tenant_name'
      AND object_id = OBJECT_ID('silver.xero_tenants')
)
BEGIN
    CREATE INDEX ix_silver_xero_tenants_tenant_name
        ON silver.xero_tenants (tenant_name);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_silver_xero_tenants_location_city'
      AND object_id = OBJECT_ID('silver.xero_tenants')
)
BEGIN
    CREATE INDEX ix_silver_xero_tenants_location_city
        ON silver.xero_tenants (location_city);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'xero'
)
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA xero';
END
GO

EXEC sp_executesql N'
CREATE OR ALTER VIEW xero.silver_tenants AS
SELECT
    id,
    xero_connection_id,
    xero_tenant_id,
    xero_tenant_type,
    tenant_name,
    tenant_connection_id,
    auth_event_id,
    raw_tenant_payload,
    location_source_id,
    location_nexudus_uuid,
    location_name,
    location_web_address,
    location_address,
    location_postal_code,
    location_city,
    location_state,
    location_country_name,
    location_currency_code,
    location_email,
    location_web_contact,
    community_manager_name,
    location_match_rule,
    first_seen_at,
    last_synced_at
FROM silver.xero_tenants;
';
GO

-- -----------------------------------------------------------------------------
-- meta.xero_oauth_states
-- -----------------------------------------------------------------------------
IF OBJECT_ID('meta.xero_oauth_states', 'U') IS NULL
BEGIN
    CREATE TABLE meta.xero_oauth_states (
        id              BIGINT              IDENTITY(1,1) PRIMARY KEY,
        state           NVARCHAR(255)       NOT NULL,
        owner_type      NVARCHAR(64)        NULL,
        owner_id        NVARCHAR(128)       NULL,
        expires_at      DATETIME2           NOT NULL,
        consumed_at     DATETIME2           NULL,
        created_at      DATETIME2           NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT uq_meta_xero_oauth_states_state UNIQUE (state)
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ix_meta_xero_oauth_states_expires_at'
      AND object_id = OBJECT_ID('meta.xero_oauth_states')
)
BEGIN
    CREATE INDEX ix_meta_xero_oauth_states_expires_at
        ON meta.xero_oauth_states (expires_at);
END
GO


SELECT COUNT(1) FROM silver.xero_invoices