-- ============================================================
-- 02_bronze.sql
-- Bronze layer: raw ingestion from all sources.
--
-- Design rules:
--   - One table per entity per source
--   - Naming: bronze.{source}_{entity}
--   - Always store full raw JSON
--   - Never update rows — always INSERT (append-only)
--   - sync_run_id ties all rows from one pipeline run together
--   - Retain indefinitely (or partition/archive by synced_at)
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- NEXUDUS
-- ──────────────────────────────────────────────────────────────

CREATE TABLE bronze.nexudus_locations (
    id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id     UNIQUEIDENTIFIER NOT NULL,
    source_id       BIGINT          NOT NULL,       -- Nexudus Id field
    raw_json        NVARCHAR(MAX)   NOT NULL,
    synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_locations_source_id  ON bronze.nexudus_locations (source_id);
CREATE INDEX ix_bronze_nexudus_locations_sync_run   ON bronze.nexudus_locations (sync_run_id);
CREATE INDEX ix_bronze_nexudus_locations_synced_at  ON bronze.nexudus_locations (synced_at);
GO

-- ──────────────────────────────────────────────────
-- Products = FloorPlanDesks in Nexudus
-- (Private Offices, Hot Desks, Dedicated Desks, etc.)
-- ──────────────────────────────────────────────────
CREATE TABLE bronze.nexudus_products (
    id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id     UNIQUEIDENTIFIER NOT NULL,
    source_id       BIGINT          NOT NULL,       -- FloorPlanDesk Id
    location_id     BIGINT          NULL,           -- FloorPlanBusinessId (denorm for easy filtering)
    item_type       INT             NULL,           -- ItemType (1=PrivateOffice, 2=HotDesk, etc.)
    raw_json        NVARCHAR(MAX)   NOT NULL,
    synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_products_source_id  ON bronze.nexudus_products (source_id);
CREATE INDEX ix_bronze_nexudus_products_location   ON bronze.nexudus_products (location_id);
CREATE INDEX ix_bronze_nexudus_products_sync_run   ON bronze.nexudus_products (sync_run_id);
CREATE INDEX ix_bronze_nexudus_products_synced_at  ON bronze.nexudus_products (synced_at);
GO

CREATE TABLE bronze.nexudus_contracts (
    id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id     UNIQUEIDENTIFIER NOT NULL,
    source_id       BIGINT          NOT NULL,       -- CoworkerContract Id
    product_id      BIGINT          NULL,           -- FloorPlanDesk Id (denorm)
    location_id     BIGINT          NULL,           -- FloorPlanBusinessId (denorm)
    raw_json        NVARCHAR(MAX)   NOT NULL,
    synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_contracts_source_id ON bronze.nexudus_contracts (source_id);
CREATE INDEX ix_bronze_nexudus_contracts_product   ON bronze.nexudus_contracts (product_id);
CREATE INDEX ix_bronze_nexudus_contracts_location  ON bronze.nexudus_contracts (location_id);
CREATE INDEX ix_bronze_nexudus_contracts_sync_run  ON bronze.nexudus_contracts (sync_run_id);
CREATE INDEX ix_bronze_nexudus_contracts_synced_at ON bronze.nexudus_contracts (synced_at);
GO

-- ──────────────────────────────────────────────────
-- Resources = meeting rooms / phone booths etc.
-- Fetched via GET /spaces/resources/{id}
-- ──────────────────────────────────────────────────
CREATE TABLE bronze.nexudus_resources (
    id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id     UNIQUEIDENTIFIER NOT NULL,
    source_id       BIGINT          NOT NULL,       -- Resource Id
    location_id     BIGINT          NULL,           -- BusinessId (denorm)
    raw_json        NVARCHAR(MAX)   NOT NULL,
    synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_resources_source_id ON bronze.nexudus_resources (source_id);
CREATE INDEX ix_bronze_nexudus_resources_sync_run  ON bronze.nexudus_resources (sync_run_id);
CREATE INDEX ix_bronze_nexudus_resources_synced_at ON bronze.nexudus_resources (synced_at);
GO

-- ──────────────────────────────────────────────────
-- Extra Services = day passes, meeting room rates
-- Fetched via GET /billing/extraservices
-- ──────────────────────────────────────────────────
CREATE TABLE bronze.nexudus_extra_services (
    id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id     UNIQUEIDENTIFIER NOT NULL,
    source_id       BIGINT          NOT NULL,       -- ExtraService Id
    location_id     BIGINT          NULL,           -- BusinessId (denorm)
    raw_json        NVARCHAR(MAX)   NOT NULL,
    synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_extra_services_source_id ON bronze.nexudus_extra_services (source_id);
CREATE INDEX ix_bronze_nexudus_extra_services_location  ON bronze.nexudus_extra_services (location_id);
CREATE INDEX ix_bronze_nexudus_extra_services_sync_run  ON bronze.nexudus_extra_services (sync_run_id);
CREATE INDEX ix_bronze_nexudus_extra_services_synced_at ON bronze.nexudus_extra_services (synced_at);
GO


-- ──────────────────────────────────────────────────────────────
-- HUBSPOT (placeholder — add columns as you discover the shape)
-- ──────────────────────────────────────────────────────────────

-- CREATE TABLE bronze.hubspot_contacts (
--     id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
--     sync_run_id     UNIQUEIDENTIFIER NOT NULL,
--     source_id       NVARCHAR(64)    NOT NULL,       -- HubSpot object id (string)
--     raw_json        NVARCHAR(MAX)   NOT NULL,
--     synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
-- );

-- CREATE INDEX ix_bronze_hubspot_contacts_source_id ON bronze.hubspot_contacts (source_id);
-- CREATE INDEX ix_bronze_hubspot_contacts_sync_run  ON bronze.hubspot_contacts (sync_run_id);
-- GO

-- CREATE TABLE bronze.hubspot_companies (
--     id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
--     sync_run_id     UNIQUEIDENTIFIER NOT NULL,
--     source_id       NVARCHAR(64)    NOT NULL,
--     raw_json        NVARCHAR(MAX)   NOT NULL,
--     synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
-- );

-- CREATE INDEX ix_bronze_hubspot_companies_source_id ON bronze.hubspot_companies (source_id);
-- CREATE INDEX ix_bronze_hubspot_companies_sync_run  ON bronze.hubspot_companies (sync_run_id);
-- GO

-- CREATE TABLE bronze.hubspot_deals (
--     id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
--     sync_run_id     UNIQUEIDENTIFIER NOT NULL,
--     source_id       NVARCHAR(64)    NOT NULL,
--     raw_json        NVARCHAR(MAX)   NOT NULL,
--     synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
-- );

-- CREATE INDEX ix_bronze_hubspot_deals_source_id ON bronze.hubspot_deals (source_id);
-- CREATE INDEX ix_bronze_hubspot_deals_sync_run  ON bronze.hubspot_deals (sync_run_id);
-- GO


-- ──────────────────────────────────────────────────────────────
-- GOOGLE DRIVE / ONEDRIVE (placeholder)
-- File metadata only — actual file content stays in Blob Storage
-- ──────────────────────────────────────────────────────────────

-- CREATE TABLE bronze.gdrive_files (
--     id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
--     sync_run_id     UNIQUEIDENTIFIER NOT NULL,
--     source_id       NVARCHAR(256)   NOT NULL,       -- Google file id
--     raw_json        NVARCHAR(MAX)   NOT NULL,
--     blob_path       NVARCHAR(1024)  NULL,           -- path in Azure Blob if content stored
--     synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
-- );

-- CREATE INDEX ix_bronze_gdrive_files_source_id ON bronze.gdrive_files (source_id);
-- CREATE INDEX ix_bronze_gdrive_files_sync_run  ON bronze.gdrive_files (sync_run_id);
-- GO

-- CREATE TABLE bronze.onedrive_files (
--     id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
--     sync_run_id     UNIQUEIDENTIFIER NOT NULL,
--     source_id       NVARCHAR(256)   NOT NULL,       -- OneDrive item id
--     raw_json        NVARCHAR(MAX)   NOT NULL,
--     blob_path       NVARCHAR(1024)  NULL,
--     synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
-- );

-- CREATE INDEX ix_bronze_onedrive_files_source_id ON bronze.onedrive_files (source_id);
-- CREATE INDEX ix_bronze_onedrive_files_sync_run  ON bronze.onedrive_files (sync_run_id);
GO