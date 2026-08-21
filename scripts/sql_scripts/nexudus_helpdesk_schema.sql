-- ============================================================
-- nexudus_helpdesk_schema.sql  (2026-08-20)
--
-- Nexudus help desk ("customer requests"): bronze + silver tables for
--   - help desk messages     (GET /api/support/helpdeskmessages)     = the tickets
--   - help desk comments     (GET /api/support/helpdeskcomments)     = the reply thread
--   - help desk departments  (GET /api/support/helpdeskdepartments)  = routing categories
--
-- Run once against the warehouse DB.
--
-- NB the API area is `support`, not the `community` you would guess from the
-- Nexudus admin nav.
--
-- Relationships:
--   silver.nexudus_helpdesk_messages.location_source_id
--       -> silver.nexudus_locations.source_id            (BusinessId)
--   silver.nexudus_helpdesk_messages.coworker_source_id
--       -> silver.nexudus_coworkers.source_id            (the requester)
--   silver.nexudus_helpdesk_messages.department_source_id
--       -> silver.nexudus_helpdesk_departments.source_id (NULL on ~3% of tickets)
--   silver.nexudus_helpdesk_comments.helpdesk_message_source_id
--       -> silver.nexudus_helpdesk_messages.source_id
--   silver.nexudus_helpdesk_comments.location_source_id
--       -> silver.nexudus_locations.source_id
--       (derived from the parent message — the comment payload has no BusinessId,
--        same trick as event_products inheriting from its calendar event)
--   silver.nexudus_helpdesk_departments.location_source_id
--       -> silver.nexudus_locations.source_id            (departments are per-location)
--
-- Ingestion is HYBRID (see functions/nexudus_helpdesk_sync.py and
-- functions/nexudus_helpdesk_webhook.py):
--   * webhook  -> Nexudus actions 45 (HelDeskMessageCreated — Nexudus's own
--                 typo) and 46 (HelpDeskCommentCreated) push new tickets and
--                 replies within seconds.
--   * poll     -> a 15-minute timer reconciles UPDATES. This is not optional:
--                 Nexudus has no update/close/assign webhook event anywhere in
--                 its 90-code eWebhookAction enum, so Closed / ClosedOn /
--                 OwnerId / FirstResponseTimeInMinutes would otherwise never
--                 change after a ticket is first created.
--
-- Soft delete: is_deleted/deleted_at maintained weekly by
-- nexudus_silver_reconcile. Downstream reads MUST filter is_deleted = 0.
-- ============================================================

-- ── Bronze ──────────────────────────────────────────────────

CREATE TABLE bronze.nexudus_helpdesk_messages (
    id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id     UNIQUEIDENTIFIER NOT NULL,
    source_id       BIGINT          NOT NULL,       -- HelpDeskMessage Id
    location_id     BIGINT          NULL,           -- BusinessId (denorm)
    coworker_id     BIGINT          NULL,           -- CoworkerId (denorm)
    department_id   BIGINT          NULL,           -- HelpDeskDepartmentId (denorm)
    raw_json        NVARCHAR(MAX)   NOT NULL,
    payload_hash    CHAR(64)        NULL,           -- SHA-256 of raw_json for change detection
    synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_helpdesk_messages_source_id  ON bronze.nexudus_helpdesk_messages (source_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_messages_location   ON bronze.nexudus_helpdesk_messages (location_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_messages_coworker   ON bronze.nexudus_helpdesk_messages (coworker_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_messages_sync_run   ON bronze.nexudus_helpdesk_messages (sync_run_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_messages_synced_at  ON bronze.nexudus_helpdesk_messages (synced_at);
GO

CREATE TABLE bronze.nexudus_helpdesk_comments (
    id                  BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id         UNIQUEIDENTIFIER NOT NULL,
    source_id           BIGINT          NOT NULL,   -- HelpDeskComment Id
    helpdesk_message_id BIGINT          NULL,       -- HelpDeskMessageId (denorm; payload has no BusinessId)
    coworker_id         BIGINT          NULL,       -- CoworkerId (denorm — author)
    raw_json            NVARCHAR(MAX)   NOT NULL,
    payload_hash        CHAR(64)        NULL,       -- SHA-256 of raw_json for change detection
    synced_at           DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_helpdesk_comments_source_id ON bronze.nexudus_helpdesk_comments (source_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_comments_message   ON bronze.nexudus_helpdesk_comments (helpdesk_message_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_comments_coworker  ON bronze.nexudus_helpdesk_comments (coworker_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_comments_sync_run  ON bronze.nexudus_helpdesk_comments (sync_run_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_comments_synced_at ON bronze.nexudus_helpdesk_comments (synced_at);
GO

CREATE TABLE bronze.nexudus_helpdesk_departments (
    id              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    sync_run_id     UNIQUEIDENTIFIER NOT NULL,
    source_id       BIGINT          NOT NULL,       -- HelpDeskDepartment Id
    location_id     BIGINT          NULL,           -- BusinessId (denorm)
    raw_json        NVARCHAR(MAX)   NOT NULL,
    payload_hash    CHAR(64)        NULL,           -- SHA-256 of raw_json for change detection
    synced_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_bronze_nexudus_helpdesk_departments_source_id ON bronze.nexudus_helpdesk_departments (source_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_departments_location  ON bronze.nexudus_helpdesk_departments (location_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_departments_sync_run  ON bronze.nexudus_helpdesk_departments (sync_run_id);
CREATE INDEX ix_bronze_nexudus_helpdesk_departments_synced_at ON bronze.nexudus_helpdesk_departments (synced_at);
GO

-- ── Silver ──────────────────────────────────────────────────

CREATE TABLE silver.nexudus_helpdesk_messages (
    id                          BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id                   BIGINT          NOT NULL,
    CONSTRAINT uq_silver_nexudus_helpdesk_messages_source_id UNIQUE (source_id),
    unique_id                   NVARCHAR(64)    NULL,           -- Nexudus UniqueId (GUID)

    -- Traceability
    bronze_id                   BIGINT          NULL,
    sync_run_id                 UNIQUEIDENTIFIER NULL,

    -- Location (soft FK -> silver.nexudus_locations.source_id)
    location_source_id          BIGINT          NOT NULL,       -- BusinessId

    -- Requester (soft FK -> silver.nexudus_coworkers.source_id)
    coworker_source_id          BIGINT          NULL,
    coworker_full_name          NVARCHAR(512)   NULL,           -- denorm, survives coworker deletion

    -- Routing (soft FK -> silver.nexudus_helpdesk_departments.source_id)
    department_source_id        BIGINT          NULL,           -- NULL on ~3% of tickets
    department_name             NVARCHAR(512)   NULL,           -- denorm; names repeat across locations

    -- Content
    subject                     NVARCHAR(1024)  NULL,           -- observed max 123 chars, emoji common
    message_text                NVARCHAR(MAX)   NULL,           -- observed max 2,384 chars

    -- Triage
    priority                    INT             NULL,           -- only value observed so far is 2

    -- Lifecycle
    is_closed                   BIT             NOT NULL DEFAULT 0,
    closed_on                   DATETIME2       NULL,           -- NULL on ~2% of closed tickets

    -- Assignment (Nexudus user, NOT a coworker — no silver table to join)
    owner_source_id             BIGINT          NULL,
    owner_full_name             NVARCHAR(512)   NULL,

    -- SLA
    first_response_minutes      INT             NULL,           -- populated on ~84% of tickets
    -- Derived by the transformer as (closed_on - created_on) in minutes.
    -- The source field `MinutesToClose` is NOT used: Nexudus computes it
    -- backwards and returns a negative number (verified 2026-08-20 on a ticket
    -- created 14:46 and closed 15:28, which reported -41.65).
    minutes_to_close            DECIMAL(18,2)   NULL,

    -- Nexudus AI help-desk integration (mostly unused on this tenant)
    ai_processing_result        INT             NULL,           -- 0 or 1
    ai_channel_session_id       NVARCHAR(128)   NULL,           -- always NULL so far
    support_issue_category      NVARCHAR(256)   NULL,           -- always NULL so far

    -- Attachment
    image_file_name             NVARCHAR(512)   NULL,

    -- Audit
    updated_by                  NVARCHAR(512)   NULL,
    created_on                  DATETIME2       NULL,
    updated_on                  DATETIME2       NULL,

    -- Soft delete (maintained by nexudus_silver_reconcile, weekly)
    is_deleted                  BIT             NOT NULL DEFAULT 0,
    deleted_at                  DATETIME2       NULL,

    -- Pipeline timestamps
    first_seen_at               DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_helpdesk_messages_location   ON silver.nexudus_helpdesk_messages (location_source_id);
CREATE INDEX ix_silver_nexudus_helpdesk_messages_coworker   ON silver.nexudus_helpdesk_messages (coworker_source_id);
CREATE INDEX ix_silver_nexudus_helpdesk_messages_department ON silver.nexudus_helpdesk_messages (department_source_id);
CREATE INDEX ix_silver_nexudus_helpdesk_messages_created    ON silver.nexudus_helpdesk_messages (created_on);
CREATE INDEX ix_silver_nexudus_helpdesk_messages_open       ON silver.nexudus_helpdesk_messages (is_closed, is_deleted);
CREATE INDEX ix_silver_nexudus_helpdesk_messages_deleted    ON silver.nexudus_helpdesk_messages (is_deleted);
GO

CREATE TABLE silver.nexudus_helpdesk_comments (
    id                          BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id                   BIGINT          NOT NULL,
    CONSTRAINT uq_silver_nexudus_helpdesk_comments_source_id UNIQUE (source_id),
    unique_id                   NVARCHAR(64)    NULL,           -- Nexudus UniqueId (GUID)

    -- Traceability
    bronze_id                   BIGINT          NULL,
    sync_run_id                 UNIQUEIDENTIFIER NULL,

    -- Parent ticket (soft FK -> silver.nexudus_helpdesk_messages.source_id)
    helpdesk_message_source_id  BIGINT          NOT NULL,

    -- Location, inherited from the parent message
    -- (soft FK -> silver.nexudus_locations.source_id)
    location_source_id          BIGINT          NULL,

    -- Author (soft FK -> silver.nexudus_coworkers.source_id).
    -- Staff replies also carry a CoworkerId, so this is "who wrote it",
    -- not "the customer" — use is_internal / updated_by to tell them apart.
    coworker_source_id          BIGINT          NULL,
    coworker_full_name          NVARCHAR(512)   NULL,

    -- Content
    message_text                NVARCHAR(MAX)   NULL,           -- observed max 7,103 chars

    -- Staff-only note rather than a customer-visible reply (~0.3% of rows)
    is_internal                 BIT             NOT NULL DEFAULT 0,

    -- Attachment
    image_file_name             NVARCHAR(512)   NULL,

    -- Audit
    updated_by                  NVARCHAR(512)   NULL,
    created_on                  DATETIME2       NULL,
    updated_on                  DATETIME2       NULL,

    -- Soft delete (maintained by nexudus_silver_reconcile, weekly)
    is_deleted                  BIT             NOT NULL DEFAULT 0,
    deleted_at                  DATETIME2       NULL,

    -- Pipeline timestamps
    first_seen_at               DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_helpdesk_comments_message  ON silver.nexudus_helpdesk_comments (helpdesk_message_source_id);
CREATE INDEX ix_silver_nexudus_helpdesk_comments_location ON silver.nexudus_helpdesk_comments (location_source_id);
CREATE INDEX ix_silver_nexudus_helpdesk_comments_coworker ON silver.nexudus_helpdesk_comments (coworker_source_id);
CREATE INDEX ix_silver_nexudus_helpdesk_comments_created  ON silver.nexudus_helpdesk_comments (created_on);
CREATE INDEX ix_silver_nexudus_helpdesk_comments_deleted  ON silver.nexudus_helpdesk_comments (is_deleted);
GO

CREATE TABLE silver.nexudus_helpdesk_departments (
    id                          BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- Source identity
    source_id                   BIGINT          NOT NULL,
    CONSTRAINT uq_silver_nexudus_helpdesk_departments_source_id UNIQUE (source_id),
    unique_id                   NVARCHAR(64)    NULL,           -- Nexudus UniqueId (GUID)

    -- Traceability
    bronze_id                   BIGINT          NULL,
    sync_run_id                 UNIQUEIDENTIFIER NULL,

    -- Location (soft FK -> silver.nexudus_locations.source_id).
    -- Departments are per-location, so the same name (e.g. "Air con queries")
    -- legitimately exists many times over — always group by source_id, or by
    -- (location_source_id, name), never by name alone.
    location_source_id          BIGINT          NOT NULL,

    -- Identity
    name                        NVARCHAR(512)   NOT NULL,
    description                 NVARCHAR(MAX)   NULL,

    -- Flags
    is_active                   BIT             NOT NULL DEFAULT 0,
    task_list_id                BIGINT          NULL,

    -- Audit
    updated_by                  NVARCHAR(512)   NULL,
    created_on                  DATETIME2       NULL,
    updated_on                  DATETIME2       NULL,

    -- Soft delete (maintained by nexudus_silver_reconcile, weekly)
    is_deleted                  BIT             NOT NULL DEFAULT 0,
    deleted_at                  DATETIME2       NULL,

    -- Pipeline timestamps
    first_seen_at               DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    last_synced_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_silver_nexudus_helpdesk_departments_location ON silver.nexudus_helpdesk_departments (location_source_id);
CREATE INDEX ix_silver_nexudus_helpdesk_departments_deleted  ON silver.nexudus_helpdesk_departments (is_deleted);
GO

-- ── Verification ────────────────────────────────────────────
-- SELECT COUNT(*) FROM bronze.nexudus_helpdesk_messages;
-- SELECT COUNT(*) FROM silver.nexudus_helpdesk_messages WHERE is_deleted = 0;
--
-- Open tickets right now, newest first:
-- SELECT m.source_id, l.name AS location, m.department_name, m.subject,
--        m.coworker_full_name, m.created_on, m.first_response_minutes
-- FROM silver.nexudus_helpdesk_messages m
-- LEFT JOIN silver.nexudus_locations l ON l.source_id = m.location_source_id
-- WHERE m.is_deleted = 0 AND m.is_closed = 0
-- ORDER BY m.created_on DESC;
--
-- Median first-response minutes per location, last 90 days:
-- SELECT l.name AS location, COUNT(*) AS tickets,
--        AVG(CAST(m.first_response_minutes AS FLOAT)) AS avg_first_response_min
-- FROM silver.nexudus_helpdesk_messages m
-- LEFT JOIN silver.nexudus_locations l ON l.source_id = m.location_source_id
-- WHERE m.is_deleted = 0
--   AND m.created_on >= DATEADD(DAY, -90, GETUTCDATE())
--   AND m.first_response_minutes IS NOT NULL
-- GROUP BY l.name ORDER BY tickets DESC;
--
-- Full thread for one ticket:
-- SELECT 'ticket' AS kind, m.created_on, m.coworker_full_name, m.subject AS body
-- FROM silver.nexudus_helpdesk_messages m WHERE m.source_id = <id>
-- UNION ALL
-- SELECT 'reply', c.created_on, c.coworker_full_name, c.message_text
-- FROM silver.nexudus_helpdesk_comments c
-- WHERE c.helpdesk_message_source_id = <id> AND c.is_deleted = 0
-- ORDER BY created_on;
