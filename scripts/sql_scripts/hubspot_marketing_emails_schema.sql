-- ============================================================
-- hubspot_marketing_emails_schema.sql  (2026-06-10)
--
-- HubSpot marketing emails: bronze + silver tables.
-- Source: HubSpot Marketing Email API v3
--   GET https://api.hubapi.com/marketing/v3/emails?includeStats=true
--
-- Synced daily by functions/hubspot_sync.py (source_name = 'hubspot'),
-- gated behind ENABLE_HUBSPOT_FUNCTIONS + HUBSPOT_ACCESS_TOKEN.
-- Run this script once against the warehouse DB.
--
-- Design notes:
--   - source_id is the HubSpot email id (numeric, but delivered as a
--     string by the v3 API) -> NVARCHAR(64).
--   - Bronze is latest-payload-wins (UNIQUE source_id, overwrite MERGE)
--     with payload_hash change detection, like bronze.competence_*.
--   - Silver is fully FLAT — no JSON columns (policy). The complete raw
--     payload (incl. stats and content structure) always remains in
--     bronze.hubspot_marketing_emails.raw_json.
--   - is_deleted/deleted_at maintained by the reconcile step embedded in
--     hubspot_sync. Downstream reads MUST filter WHERE is_deleted = 0.
-- ============================================================

-- ── Bronze ──────────────────────────────────────────────────

CREATE TABLE bronze.hubspot_marketing_emails (
    id            BIGINT           IDENTITY(1,1) NOT NULL,
    sync_run_id   UNIQUEIDENTIFIER NOT NULL,
    source_id     NVARCHAR(64)     NOT NULL,       -- HubSpot email id
    email_state   NVARCHAR(64)     NULL,           -- denorm for filtering (DRAFT/PUBLISHED/...)
    raw_json      NVARCHAR(MAX)    NOT NULL,
    payload_hash  CHAR(64)         NULL,           -- SHA-256 of raw_json for change detection
    synced_at     DATETIME2        NOT NULL CONSTRAINT df_bronze_hubspot_emails_synced_at DEFAULT GETUTCDATE(),
    CONSTRAINT pk_bronze_hubspot_marketing_emails PRIMARY KEY (id),
    CONSTRAINT uq_bronze_hubspot_marketing_emails_source_id UNIQUE (source_id)
);

CREATE INDEX ix_bronze_hubspot_emails_sync_run  ON bronze.hubspot_marketing_emails (sync_run_id);
CREATE INDEX ix_bronze_hubspot_emails_synced_at ON bronze.hubspot_marketing_emails (synced_at);
GO

-- ── Silver ──────────────────────────────────────────────────

CREATE TABLE silver.hubspot_marketing_emails (
    source_id           NVARCHAR(64)     NOT NULL,  -- HubSpot email id
    bronze_id           BIGINT           NULL,
    sync_run_id         UNIQUEIDENTIFIER NULL,

    -- Identity
    name                NVARCHAR(512)    NOT NULL,
    subject             NVARCHAR(1000)   NULL,
    state               NVARCHAR(64)     NULL,      -- DRAFT / PUBLISHED / ...
    email_type          NVARCHAR(64)     NULL,      -- BATCH_EMAIL / AUTOMATED_EMAIL / ...
    language            NVARCHAR(16)     NULL,
    archived            BIT              NOT NULL CONSTRAINT df_silver_hubspot_emails_archived DEFAULT 0,
    is_published        BIT              NOT NULL CONSTRAINT df_silver_hubspot_emails_published DEFAULT 0,

    -- Campaign link
    campaign_id         NVARCHAR(64)     NULL,      -- HubSpot campaign GUID
    campaign_name       NVARCHAR(512)    NULL,

    -- Sender
    from_name           NVARCHAR(512)    NULL,
    reply_to            NVARCHAR(512)    NULL,

    -- Content / body (flattened from content.widgets)
    subject_preview_text NVARCHAR(1000)  NULL,      -- inbox preview text
    body_html           NVARCHAR(MAX)    NULL,      -- concatenated rich-text widget HTML, in display order
    body_plain_text     NVARCHAR(MAX)    NULL,      -- content.plainTextVersion, else tag-stripped body_html
    template_path       NVARCHAR(500)    NULL,      -- content.templatePath
    content_widget_count INT             NULL,
    content_widget_names NVARCHAR(MAX)   NULL,
    content_primary_widget_id NVARCHAR(256) NULL,
    content_primary_widget_name NVARCHAR(256) NULL,
    content_primary_widget_type NVARCHAR(128) NULL,
    content_primary_widget_module_id NVARCHAR(128) NULL,
    content_primary_widget_body_module_id NVARCHAR(128) NULL,
    content_primary_widget_html NVARCHAR(MAX) NULL,
    web_version_url     NVARCHAR(1000)   NULL,

    -- Timestamps
    created_at          DATETIME2        NULL,
    updated_at          DATETIME2        NULL,
    published_at        DATETIME2        NULL,

    -- KPI counters (stats.counters)
    stat_sent           INT              NULL,
    stat_delivered      INT              NULL,
    stat_opens          INT              NULL,
    stat_clicks         INT              NULL,
    stat_bounces        INT              NULL,
    stat_unsubscribed   INT              NULL,
    stat_replies        INT              NULL,
    stat_spam_reports   INT              NULL,
    stat_dropped        INT              NULL,
    stat_selected       INT              NULL,
    stat_pending        INT              NULL,
    stat_suppressed     INT              NULL,
    stat_not_sent       INT              NULL,
    stat_hard_bounces   INT              NULL,      -- counters key: hardbounced
    stat_soft_bounces   INT              NULL,      -- counters key: softbounced
    stat_contacts_lost  INT              NULL,

    -- KPI ratios (stats.ratios, PERCENTAGES as returned by HubSpot: 30.901 = 30.9%)
    open_rate           FLOAT            NULL,
    click_rate          FLOAT            NULL,
    click_through_rate  FLOAT            NULL,
    delivered_rate      FLOAT            NULL,
    bounce_rate         FLOAT            NULL,
    unsubscribed_rate   FLOAT            NULL,
    reply_rate          FLOAT            NULL,
    spam_report_rate    FLOAT            NULL,
    hard_bounce_rate    FLOAT            NULL,
    soft_bounce_rate    FLOAT            NULL,
    contacts_lost_rate  FLOAT            NULL,
    pending_rate        FLOAT            NULL,
    not_sent_rate       FLOAT            NULL,

    -- Device breakdown (stats.deviceBreakdown)
    opens_computer      INT              NULL,
    opens_mobile        INT              NULL,
    opens_unknown       INT              NULL,
    clicks_computer     INT              NULL,
    clicks_mobile       INT              NULL,
    clicks_unknown      INT              NULL,

    -- Soft delete (maintained by the reconcile step in hubspot_sync)
    is_deleted          BIT              NOT NULL CONSTRAINT df_silver_hubspot_emails_is_deleted DEFAULT 0,
    deleted_at          DATETIME2        NULL,

    -- Pipeline timestamps
    first_seen_at       DATETIME2        NOT NULL CONSTRAINT df_silver_hubspot_emails_first_seen DEFAULT GETUTCDATE(),
    last_synced_at      DATETIME2        NOT NULL CONSTRAINT df_silver_hubspot_emails_synced_at DEFAULT GETUTCDATE(),

    CONSTRAINT pk_silver_hubspot_marketing_emails PRIMARY KEY (source_id)
);

CREATE INDEX ix_silver_hubspot_emails_state     ON silver.hubspot_marketing_emails (state);
CREATE INDEX ix_silver_hubspot_emails_campaign  ON silver.hubspot_marketing_emails (campaign_id);
CREATE INDEX ix_silver_hubspot_emails_published ON silver.hubspot_marketing_emails (published_at);
CREATE INDEX ix_silver_hubspot_emails_deleted   ON silver.hubspot_marketing_emails (is_deleted);
GO
