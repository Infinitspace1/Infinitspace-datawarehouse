-- ============================================================
-- silver_flatten_json_migration.sql  (2026-06-10)
--
-- Flatten the JSON columns out of the silver layer (policy: no JSON
-- columns in silver — raw payloads stay in bronze.*.raw_json).
--
--   silver.hubspot_marketing_emails:
--     DROP  content_json, stats_json
--     ADD   subject_preview_text, body_html, template_path,
--           content widget metadata from content.widgets,
--           stat_selected, stat_pending, stat_suppressed,
--           hard_bounce_rate, soft_bounce_rate, contacts_lost_rate,
--           pending_rate, not_sent_rate,
--           opens_computer/mobile/unknown, clicks_computer/mobile/unknown
--   silver.eventbrite_events:
--     DROP  venue_json, ticket_availability_json
--     ADD   detailed venue address/resource columns, detailed ticket
--           price columns, and sales start local/timezone columns
--
-- Idempotent — safe to re-run. After applying, re-run the backfills so
-- the new columns are populated for existing rows:
--   python scripts/python_scripts/backfill_flatten_silver_json.py
-- ============================================================

-- ── silver.hubspot_marketing_emails ─────────────────────────

IF COL_LENGTH('silver.hubspot_marketing_emails', 'content_json') IS NOT NULL
    ALTER TABLE silver.hubspot_marketing_emails DROP COLUMN content_json;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'stats_json') IS NOT NULL
    ALTER TABLE silver.hubspot_marketing_emails DROP COLUMN stats_json;
GO

IF COL_LENGTH('silver.hubspot_marketing_emails', 'subject_preview_text') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD subject_preview_text NVARCHAR(1000) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'body_html') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD body_html NVARCHAR(MAX) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'template_path') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD template_path NVARCHAR(500) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'content_widget_count') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD content_widget_count INT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'content_widget_names') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD content_widget_names NVARCHAR(MAX) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'content_primary_widget_id') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD content_primary_widget_id NVARCHAR(256) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'content_primary_widget_name') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD content_primary_widget_name NVARCHAR(256) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'content_primary_widget_type') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD content_primary_widget_type NVARCHAR(128) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'content_primary_widget_module_id') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD content_primary_widget_module_id NVARCHAR(128) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'content_primary_widget_body_module_id') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD content_primary_widget_body_module_id NVARCHAR(128) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'content_primary_widget_html') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD content_primary_widget_html NVARCHAR(MAX) NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'stat_selected') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD stat_selected INT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'stat_pending') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD stat_pending INT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'stat_suppressed') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD stat_suppressed INT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'hard_bounce_rate') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD hard_bounce_rate FLOAT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'soft_bounce_rate') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD soft_bounce_rate FLOAT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'contacts_lost_rate') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD contacts_lost_rate FLOAT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'pending_rate') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD pending_rate FLOAT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'not_sent_rate') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD not_sent_rate FLOAT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'opens_computer') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD opens_computer INT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'opens_mobile') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD opens_mobile INT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'opens_unknown') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD opens_unknown INT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'clicks_computer') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD clicks_computer INT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'clicks_mobile') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD clicks_mobile INT NULL;
IF COL_LENGTH('silver.hubspot_marketing_emails', 'clicks_unknown') IS NULL
    ALTER TABLE silver.hubspot_marketing_emails ADD clicks_unknown INT NULL;
GO

-- ── silver.eventbrite_events ────────────────────────────────

IF COL_LENGTH('silver.eventbrite_events', 'venue_json') IS NOT NULL
    ALTER TABLE silver.eventbrite_events DROP COLUMN venue_json;
IF COL_LENGTH('silver.eventbrite_events', 'ticket_availability_json') IS NOT NULL
    ALTER TABLE silver.eventbrite_events DROP COLUMN ticket_availability_json;
GO

IF COL_LENGTH('silver.eventbrite_events', 'venue_address_1') IS NULL
    ALTER TABLE silver.eventbrite_events ADD venue_address_1 NVARCHAR(500) NULL;
IF COL_LENGTH('silver.eventbrite_events', 'venue_address_2') IS NULL
    ALTER TABLE silver.eventbrite_events ADD venue_address_2 NVARCHAR(500) NULL;
IF COL_LENGTH('silver.eventbrite_events', 'venue_resource_uri') IS NULL
    ALTER TABLE silver.eventbrite_events ADD venue_resource_uri NVARCHAR(1000) NULL;
IF COL_LENGTH('silver.eventbrite_events', 'venue_address_latitude') IS NULL
    ALTER TABLE silver.eventbrite_events ADD venue_address_latitude FLOAT NULL;
IF COL_LENGTH('silver.eventbrite_events', 'venue_address_longitude') IS NULL
    ALTER TABLE silver.eventbrite_events ADD venue_address_longitude FLOAT NULL;
IF COL_LENGTH('silver.eventbrite_events', 'venue_localized_area') IS NULL
    ALTER TABLE silver.eventbrite_events ADD venue_localized_area NVARCHAR(500) NULL;
IF COL_LENGTH('silver.eventbrite_events', 'venue_multi_line_address') IS NULL
    ALTER TABLE silver.eventbrite_events ADD venue_multi_line_address NVARCHAR(1000) NULL;
IF COL_LENGTH('silver.eventbrite_events', 'venue_capacity') IS NULL
    ALTER TABLE silver.eventbrite_events ADD venue_capacity INT NULL;
IF COL_LENGTH('silver.eventbrite_events', 'venue_age_restriction') IS NULL
    ALTER TABLE silver.eventbrite_events ADD venue_age_restriction NVARCHAR(64) NULL;
IF COL_LENGTH('silver.eventbrite_events', 'minimum_ticket_price_currency') IS NULL
    ALTER TABLE silver.eventbrite_events ADD minimum_ticket_price_currency NVARCHAR(8) NULL;
IF COL_LENGTH('silver.eventbrite_events', 'minimum_ticket_price_minor') IS NULL
    ALTER TABLE silver.eventbrite_events ADD minimum_ticket_price_minor INT NULL;
IF COL_LENGTH('silver.eventbrite_events', 'maximum_ticket_price_currency') IS NULL
    ALTER TABLE silver.eventbrite_events ADD maximum_ticket_price_currency NVARCHAR(8) NULL;
IF COL_LENGTH('silver.eventbrite_events', 'maximum_ticket_price_minor') IS NULL
    ALTER TABLE silver.eventbrite_events ADD maximum_ticket_price_minor INT NULL;
IF COL_LENGTH('silver.eventbrite_events', 'ticket_currency') IS NULL
    ALTER TABLE silver.eventbrite_events ADD ticket_currency NVARCHAR(8) NULL;
IF COL_LENGTH('silver.eventbrite_events', 'sales_start_local') IS NULL
    ALTER TABLE silver.eventbrite_events ADD sales_start_local DATETIME2 NULL;
IF COL_LENGTH('silver.eventbrite_events', 'sales_start_timezone') IS NULL
    ALTER TABLE silver.eventbrite_events ADD sales_start_timezone NVARCHAR(64) NULL;
GO
