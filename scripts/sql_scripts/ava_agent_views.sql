-- =============================================================================
-- ava.vw_agent_*  —  Serving views for the Copilot Studio "Azure SQL AGENT"
-- =============================================================================
-- Purpose : A narrow, safe, LLM-friendly surface over the ava schema.
--
--           The ava schema also contains LIVE CUSTOMER CONVERSATION DATA
--           (ava.messages, ava.website_conversations, ava.hubspot_conversations,
--            ava.chat_processed_messages, ava.test_conversations, ...).
--           The agent must NEVER be able to reach those. That is enforced by
--           granting its SQL login SELECT on these four views ONLY — see the
--           "Least-privilege login" section at the bottom. Prompt instructions
--           are not a security control; the GRANT is.
--
-- Views:
--   ava.vw_agent_location_summary   — 1 row per location (8). Start here.
--   ava.vw_agent_office_availability— private offices, with availability + timing
--   ava.vw_agent_faq                — active FAQs, location resolved
--   ava.vw_agent_plans              — ancillary plans (tariffs) per location
--
-- Design notes / why these views exist at all:
--   1. city IS NULL IN 100% OF ROWS in both base tables (876/876 and 87/87).
--      The city only exists inside location_name ("Amsterdam - Noord - ...").
--      Every view derives it, preferring the real column if it is ever
--      populated: COALESCE(city, <parsed prefix>). All 876 rows parse cleanly
--      on the ' - ' separator. The underlying refresh SPs should still be fixed;
--      this is a shim so the agent works today and needs no change afterwards.
--   2. Desk rows repeat heavily (292 hot_desk rows for 8 locations) even though
--      the business invariant is ONE price per location. The summary view
--      collapses them so the model cannot present duplicates as distinct offers.
--   3. 78% of private offices are occupied (353 of 451). Occupied rows are kept
--      (needed to answer "when does a 10-person office free up?") but the
--      bookable flag and the notes are surfaced explicitly.
--   4. A price of 0 is "price on request", never free. The office view converts
--      0 -> NULL and raises price_on_request, so £0 is structurally unquotable.
--
-- Apply with:
--   .\venv\Scripts\python.exe scripts\python_scripts\apply_schema_script.py ^
--       scripts/sql_scripts/ava_agent_views.sql
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. ava.vw_agent_location_summary
--    One row per location (8). Small enough to SELECT * on almost every turn,
--    which is the point: it answers most questions without a second round trip.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW ava.vw_agent_location_summary
AS
WITH base AS (
    SELECT
        pa.location_source_id,
        pa.location_name,
        COALESCE(
            pa.city,
            CASE WHEN CHARINDEX(' - ', pa.location_name) > 0
                 THEN LTRIM(RTRIM(LEFT(pa.location_name,
                                       CHARINDEX(' - ', pa.location_name) - 1)))
            END
        )                                       AS city,
        pa.country_name,
        pa.currency_code,
        pa.item_category,
        pa.price,
        pa.external_price,
        pa.capacity,
        pa.is_available,
        pa.last_refreshed_at
    FROM ava.product_availability AS pa
),
agg AS (
    SELECT
        b.location_source_id,
        MAX(b.location_name)                    AS location_name,
        MAX(b.city)                             AS city,
        MAX(b.country_name)                     AS country_name,
        MAX(b.currency_code)                    AS currency_code,

        -- Desks: invariant is one price per location, so MIN collapses the
        -- duplicate product rows to the single real price.
        MIN(CASE WHEN b.item_category = 'hot_desk'
                 THEN b.price END)              AS hot_desk_price_per_month,
        MIN(CASE WHEN b.item_category = 'dedicated_desk'
                 THEN b.price END)              AS dedicated_desk_price_per_month,

        -- Day passes: price = member, external_price = non-member.
        MIN(CASE WHEN b.item_category = 'day_pass'
                 THEN b.price END)              AS day_pass_price_member,
        MAX(CASE WHEN b.item_category = 'day_pass'
                 THEN b.external_price END)     AS day_pass_price_non_member,

        -- Meeting rooms: priced per booking, two tiers.
        COUNT(CASE WHEN b.item_category = 'meeting_room'
                   THEN 1 END)                  AS meeting_room_types,
        MIN(CASE WHEN b.item_category = 'meeting_room'
                 THEN b.price END)              AS meeting_room_member_price_from,
        MAX(CASE WHEN b.item_category = 'meeting_room'
                 THEN b.price END)              AS meeting_room_member_price_to,
        MIN(CASE WHEN b.item_category = 'meeting_room'
                 THEN b.external_price END)     AS meeting_room_non_member_price_from,
        MAX(CASE WHEN b.item_category = 'meeting_room'
                 THEN b.external_price END)     AS meeting_room_non_member_price_to,
        MAX(CASE WHEN b.item_category = 'meeting_room'
                 THEN b.capacity END)           AS meeting_room_max_capacity,

        -- Private offices: only the bookable ones drive the quotable range.
        COUNT(CASE WHEN b.item_category = 'private_office'
                   THEN 1 END)                  AS private_office_total,
        COUNT(CASE WHEN b.item_category = 'private_office' AND b.is_available = 1
                   THEN 1 END)                  AS private_office_available_now,
        MIN(CASE WHEN b.item_category = 'private_office'
                  AND b.is_available = 1 AND b.price > 0
                 THEN b.price END)              AS private_office_price_from,
        MAX(CASE WHEN b.item_category = 'private_office'
                  AND b.is_available = 1 AND b.price > 0
                 THEN b.price END)              AS private_office_price_to,
        MIN(CASE WHEN b.item_category = 'private_office' AND b.is_available = 1
                 THEN b.capacity END)           AS private_office_capacity_from,
        MAX(CASE WHEN b.item_category = 'private_office' AND b.is_available = 1
                 THEN b.capacity END)           AS private_office_capacity_to,

        MAX(b.last_refreshed_at)                AS data_last_refreshed_at
    FROM base AS b
    GROUP BY b.location_source_id
),
plans AS (
    SELECT lp.location_source_id, COUNT(*) AS plan_count
    FROM ava.location_plans AS lp
    WHERE lp.active = 1
    GROUP BY lp.location_source_id
)
SELECT
    a.location_source_id,
    a.location_name,
    a.city,
    a.country_name,
    a.currency_code,
    a.hot_desk_price_per_month,
    a.dedicated_desk_price_per_month,
    a.day_pass_price_member,
    a.day_pass_price_non_member,
    a.meeting_room_types,
    a.meeting_room_member_price_from,
    a.meeting_room_member_price_to,
    a.meeting_room_non_member_price_from,
    a.meeting_room_non_member_price_to,
    a.meeting_room_max_capacity,
    a.private_office_total,
    a.private_office_available_now,
    a.private_office_price_from,
    a.private_office_price_to,
    a.private_office_capacity_from,
    a.private_office_capacity_to,
    ISNULL(p.plan_count, 0)                     AS plan_count,
    a.data_last_refreshed_at
FROM agg AS a
LEFT JOIN plans AS p
       ON p.location_source_id = a.location_source_id;
GO


-- -----------------------------------------------------------------------------
-- 2. ava.vw_agent_office_availability
--    All private offices (occupied ones included, so timing questions work).
--    is_bookable_now is the flag the agent must filter on before quoting.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW ava.vw_agent_office_availability
AS
SELECT
    pa.location_source_id,
    pa.location_name,
    COALESCE(
        pa.city,
        CASE WHEN CHARINDEX(' - ', pa.location_name) > 0
             THEN LTRIM(RTRIM(LEFT(pa.location_name,
                                   CHARINDEX(' - ', pa.location_name) - 1)))
        END
    )                                           AS city,
    pa.country_name,
    pa.item_name                                AS office_name,
    pa.capacity                                 AS desks,

    -- 0 means "price on request", never free. Nulled so it cannot be quoted.
    CASE WHEN pa.price IS NULL OR pa.price = 0
         THEN NULL ELSE pa.price END            AS price_per_month,
    CAST(CASE WHEN pa.price IS NULL OR pa.price = 0
              THEN 1 ELSE 0 END AS BIT)         AS price_on_request,
    pa.currency_code,

    pa.is_available                             AS is_bookable_now,
    pa.available_from,
    pa.occupied_until,
    pa.next_occupied_from,
    pa.chain_occupied_until,
    pa.availability_notes,

    pa.last_refreshed_at                        AS data_last_refreshed_at
FROM ava.product_availability AS pa
WHERE pa.item_category = 'private_office'
  -- Defence in depth: the refresh SP already excludes mis-typed Nexudus rows
  -- (parking bays / meeting rooms filed as ItemType=1). Currently 0 rows match,
  -- but the guard costs nothing and this view is customer-facing.
  AND pa.capacity >= 1
  AND pa.item_name NOT LIKE 'Parking%'
  AND pa.item_name NOT LIKE '%Meeting Room%';
GO


-- -----------------------------------------------------------------------------
-- 3. ava.vw_agent_faq
--    Active FAQs only. scope tells the agent whether an answer is company-wide
--    or specific to one location.
--
--    NOTE: the trailing predicate also excludes FAQs whose location_id does not
--    resolve to a known location — otherwise the agent would present a phantom
--    location's answers as company-wide fact. It is currently a NO-OP: the only
--    unresolvable location (1414964752, 12 rows) is already is_active = 0, i.e.
--    that location was retired cleanly. The guard stays as a tripwire for the
--    next retirement that is not.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW ava.vw_agent_faq
AS
WITH loc AS (
    SELECT DISTINCT
        pa.location_source_id,
        pa.location_name,
        COALESCE(
            pa.city,
            CASE WHEN CHARINDEX(' - ', pa.location_name) > 0
                 THEN LTRIM(RTRIM(LEFT(pa.location_name,
                                       CHARINDEX(' - ', pa.location_name) - 1)))
            END
        )                                       AS city,
        pa.country_name
    FROM ava.product_availability AS pa
)
SELECT
    f.id                                        AS faq_id,
    CASE WHEN f.location_id IS NULL
         THEN 'all_locations' ELSE 'location' END AS scope,
    l.location_source_id,
    l.location_name,
    l.city,
    l.country_name,
    f.category,
    f.question,
    f.answer,
    f.display_order
FROM ava.faqs AS f
LEFT JOIN loc AS l
       ON l.location_source_id = f.location_id
WHERE f.is_active = 1
  AND (f.location_id IS NULL OR l.location_source_id IS NOT NULL);
GO


-- -----------------------------------------------------------------------------
-- 4. ava.vw_agent_plans
--    Ancillary plans (Nexudus tariffs): parking, mailbox, business address,
--    bandwidth, part-time access, service packages...
--    Desks and offices are deliberately NOT here (priced via the other views).
--
--    charge_period is omitted: it is NULL on all 87 rows, so exposing it would
--    only invite the model to invent a billing period.
--    'visible' is NOT filtered: it is 0 on all 87 rows, so filtering on it
--    would return an empty set. 'active' is 1 on all 87.
-- -----------------------------------------------------------------------------
CREATE OR ALTER VIEW ava.vw_agent_plans
AS
SELECT
    lp.location_source_id,
    lp.location_name,
    COALESCE(
        lp.city,
        CASE WHEN CHARINDEX(' - ', lp.location_name) > 0
             THEN LTRIM(RTRIM(LEFT(lp.location_name,
                                   CHARINDEX(' - ', lp.location_name) - 1)))
        END
    )                                           AS city,
    lp.country_name,
    lp.plan_name,
    lp.description,
    lp.system_tariff_type_label                 AS plan_type,
    lp.price,
    lp.currency_code,
    lp.signup_fee,
    lp.deposit,
    lp.included_credit_amount,
    lp.time_credit_minutes,
    lp.term_duration_months,
    lp.notice_period_days,
    lp.last_refreshed_at                        AS data_last_refreshed_at
FROM ava.location_plans AS lp
WHERE lp.active = 1;
GO


-- =============================================================================
-- Least-privilege login for the Copilot Studio connector
-- =============================================================================
-- Ownership chaining does the work: the views and their base tables share the
-- ava schema owner, so this user can read the views WITHOUT any permission on
-- ava.product_availability / ava.faqs / ava.messages / ava.website_conversations.
-- Do not grant anything else. Do not add db_datareader — that would hand it
-- every table in the database, including the conversation tables.
--
-- PREFERRED (Azure SQL Database): a CONTAINED user. No [master] access needed,
-- and the credential travels with the database. Run on infinitspace-prod-main-db:
--
--   CREATE USER ava_agent_ro WITH PASSWORD = '<strong-password-from-key-vault>';
--   GRANT SELECT ON ava.vw_agent_location_summary    TO ava_agent_ro;
--   GRANT SELECT ON ava.vw_agent_office_availability TO ava_agent_ro;
--   GRANT SELECT ON ava.vw_agent_faq                 TO ava_agent_ro;
--   GRANT SELECT ON ava.vw_agent_plans               TO ava_agent_ro;
--
-- ALTERNATIVE (server login, if you need the same identity on several DBs):
--   -- on [master]:
--   CREATE LOGIN ava_agent_ro WITH PASSWORD = '<strong-password-from-key-vault>';
--   -- on the warehouse database:
--   CREATE USER ava_agent_ro FOR LOGIN ava_agent_ro;
--   -- ...then the same four GRANTs as above.
--
-- Verify the blast radius is actually closed (all four must fail / return 0):
--   EXECUTE AS USER = 'ava_agent_ro';
--     SELECT COUNT(*) FROM ava.vw_agent_location_summary;  -- expect 8
--     SELECT COUNT(*) FROM ava.messages;                   -- expect: permission denied
--     SELECT COUNT(*) FROM ava.website_conversations;      -- expect: permission denied
--     SELECT COUNT(*) FROM silver.nexudus_coworkers;       -- expect: permission denied
--   REVERT;
-- =============================================================================


-- =============================================================================
-- Verification
-- =============================================================================
-- SELECT * FROM ava.vw_agent_location_summary;                        -- 8 rows
-- SELECT COUNT(*) FROM ava.vw_agent_office_availability;              -- 451
-- SELECT COUNT(*) FROM ava.vw_agent_office_availability
--   WHERE is_bookable_now = 1;                                        -- 98
-- SELECT COUNT(*) FROM ava.vw_agent_office_availability
--   WHERE price_on_request = 1 AND is_bookable_now = 1;               -- 1 (Aldgate 6029A)
-- SELECT COUNT(*) FROM ava.vw_agent_faq;                              -- 106 (17 global + 89 location)
-- SELECT scope, COUNT(*) FROM ava.vw_agent_faq GROUP BY scope;
-- SELECT COUNT(*) FROM ava.vw_agent_plans;                            -- 87
-- -- city must be non-NULL everywhere:
-- SELECT COUNT(*) FROM ava.vw_agent_location_summary WHERE city IS NULL;   -- 0
-- =============================================================================
