-- =============================================================================
-- ava.location_plans  +  ava.sp_refresh_location_plans
-- =============================================================================
-- Purpose : Denormalized, per-location list of Nexudus PLANS (the UI calls them
--           "Plans"; the API calls them "Tariffs" — endpoint GET /billing/tariffs)
--           that Ava can quote. One row per (location, plan).
--
--           This is a SERVING-LAYER filtered view of silver.nexudus_tariffs.
--           The silver layer keeps EVERY tariff untouched (full reference data);
--           all filtering happens here so the Ava table only carries the plans
--           we actually want Ava to surface.
--
-- Exclusion rules (a plan is DROPPED from this table when ANY apply):
--   1. is_deleted = 1                              (soft-deleted in source)
--   2. price <= 0  OR price IS NULL                ("0 euros services")
--   3. SystemTariffType IN (1, 3, 5)              core desk/office products:
--        1 = Private Office   3 = Dedicated Desk   5 = Hot Desk
--      Those three are sold/priced elsewhere (private offices per-contract,
--      hot/dedicated desks via ava.product_availability) and are explicitly
--      out of scope for the plans table.
--   4. the plan's location does not resolve to a silver location
--      (a handful of plans hang off a "beyond"-level BusinessId with no
--       physical location — not useful for a per-location plan list).
--
--   Everything else is kept: connectivity (bandwidth / network ports / IP /
--   VLAN / dedicated broadband), parking, business-address registration,
--   physical mailbox, rack space, service packages, part-time access, etc.
--
-- SystemTariffType source:
--   This enum is NOT exposed on silver.nexudus_tariffs (silver is deliberately
--   left unchanged). It lives only in the raw Nexudus payload, so the SP reads
--   it back from bronze.nexudus_tariffs.raw_json via JSON_VALUE. bronze is a
--   latest-payload upsert keyed UNIQUE on source_id, so there is exactly one
--   bronze row per tariff.
--
-- Refreshed: Daily, right after ava.product_availability, via
--            functions/ava_refresh.py (Azure Function, timer 03:00 UTC).
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ava')
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA ava';
END
GO

-- Drop and recreate (schema evolution handled by SP refresh, not table alter)
IF OBJECT_ID('ava.location_plans', 'U') IS NOT NULL
    DROP TABLE ava.location_plans;
GO

CREATE TABLE ava.location_plans (
    id                          BIGINT          IDENTITY(1,1) PRIMARY KEY,

    -- -------------------------------------------------------------------------
    -- Location context (denormalised for Ava — avoids joins at query time)
    -- -------------------------------------------------------------------------
    location_source_id          BIGINT          NOT NULL,   -- silver.nexudus_locations.source_id
    location_name               NVARCHAR(512)   NOT NULL,
    city                        NVARCHAR(255)   NULL,
    country_name                NVARCHAR(128)   NULL,

    -- -------------------------------------------------------------------------
    -- Plan identity  (= Nexudus tariff)
    -- -------------------------------------------------------------------------
    tariff_source_id            BIGINT          NOT NULL,   -- silver.nexudus_tariffs.source_id
    plan_name                   NVARCHAR(512)   NOT NULL,
    description                 NVARCHAR(MAX)   NULL,

    -- Nexudus SystemTariffType (read from bronze raw payload).
    --   8 = part-time access, 9 = mailbox/storage, 99 = custom/ancillary service.
    --   (1/3/5 are excluded from this table — see header.)
    system_tariff_type          INT             NULL,
    system_tariff_type_label    NVARCHAR(64)    NULL,       -- friendly label for the above

    -- -------------------------------------------------------------------------
    -- Pricing
    -- -------------------------------------------------------------------------
    price                       DECIMAL(12,2)   NULL,       -- always > 0 in this table
    currency_code               NVARCHAR(8)     NULL,
    signup_fee                  DECIMAL(12,2)   NULL,
    deposit                     DECIMAL(12,2)   NULL,
    included_credit_amount      DECIMAL(12,2)   NULL,
    time_credit_minutes         INT             NULL,

    -- -------------------------------------------------------------------------
    -- Billing cadence (raw Nexudus enums, passed through from silver)
    -- -------------------------------------------------------------------------
    charge_period               INT             NULL,       -- 0=monthly, 1=weekly, ... (Nexudus enum)
    term_duration_months        INT             NULL,
    notice_period_days          INT             NULL,

    -- -------------------------------------------------------------------------
    -- Accounting context (handy for Ava — which revenue bucket the plan books to)
    -- -------------------------------------------------------------------------
    financial_account_id        BIGINT          NULL,
    financial_account_name      NVARCHAR(512)   NULL,

    -- -------------------------------------------------------------------------
    -- Flags
    -- -------------------------------------------------------------------------
    active                      BIT             NULL,
    visible                     BIT             NULL,

    -- -------------------------------------------------------------------------
    -- Freshness
    -- -------------------------------------------------------------------------
    last_refreshed_at           DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);
GO

CREATE INDEX ix_ava_location_plans_location
    ON ava.location_plans (location_source_id);

CREATE INDEX ix_ava_location_plans_tariff
    ON ava.location_plans (tariff_source_id);

CREATE INDEX ix_ava_location_plans_type
    ON ava.location_plans (system_tariff_type);
GO


-- =============================================================================
-- ava.sp_refresh_location_plans
-- =============================================================================
CREATE OR ALTER PROCEDURE ava.sp_refresh_location_plans
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION;
    BEGIN TRY

        -- DELETE (not TRUNCATE) so this runs with the function app's default
        -- DELETE permission — same rationale as sp_refresh_product_availability.
        DELETE FROM ava.location_plans;

        ;WITH tariff_typed AS (
            -- Silver tariff (full typed reference data) + the SystemTariffType
            -- enum read back from the raw bronze payload. silver is left
            -- unchanged, so the enum can only come from bronze.raw_json.
            SELECT
                t.source_id,
                t.name,
                t.description,
                t.location_source_id,
                t.price,
                t.currency_code,
                t.signup_fee,
                t.deposit,
                t.included_credit_amount,
                t.time_credit_minutes,
                t.charge_period,
                t.term_duration_months,
                t.notice_period_days,
                t.financial_account_id,
                t.active,
                t.visible,
                TRY_CONVERT(INT, JSON_VALUE(b.raw_json, '$.SystemTariffType')) AS system_tariff_type
            FROM silver.nexudus_tariffs t
            LEFT JOIN bronze.nexudus_tariffs b
                ON b.source_id = t.source_id
            WHERE t.is_deleted = 0
        )
        INSERT INTO ava.location_plans (
            location_source_id, location_name, city, country_name,
            tariff_source_id, plan_name, description,
            system_tariff_type, system_tariff_type_label,
            price, currency_code, signup_fee, deposit,
            included_credit_amount, time_credit_minutes,
            charge_period, term_duration_months, notice_period_days,
            financial_account_id, financial_account_name,
            active, visible,
            last_refreshed_at
        )
        SELECT
            tt.location_source_id,
            l.name              AS location_name,
            l.city,
            l.country_name,
            tt.source_id        AS tariff_source_id,
            tt.name             AS plan_name,
            tt.description,
            tt.system_tariff_type,
            CASE tt.system_tariff_type
                WHEN 8  THEN 'part_time_access'
                WHEN 9  THEN 'mailbox_storage'
                ELSE         'service'
            END                 AS system_tariff_type_label,
            tt.price,
            tt.currency_code,
            tt.signup_fee,
            tt.deposit,
            tt.included_credit_amount,
            tt.time_credit_minutes,
            tt.charge_period,
            tt.term_duration_months,
            tt.notice_period_days,
            tt.financial_account_id,
            fa.name             AS financial_account_name,
            tt.active,
            tt.visible,
            GETUTCDATE()
        FROM tariff_typed tt
        JOIN silver.nexudus_locations l
            ON  l.source_id  = tt.location_source_id
            AND l.is_deleted = 0
        LEFT JOIN silver.nexudus_financial_accounts fa
            ON  fa.source_id = tt.financial_account_id
            AND fa.is_deleted = 0
        WHERE tt.price > 0                                       -- drop "0 euros services"
          AND (tt.system_tariff_type IS NULL
               OR tt.system_tariff_type NOT IN (1, 3, 5));       -- drop hot/dedicated desk + private office

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;   -- re-raise so the Azure Function records the failure in meta
    END CATCH;
END;
GO


-- Populate immediately on apply (idempotent; mirrors ava_product_availability_schema.sql).
EXEC ava.sp_refresh_location_plans;
GO

-- ── Verification ─────────────────────────────────────────────────────────────
SELECT COUNT(*) AS total_plans FROM ava.location_plans;

SELECT
    location_name,
    COUNT(*)            AS plan_count,
    MIN(price)          AS min_price,
    MAX(price)          AS max_price,
    MAX(currency_code)  AS currency_code
FROM ava.location_plans
GROUP BY location_name
ORDER BY location_name;
