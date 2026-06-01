-- =============================================================================
-- landlord_dashboard_materialized.sql
--
-- Materialized snapshot tables for the strategic-partnership / landlord
-- dashboard. The dashboard reads from these tables instead of querying the
-- gold.vw_landlord_*_monthly views directly, because for large buildings
-- (Aldgate: ~4k contracts) the views' recursive month spine + CROSS APPLY
-- STRING_SPLIT(floor_plan_desk_ids) explodes to multi-second query times.
--
-- Refresh:
--   functions/landlord_materialize_dashboard.py runs daily at 03:00 UTC
--   (default schedule "0 0 3 * * *", sits between silver sync at 02:30 and
--   the sync-health email at 06:00) and does:
--     1. TRUNCATE gold.t_landlord_contract_book_monthly
--     2. INSERT INTO ... SELECT FROM gold.vw_landlord_contract_book_monthly
--     3. Same for gold.t_landlord_revenue_past_location_monthly and
--        gold.t_landlord_membership_book_monthly
--   Each refresh is wrapped in a transaction so readers never see a partially
--   populated table. RunTracker logs row counts + elapsed time.
--
-- Indexes:
--   Primary key (location_source_id, period) → point lookups in <10ms.
--   The dashboard always filters by both columns; this is the only access
--   pattern.
--
-- Staleness budget:
--   Silver itself refreshes once per day (silver_nexudus at 02:30 UTC), so
--   the dashboard data is at most one silver-cycle stale. The dashboard's
--   "last refreshed at" badge surfaces the refresh timestamp.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC sp_executesql N'CREATE SCHEMA gold';
GO


-- ── 1. Contract-book monthly snapshot ────────────────────────────────────────
IF OBJECT_ID('gold.t_landlord_contract_book_monthly', 'U') IS NOT NULL
    DROP TABLE gold.t_landlord_contract_book_monthly;
GO

CREATE TABLE gold.t_landlord_contract_book_monthly (
    period                          CHAR(7)        NOT NULL,
    month_start_date                DATE           NOT NULL,
    location_source_id              BIGINT         NOT NULL,
    location_name                   NVARCHAR(255)  NULL,
    location_city                   NVARCHAR(255)  NULL,
    location_country_name           NVARCHAR(255)  NULL,
    total_workstation_capacity      INT            NULL,
    active_contract_count           INT            NULL,
    occupied_workstations           INT            NULL,
    vacant_workstations             INT            NULL,
    occupancy_pct                   DECIMAL(9,4)   NULL,
    sold_monthly_revenue            DECIMAL(18,2)  NULL,
    list_monthly_revenue            DECIMAL(18,2)  NULL,
    avg_sold_price_per_ws           DECIMAL(18,2)  NULL,
    avg_list_price_per_ws           DECIMAL(18,2)  NULL,
    avg_discount_pct                DECIMAL(9,4)   NULL,
    discount_monthly_value          DECIMAL(18,2)  NULL,
    private_office_contract_count   INT            NULL,
    private_office_capacity         INT            NULL,
    private_office_sold_revenue     DECIMAL(18,2)  NULL,
    private_office_list_revenue     DECIMAL(18,2)  NULL,
    new_workstations_starting       INT            NULL,
    workstations_cancelling         INT            NULL,
    net_workstation_change          INT            NULL,
    contracts_missing_list_price    INT            NULL,
    adjustment_contract_count       INT            NULL,
    adjustment_monthly_value        DECIMAL(18,2)  NULL,
    calculation_basis               NVARCHAR(32)   NULL,
    refreshed_at                    DATETIME2(0)   NOT NULL,
    CONSTRAINT pk_t_landlord_contract_book_monthly
        PRIMARY KEY CLUSTERED (location_source_id, period)
);
GO


-- ── 2. Invoice-based past revenue snapshot ──────────────────────────────────
IF OBJECT_ID('gold.t_landlord_revenue_past_location_monthly', 'U') IS NOT NULL
    DROP TABLE gold.t_landlord_revenue_past_location_monthly;
GO

CREATE TABLE gold.t_landlord_revenue_past_location_monthly (
    period                  CHAR(7)        NOT NULL,
    month_start_date        DATE           NOT NULL,
    location_source_id      BIGINT         NOT NULL,
    location_name           NVARCHAR(255)  NULL,
    currency_code           NVARCHAR(8)    NULL,
    sold_monthly_revenue    DECIMAL(18,2)  NULL,
    line_count              INT            NULL,
    negative_line_count     INT            NULL,
    member_count            INT            NULL,
    refreshed_at            DATETIME2(0)   NOT NULL,
    CONSTRAINT pk_t_landlord_revenue_past_location_monthly
        PRIMARY KEY CLUSTERED (location_source_id, period)
);
GO


-- ── 3. Membership-book monthly snapshot (forecast side) ─────────────────────
-- The forecast bars on the dashboard read from
-- gold.vw_landlord_membership_book_monthly. Adding it here too because the
-- view scans the same silver tables and shows up in the dashboard hot path.
IF OBJECT_ID('gold.t_landlord_membership_book_monthly', 'U') IS NOT NULL
    DROP TABLE gold.t_landlord_membership_book_monthly;
GO

CREATE TABLE gold.t_landlord_membership_book_monthly (
    period                          CHAR(7)        NOT NULL,
    month_start_date                DATE           NOT NULL,
    location_source_id              BIGINT         NOT NULL,
    location_name                   NVARCHAR(255)  NULL,
    location_city                   NVARCHAR(255)  NULL,
    location_country_name           NVARCHAR(255)  NULL,
    total_workstation_capacity      INT            NULL,
    active_contract_count           INT            NULL,
    occupied_workstations           INT            NULL,
    vacant_workstations             INT            NULL,
    occupancy_pct                   DECIMAL(9,4)   NULL,
    sold_monthly_revenue            DECIMAL(18,2)  NULL,
    list_monthly_revenue            DECIMAL(18,2)  NULL,
    avg_sold_price_per_ws           DECIMAL(18,2)  NULL,
    avg_list_price_per_ws           DECIMAL(18,2)  NULL,
    adjustment_contract_count       INT            NULL,
    adjustment_monthly_value        DECIMAL(18,2)  NULL,
    calculation_basis               NVARCHAR(32)   NULL,
    refreshed_at                    DATETIME2(0)   NOT NULL,
    CONSTRAINT pk_t_landlord_membership_book_monthly
        PRIMARY KEY CLUSTERED (location_source_id, period)
);
GO
