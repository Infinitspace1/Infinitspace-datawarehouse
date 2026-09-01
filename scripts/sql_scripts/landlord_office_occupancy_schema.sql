-- =============================================================================
-- landlord_office_occupancy_schema.sql            (added 2026-09-01)
--
-- gold.vw_landlord_office_occupancy_monthly
--   One row per (location, month, PRODUCT) - the physical inventory list behind
--   the occupancy KPI. Each office / desk shows how many workstations it holds
--   and which company occupies it, or that it is vacant.
--
--   Answers "which offices are let, to whom, and what exactly is empty?", which
--   neither existing view could: the contract book aggregates to a location
--   total, and vw_landlord_monthly_contract_detail is per CONTRACT, so vacant
--   space - having no contract - appears in neither.
--
-- CAPACITY UNIVERSE - deliberately identical to the location_capacity CTE in
-- gold.vw_landlord_contract_book_monthly, so SUM(workstations) over
-- is_in_capacity = 1 equals that view's total_workstation_capacity to the desk:
--     item_type IN (1,2,3), is_deleted = 0, is_available = 1, price > 0,
--     available_from +4h-shifted <= EOMONTH(month), available_to >= month_start
--   The +4h shift matters: Nexudus stores "available from D" as D 22:00 UTC,
--   i.e. midnight LOCAL on D+1 (Zuidtoren floor 2 would otherwise count a full
--   month early). price > 0 separates real inventory from ?0 placeholders.
--
-- OCCUPANCY - a product is occupied in month M when a contract that is active
--   at the END of M lists it in floor_plan_desk_ids. Same month-end convention
--   and status filter as the contract book, so the two agree.
--
-- is_in_capacity = 0 ROWS: a product can be OCCUPIED yet excluded from
--   capacity - typically a ?0-priced product, which the capacity filter drops on
--   purpose. Those rows are still emitted, flagged, so the difference between
--   this table's occupied desks and the KPI's occupied_workstations is visible
--   rather than mysterious. Live example (Aug-2026, the Bower): "Meeting room
--   TB 1-E" is typed item_type = 1 (Private Office) at price 0 and is linked to
--   MY TUTORWEB's contract - so the contract book counts 333 occupied desks
--   while the priced inventory only holds 332. That is a Nexudus data-entry
--   problem (a meeting room mistyped as an office), and this view is where it
--   becomes visible.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC sp_executesql N'CREATE SCHEMA gold';
GO


CREATE OR ALTER VIEW gold.vw_landlord_office_occupancy_monthly
AS
WITH month_offsets AS (
    -- Same window as the other landlord views (-24 .. +12): every month the
    -- period selector offers must resolve, plus 12 of history behind it.
    SELECT -24 AS n
    UNION ALL
    SELECT n + 1 FROM month_offsets WHERE n < 12
),
month_spine AS (
    SELECT DATEADD(MONTH, n, DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)) AS month_start
    FROM month_offsets
),
-- Every product that either counts toward capacity in month M, or is occupied
-- in M despite not counting. The CASE is the capacity filter itself, kept as a
-- flag rather than a WHERE so the excluded-but-occupied rows survive.
product_month AS (
    SELECT
        ms.month_start,
        p.source_id                       AS product_source_id,
        p.location_source_id,
        p.name                            AS product_name,
        p.floor_plan_name,
        p.item_type,
        p.price,
        CASE WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1) ELSE 1 END AS workstations,
        CASE
            WHEN p.is_available = 1
             AND ISNULL(p.price, 0) > 0
             AND (p.available_from IS NULL OR CAST(DATEADD(HOUR, 4, p.available_from) AS DATE) <= EOMONTH(ms.month_start))
             AND (p.available_to   IS NULL OR CAST(p.available_to AS DATE) >= ms.month_start)
            THEN 1 ELSE 0
        END                               AS is_in_capacity
    FROM month_spine ms
    INNER JOIN silver.nexudus_products p
        ON  p.item_type IN (1, 2, 3)
        AND p.is_deleted = 0
    INNER JOIN silver.nexudus_locations loc
        ON  loc.source_id  = p.location_source_id
        AND loc.is_deleted = 0
),
-- Split floor_plan_desk_ids ONCE. Doing it inside the month join instead costs
-- a STRING_SPLIT per contract PER MONTH across the whole spine, which took the
-- view past two minutes.
contract_desks AS (
    SELECT
        c.source_id                        AS contract_source_id,
        c.location_source_id,
        TRY_CONVERT(BIGINT, TRIM(s.value)) AS product_source_id,
        COALESCE(NULLIF(c.coworker_company, N''), c.coworker_billing_name, c.coworker_name) AS member_company_name,
        CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) AS effective_start_date,
        CAST(c.start_date        AS DATE)  AS contract_start_date,
        CAST(c.cancellation_date AS DATE)  AS contract_cancellation_date,
        COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) AS sold_monthly_fee
    FROM silver.nexudus_contracts c
    CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
    WHERE c.is_deleted = 0
      AND c.start_date IS NOT NULL
      AND TRIM(s.value) <> N''
      -- Status filter: active, in-notice, or future-signed. This copies
      -- gold.vw_landlord_contract_book_monthly VERBATIM, including its
      -- unshifted `CAST(start_date AS DATE) > GETUTCDATE()` future-signed test,
      -- because this view exists to EXPLAIN that view's occupancy KPI - if the
      -- two disagree on which contracts count, the table cannot account for the
      -- number on the card. Note the landlord views are not yet consistent
      -- here: vw_landlord_company_type_book_monthly uses the +4h-shifted form
      -- with >=, which is why it and the book disagree on future months. When
      -- that is unified, this filter follows the book.
      AND (
          c.active = 1
          OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
          OR (c.active = 0 AND c.cancelled = 0
              AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE))
      )
),
-- Contracts active at the END of the month. Mirrors the contract book's
-- active_by_month. ROW_NUMBER rather than a correlated TOP 1: a product should
-- carry one contract, and this keeps the row grain stable if Nexudus ever holds
-- two overlapping claims on the same desk.
contract_product AS (
    SELECT
        ms.month_start, cd.location_source_id, cd.product_source_id,
        cd.member_company_name, cd.contract_source_id,
        cd.contract_start_date, cd.contract_cancellation_date, cd.sold_monthly_fee,
        ROW_NUMBER() OVER (
            PARTITION BY ms.month_start, cd.location_source_id, cd.product_source_id
            ORDER BY cd.sold_monthly_fee DESC, cd.contract_source_id
        ) AS rn
    FROM month_spine ms
    INNER JOIN contract_desks cd
        ON  cd.effective_start_date <= EOMONTH(ms.month_start)
        AND (cd.contract_cancellation_date IS NULL
             OR cd.contract_cancellation_date >= EOMONTH(ms.month_start))
)
SELECT
    FORMAT(pm.month_start, 'yyyy-MM')      AS period,
    pm.month_start                         AS month_start_date,
    pm.location_source_id,
    loc.name                               AS location_name,
    pm.product_source_id,
    pm.product_name,
    pm.floor_plan_name,
    pm.item_type,
    CASE pm.item_type WHEN 1 THEN N'Private Office'
                      WHEN 2 THEN N'Dedicated Desk'
                      WHEN 3 THEN N'Hot Desk' END AS product_type,
    pm.workstations,
    pm.price                               AS list_price,
    pm.is_in_capacity,
    CASE WHEN occ.member_company_name IS NULL THEN 0 ELSE 1 END AS is_occupied,
    occ.member_company_name,
    occ.contract_source_id,
    occ.contract_start_date,
    occ.contract_cancellation_date,
    occ.sold_monthly_fee
FROM product_month pm
INNER JOIN silver.nexudus_locations loc
    ON loc.source_id = pm.location_source_id
LEFT JOIN contract_product occ
    ON  occ.product_source_id  = pm.product_source_id
    AND occ.month_start        = pm.month_start
    AND occ.location_source_id = pm.location_source_id
    AND occ.rn = 1
-- Keep priced inventory, plus anything unpriced that is actually occupied (the
-- data-quality rows the header describes). Drops the long tail of retired or
-- placeholder products nobody is sitting in.
WHERE pm.is_in_capacity = 1
   OR occ.member_company_name IS NOT NULL;
GO
