-- =============================================================================
-- landlord_dashboard_revenue_schema.sql
--
-- Phase 1 (2026-05-28): Invoice-based PAST revenue for the strategic
-- partnership / landlord dashboard.
--
-- Why a separate file from landlord_dashboard_schema.sql:
--   landlord_dashboard_schema.sql is contract-based — it answers "what's
--   booked?". This file is invoice-based — it answers "what was billed?".
--   For PAST months the dashboard now reads from this view (closer to
--   what's actually recognised as revenue). For FUTURE months it keeps
--   reading from the contract-based forecast (Phase 2 will add a
--   tariff-filtered future view in this same file).
--
-- Source of truth:
--   silver.nexudus_coworker_invoice_lines (one row per billed line)
--   silver.nexudus_coworker_invoices      (parent invoice — gives dates,
--                                          location, member)
--
-- Filter:
--   Only invoice lines where LOWER(financial_account_name) LIKE '%membership fee%'.
--   This captures Private Offices, Dedicated Desks, Hot Desks, and their
--   discount/credit lines. Excludes parking, business address registration,
--   meeting-room bookings, late fees, etc.
--
-- Allocation (pro-rata by DAY count):
--   Each line's amount = unit_price * quantity. That amount is spread across
--   every calendar month that the line's effective period touches, weighted
--   by days in that month vs total days in the period.
--
--   Example: an invoice line for €7,600 with effective period 2026-04-15 →
--   2026-06-30 (76 days total) allocates:
--     2026-04:  16 days → €7,600 × 16/76 = €1,600
--     2026-05:  31 days → €7,600 × 31/76 = €3,100
--     2026-06:  30 days → €7,600 × 30/76 = €3,000
--
-- Effective period priority:
--   1. invoice_from_date AND invoice_to_date both set → use them
--   2. otherwise (a ONE-OFF charge — no service period) → allocate the whole
--                  line to the calendar month of the INVOICE DATE
--                  (created_on = Nexudus CreatedOn, the "Date" shown on the
--                  invoice list), effective_from = that month_start,
--                  effective_to = month_start of the next month
--   3. due_date → safety net only, if created_on is ever missing
--
--   (2) was due_date until 2026-08-06. due_date is a payment-terms date, not a
--   revenue date, so one-off membership-fee corrections landed in the wrong
--   month whenever the terms crossed a month boundary — e.g. a €250 correction
--   invoiced 1 Jul 2026 with a 31 Aug 2026 due date was reported in August.
--
-- Timezone normalisation:
--   All Nexudus timestamps are shifted +4h before casting to DATE. This
--   maps the "midnight local stored as 22:00 UTC the previous day" Nexudus
--   convention to the correct local-time date. e.g.
--     2026-05-31T22:00 UTC + 4h = 2026-06-01T02:00 UTC → date 2026-06-01
--   matching the human reading "start of June". Mirrors the same logic
--   used in vw_landlord_contract_book_monthly.contract_facts.
--
-- Invoice status filter:
--   Excluded: draft = 1, void = 1
--   Included: everything else (paid + unpaid + credit notes + refunded).
--   Credit notes (credit_note = 1) carry negative amounts and correctly
--   net the revenue down; if both the original AND a credit note exist
--   they net to zero, which is the right result.
--
-- Output grain:
--   One row per (period, location_source_id, member_company_name).
--   Flask can SUM per (period, location) for the chart, or read rows
--   directly for a per-member drill-down.
--
-- Window:
--   25 months (-12 to +12 from current month) so the same view can be
--   reused by future drill-downs that want to look at paid-in-advance
--   billings landing in upcoming months. The dashboard's PAST-12-MONTH
--   chart only consumes the -11..0 slice.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC sp_executesql N'CREATE SCHEMA gold';
GO


CREATE OR ALTER VIEW gold.vw_landlord_revenue_past_monthly
AS
WITH month_offsets AS (
    -- 25 months: -12 .. +12 from the current UTC month.
    SELECT -12 AS n
    UNION ALL
    SELECT n + 1 FROM month_offsets WHERE n < 12
),
month_spine AS (
    SELECT
        DATEADD(
            MONTH, n,
            DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)
        ) AS month_start,
        DATEADD(
            MONTH, n + 1,
            DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)
        ) AS month_end  -- first day of NEXT month, exclusive upper bound
    FROM month_offsets
),
-- Step 1: Filter invoice lines to membership-fee lines, join to the parent
-- invoice for dates / location / member, and compute the effective period.
filtered_lines AS (
    SELECT
        il.source_id                                  AS line_source_id,
        il.invoice_source_id,
        i.location_source_id,
        i.location_name,
        i.coworker_id,
        COALESCE(
            NULLIF(i.coworker_company_name, N''),
            NULLIF(i.bill_to_name,         N''),
            i.coworker_name
        )                                             AS member_company_name,
        i.coworker_name,
        i.currency_code,
        il.financial_account_name,
        il.description                                AS line_description,
        il.product_name                               AS line_product_name,
        il.quantity,
        il.unit_price,
        -- Per user spec: line amount is unit_price × quantity (NOT sub_total
        -- or total_amount, which may include tax).
        CAST(ISNULL(il.unit_price, 0) * ISNULL(il.quantity, 0) AS DECIMAL(18,4))
                                                      AS line_amount,
        -- Effective period for allocation.
        -- Branch 1: invoice_from + invoice_to both set → use them with +4h shift.
        -- Branch 2: no service period — a ONE-OFF charge (e.g. a membership-fee
        --           correction). Whole line lands in the INVOICE DATE's calendar
        --           month (created_on = Nexudus CreatedOn = the invoice "Date").
        -- Branch 3: due_date — safety net only, if created_on is ever missing.
        CASE
            WHEN i.invoice_from_date IS NOT NULL AND i.invoice_to_date IS NOT NULL
                THEN CAST(DATEADD(HOUR, 4, i.invoice_from_date) AS DATE)
            WHEN i.created_on IS NOT NULL
                THEN DATEFROMPARTS(
                    YEAR (CAST(DATEADD(HOUR, 4, i.created_on) AS DATE)),
                    MONTH(CAST(DATEADD(HOUR, 4, i.created_on) AS DATE)),
                    1
                )
            WHEN i.due_date IS NOT NULL
                THEN DATEFROMPARTS(
                    YEAR (CAST(DATEADD(HOUR, 4, i.due_date) AS DATE)),
                    MONTH(CAST(DATEADD(HOUR, 4, i.due_date) AS DATE)),
                    1
                )
        END                                           AS effective_from,
        CASE
            WHEN i.invoice_from_date IS NOT NULL AND i.invoice_to_date IS NOT NULL
                THEN CAST(DATEADD(HOUR, 4, i.invoice_to_date) AS DATE)
            WHEN i.created_on IS NOT NULL
                THEN DATEADD(MONTH, 1, DATEFROMPARTS(
                    YEAR (CAST(DATEADD(HOUR, 4, i.created_on) AS DATE)),
                    MONTH(CAST(DATEADD(HOUR, 4, i.created_on) AS DATE)),
                    1
                ))
            WHEN i.due_date IS NOT NULL
                THEN DATEADD(MONTH, 1, DATEFROMPARTS(
                    YEAR (CAST(DATEADD(HOUR, 4, i.due_date) AS DATE)),
                    MONTH(CAST(DATEADD(HOUR, 4, i.due_date) AS DATE)),
                    1
                ))
        END                                           AS effective_to
    FROM silver.nexudus_coworker_invoice_lines il
    INNER JOIN silver.nexudus_coworker_invoices i
        ON i.source_id = il.invoice_source_id
    WHERE LOWER(il.financial_account_name) LIKE N'%membership fee%'
      -- Exclude unsent / cancelled invoices. Keep credit notes + refunded
      -- (they net revenue down via separate negative-amount invoices).
      AND ISNULL(i.draft, 0) = 0
      AND ISNULL(i.void,  0) = 0
      -- Honour silver's tombstones (added 2026-08-12). Without these the view
      -- counted rows the reconcile had already marked as gone at source: lines
      -- deleted off a surviving invoice, and every invoice belonging to an
      -- excluded location (Kingsbourne House).
      AND ISNULL(il.is_deleted, 0) = 0
      AND ISNULL(i.is_deleted,  0) = 0
      AND i.location_source_id IS NOT NULL
      -- Must have some kind of dating
      AND (i.invoice_from_date IS NOT NULL OR i.created_on IS NOT NULL OR i.due_date IS NOT NULL)
),
-- Step 2: For each (line, month) where the line's effective period intersects
-- the month, compute the overlap days. This is the per-line, per-month
-- contribution to revenue.
line_month_allocation AS (
    SELECT
        ms.month_start,
        FORMAT(ms.month_start, 'yyyy-MM')             AS period,
        fl.line_source_id,
        fl.invoice_source_id,
        fl.location_source_id,
        fl.location_name,
        fl.member_company_name,
        fl.currency_code,
        fl.financial_account_name,
        fl.line_amount,
        fl.effective_from,
        fl.effective_to,
        -- Overlap days = max(0, min(line_end, month_end) - max(line_start, month_start))
        CASE
            WHEN fl.effective_to   <= ms.month_start THEN 0
            WHEN fl.effective_from >= ms.month_end   THEN 0
            ELSE DATEDIFF(
                DAY,
                CASE WHEN fl.effective_from > ms.month_start THEN fl.effective_from ELSE ms.month_start END,
                CASE WHEN fl.effective_to   < ms.month_end   THEN fl.effective_to   ELSE ms.month_end   END
            )
        END                                           AS overlap_days,
        DATEDIFF(DAY, fl.effective_from, fl.effective_to) AS total_days
    FROM filtered_lines fl
    INNER JOIN month_spine ms
        ON  fl.effective_from < ms.month_end
        AND fl.effective_to   > ms.month_start
)
-- Step 3: Aggregate per (period, location, member). Apply the day-weighted
-- proration when emitting the allocated amount.
SELECT
    lma.period,
    lma.month_start                                   AS month_start_date,
    lma.location_source_id,
    MAX(lma.location_name)                            AS location_name,
    lma.member_company_name,
    MAX(lma.currency_code)                            AS currency_code,
    CAST(
        SUM(
            lma.line_amount *
            CAST(lma.overlap_days AS DECIMAL(18,6))
            / NULLIF(lma.total_days, 0)
        )
        AS DECIMAL(18,2)
    )                                                 AS sold_monthly_revenue,
    COUNT(*)                                          AS line_count,
    SUM(CASE WHEN lma.line_amount < 0 THEN 1 ELSE 0 END) AS negative_line_count
FROM line_month_allocation lma
WHERE lma.total_days   > 0
  AND lma.overlap_days > 0
GROUP BY
    lma.period,
    lma.month_start,
    lma.location_source_id,
    lma.member_company_name;
GO


-- =============================================================================
-- Convenience roll-up: one row per (period, location) without member detail.
-- Used by the chart (which doesn't need per-member breakdown).
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_revenue_past_location_monthly
AS
SELECT
    period,
    MIN(month_start_date)                AS month_start_date,
    location_source_id,
    MAX(location_name)                   AS location_name,
    MAX(currency_code)                   AS currency_code,
    CAST(SUM(sold_monthly_revenue) AS DECIMAL(18,2)) AS sold_monthly_revenue,
    SUM(line_count)                      AS line_count,
    SUM(negative_line_count)             AS negative_line_count,
    COUNT(DISTINCT member_company_name)  AS member_count
FROM gold.vw_landlord_revenue_past_monthly
GROUP BY period, location_source_id;
GO


-- =============================================================================
-- Phase 2 (2026-05-28): FUTURE REVENUE + OCCUPANCY (tariff-filtered)
-- =============================================================================
--
-- gold.vw_landlord_membership_book_monthly mirrors
-- gold.vw_landlord_contract_book_monthly but joins each contract through
-- its tariff to silver.nexudus_financial_accounts and filters by
-- LOWER(financial_account.name) LIKE '%membership fee%' — the SAME rule
-- Phase 1's invoice-based revenue uses. Result: past, current, and future
-- months are now classified consistently.
--
-- Flask reads from this view for the FUTURE side of the forecast chart.
-- Past months continue to use the invoice-based view from Phase 1.
--
-- Output grain: one row per (location, period) — same shape as
-- vw_landlord_contract_book_monthly so Flask code can swap in with
-- minimal change.
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_membership_book_monthly
AS
WITH month_offsets AS (
    -- 25 months: -12 to +12 from the current UTC month.
    SELECT -12 AS n
    UNION ALL
    SELECT n + 1 FROM month_offsets WHERE n < 12
),
month_spine AS (
    SELECT
        DATEADD(MONTH, n, DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)) AS month_start
    FROM month_offsets
),
location_list AS (
    SELECT
        source_id       AS location_source_id,
        name            AS location_name,
        city            AS location_city,
        country_name    AS location_country_name
    FROM silver.nexudus_locations
    WHERE is_deleted = 0
),
-- Same capacity-and-list-price product link as contract_book_monthly so
-- numbers reconcile on the rows that pass the filter.
contract_product_link AS (
    SELECT
        c.source_id AS contract_source_id,
        SUM(
            CASE
                WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                WHEN p.item_type IN (2, 3) THEN 1
                ELSE 0
            END
        ) AS capacity,
        SUM(ISNULL(p.price, 0)) AS list_monthly_fee
    FROM silver.nexudus_contracts c
    CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
    INNER JOIN silver.nexudus_products p
        ON  p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
        AND p.item_type IN (1, 2, 3)
        AND p.is_deleted = 0
    WHERE TRIM(s.value) <> N''
      AND c.is_deleted = 0
    GROUP BY c.source_id
),
-- Per-contract facts ALREADY filtered to membership-fee accounts via tariff.
-- Same status filter as contract_book_monthly:
--   active=1                                            → current/active
--   cancelled=1 AND cancellation_date IS NOT NULL       → in or past notice
--   active=0 AND cancelled=0 AND start_date > today     → future-signed
-- PLUS the unlinked-future-contract branch (renewal handover gap).
contract_facts AS (
    SELECT
        c.source_id                 AS contract_source_id,
        c.location_source_id,
        COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) AS sold_monthly_fee,
        CAST(c.start_date        AS DATE) AS start_date,
        CAST(c.cancellation_date AS DATE) AS cancellation_date,
        CAST(DATEADD(HOUR, 4, c.start_date) AS DATE) AS effective_start_date,
        pl.capacity,
        pl.list_monthly_fee,
        CASE
            WHEN COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0 THEN 1
            ELSE 0
        END                          AS is_negative_adjustment
    FROM silver.nexudus_contracts c
    INNER JOIN silver.nexudus_tariffs t
        ON  t.source_id  = c.tariff_id
        AND t.is_deleted = 0
    INNER JOIN silver.nexudus_financial_accounts fa
        ON  fa.source_id = t.financial_account_id
        AND fa.is_deleted = 0
    LEFT JOIN contract_product_link pl
        ON pl.contract_source_id = c.source_id
    WHERE c.is_deleted = 0
      AND c.start_date IS NOT NULL
      -- THE Phase 2 FILTER — same rule as the invoice-based view
      AND LOWER(fa.name) LIKE N'%membership fee%'
      -- Status filter — matches vw_landlord_contract_book_monthly
      AND (
          c.active = 1
          OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
          OR (
              c.active = 0
              AND c.cancelled = 0
              AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
          )
      )
      -- Product-link filter — matches vw_landlord_contract_book_monthly.
      -- Accepts any resolved product link (including item_type=4 storeroom/
      -- parking, which contribute 0 capacity but real revenue), negative
      -- adjustments, and unlinked-future renewal handovers.
      AND (
          pl.contract_source_id IS NOT NULL
          OR COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) < 0
          OR (
              c.active = 0
              AND c.cancelled = 0
              AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE)
              AND COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) > 0
          )
      )
),
location_capacity AS (
    -- Time-aware capacity per (location, month) — same rules as the contract-book
    -- view's location_capacity. A product counts toward month M iff:
    --   1. is_available = 1, is_deleted = 0, price > 0
    --   2. effective available_from <= EOMONTH(M)
    --   3. available_to IS NULL OR available_to >= month_start(M)
    -- The price > 0 filter excludes Chair-style €0 placeholders; everything else
    -- (including brand-new just-priced products with no contracts yet) counts.
    -- available_from gets the same UTC end-of-day (+4h) shift as contract
    -- effective_start_date: Nexudus stores "available from D" as D 22:00/23:00
    -- UTC = midnight LOCAL on D+1, so a desk enabled "from Jul 31" belongs to
    -- August. available_to keeps the raw date (through that day, gone after).
    SELECT
        ms.month_start,
        p.location_source_id,
        SUM(
            CASE
                WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                WHEN p.item_type IN (2, 3) THEN 1
                ELSE 0
            END
        ) AS total_workstation_capacity
    FROM month_spine ms
    INNER JOIN silver.nexudus_products p
        ON  p.item_type IN (1, 2, 3)
        AND p.is_deleted = 0
        AND p.is_available = 1
        AND ISNULL(p.price, 0) > 0
        AND (p.available_from IS NULL OR CAST(DATEADD(HOUR, 4, p.available_from) AS DATE) <= EOMONTH(ms.month_start))
        AND (p.available_to   IS NULL OR CAST(p.available_to   AS DATE) >= ms.month_start)
    INNER JOIN silver.nexudus_locations loc
        ON  loc.source_id = p.location_source_id
        AND loc.is_deleted = 0
    GROUP BY ms.month_start, p.location_source_id
),
active_by_month AS (
    SELECT
        ms.month_start,
        cf.location_source_id,
        cf.contract_source_id,
        cf.capacity,
        cf.sold_monthly_fee,
        ISNULL(cf.list_monthly_fee, 0)  AS list_monthly_fee,
        cf.is_negative_adjustment
    FROM month_spine ms
    INNER JOIN contract_facts cf
        ON  cf.effective_start_date <= EOMONTH(ms.month_start)
        AND (
            cf.cancellation_date IS NULL
            OR cf.cancellation_date >= EOMONTH(ms.month_start)
        )
),
monthly_agg AS (
    SELECT
        month_start,
        location_source_id,
        SUM(CASE WHEN is_negative_adjustment = 0 THEN 1 ELSE 0 END) AS active_contract_count,
        SUM(ISNULL(capacity, 0))                                    AS occupied_workstations,
        SUM(sold_monthly_fee)                                       AS sold_monthly_revenue,
        SUM(list_monthly_fee)                                       AS list_monthly_revenue,
        SUM(CASE WHEN is_negative_adjustment = 1 THEN 1 ELSE 0 END) AS adjustment_contract_count,
        SUM(CASE WHEN is_negative_adjustment = 1 THEN sold_monthly_fee ELSE 0 END) AS adjustment_monthly_value
    FROM active_by_month
    GROUP BY month_start, location_source_id
)
SELECT
    FORMAT(ms.month_start, 'yyyy-MM')           AS period,
    ms.month_start                              AS month_start_date,
    ll.location_source_id,
    ll.location_name,
    ll.location_city,
    ll.location_country_name,

    ISNULL(lc.total_workstation_capacity, 0)    AS total_workstation_capacity,
    ISNULL(ma.active_contract_count, 0)         AS active_contract_count,
    ISNULL(ma.occupied_workstations, 0)         AS occupied_workstations,
    CASE
        WHEN ISNULL(lc.total_workstation_capacity, 0) - ISNULL(ma.occupied_workstations, 0) < 0 THEN 0
        ELSE ISNULL(lc.total_workstation_capacity, 0) - ISNULL(ma.occupied_workstations, 0)
    END                                         AS vacant_workstations,
    CAST(
        100.0 * ISNULL(ma.occupied_workstations, 0)
        / NULLIF(ISNULL(lc.total_workstation_capacity, 0), 0)
        AS DECIMAL(9,4)
    )                                           AS occupancy_pct,
    ISNULL(ma.sold_monthly_revenue, 0)          AS sold_monthly_revenue,
    ISNULL(ma.list_monthly_revenue, 0)          AS list_monthly_revenue,
    CAST(
        ISNULL(ma.sold_monthly_revenue, 0) / NULLIF(ISNULL(ma.occupied_workstations, 0), 0)
        AS DECIMAL(18,2)
    )                                           AS avg_sold_price_per_ws,
    CAST(
        ISNULL(ma.list_monthly_revenue, 0) / NULLIF(ISNULL(ma.occupied_workstations, 0), 0)
        AS DECIMAL(18,2)
    )                                           AS avg_list_price_per_ws,
    ISNULL(ma.adjustment_contract_count, 0)     AS adjustment_contract_count,
    CAST(ISNULL(ma.adjustment_monthly_value, 0) AS DECIMAL(18,2)) AS adjustment_monthly_value,
    N'membership_book'                          AS calculation_basis
FROM month_spine ms
CROSS JOIN location_list ll
LEFT JOIN location_capacity lc
    ON  lc.location_source_id = ll.location_source_id
    AND lc.month_start        = ms.month_start
LEFT JOIN monthly_agg ma
    ON  ma.month_start        = ms.month_start
    AND ma.location_source_id = ll.location_source_id;
GO


-- =============================================================================
-- Phase 2: DATA QUALITY — contracts that LOOK like membership fees but are
-- missing their floor_plan_desk_ids / capacity link in Nexudus.
--
-- These are the renewal-handover / new-tenant cases that show up in revenue
-- (because of the unlinked-future-contract branch we added) but contribute
-- 0 to occupancy. Surfaces them so ops can fix the desk assignment in
-- Nexudus and the dashboard occupancy reflects reality.
--
-- Coverage:
--   1. Active or future-signed contracts
--   2. financial_account.name LIKE '%membership fee%'
--   3. EITHER:
--        - no floor_plan_desk_ids at all, OR
--        - has floor_plan_desk_ids but they don't resolve to any item_type
--          1/2/3 product (capacity = 0)
--   4. Positive fee (we ignore the negative-adjustment lines)
--
-- Plus a separate row type for the "tariff without financial account" case
-- so ops can see those too.
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_landlord_data_quality_issues
AS
WITH product_link AS (
    SELECT
        c.source_id AS contract_source_id,
        SUM(
            CASE
                WHEN p.item_type = 1 THEN ISNULL(NULLIF(p.capacity, 0), 1)
                WHEN p.item_type IN (2, 3) THEN 1
                ELSE 0
            END
        ) AS capacity
    FROM silver.nexudus_contracts c
    CROSS APPLY STRING_SPLIT(ISNULL(c.floor_plan_desk_ids, N''), N',') s
    INNER JOIN silver.nexudus_products p
        ON  p.source_id = TRY_CONVERT(BIGINT, TRIM(s.value))
        AND p.item_type IN (1, 2, 3)
        AND p.is_deleted = 0
    WHERE TRIM(s.value) <> N''
      AND c.is_deleted = 0
    GROUP BY c.source_id
)
-- Issue type A: membership-fee contract with no resolvable desks
SELECT
    c.source_id                                  AS contract_source_id,
    c.location_source_id,
    loc.name                                     AS location_name,
    COALESCE(
        NULLIF(c.coworker_company, N''),
        c.coworker_billing_name,
        c.coworker_name
    )                                            AS member_company_name,
    c.coworker_name,
    c.tariff_id,
    t.name                                       AS tariff_name,
    fa.name                                      AS financial_account_name,
    CAST(c.start_date AS DATE)                   AS start_date,
    CAST(c.cancellation_date AS DATE)            AS cancellation_date,
    COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) AS monthly_fee,
    c.floor_plan_desk_ids                        AS desk_ids_raw,
    N'unlinked_membership_contract'              AS issue_type,
    CASE
        WHEN c.floor_plan_desk_ids IS NULL OR c.floor_plan_desk_ids = N''
            THEN N'No floor_plan_desk_ids set'
        ELSE N'floor_plan_desk_ids set but no products resolve (deleted? wrong item_type?)'
    END                                          AS issue_detail,
    c.last_synced_at
FROM silver.nexudus_contracts c
LEFT JOIN silver.nexudus_locations loc
    ON loc.source_id = c.location_source_id
INNER JOIN silver.nexudus_tariffs t
    ON  t.source_id  = c.tariff_id
    AND t.is_deleted = 0
INNER JOIN silver.nexudus_financial_accounts fa
    ON  fa.source_id = t.financial_account_id
    AND fa.is_deleted = 0
LEFT JOIN product_link pl
    ON pl.contract_source_id = c.source_id
WHERE c.is_deleted = 0
  AND LOWER(fa.name) LIKE N'%membership fee%'
  AND COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) > 0
  AND (
      c.active = 1
      OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
      OR (c.active = 0 AND c.cancelled = 0 AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE))
  )
  AND ISNULL(pl.capacity, 0) = 0

UNION ALL

-- Issue type B: tariff with no financial account — ops needs to set one
SELECT
    c.source_id                                  AS contract_source_id,
    c.location_source_id,
    loc.name                                     AS location_name,
    COALESCE(
        NULLIF(c.coworker_company, N''),
        c.coworker_billing_name,
        c.coworker_name
    )                                            AS member_company_name,
    c.coworker_name,
    c.tariff_id,
    t.name                                       AS tariff_name,
    NULL                                         AS financial_account_name,
    CAST(c.start_date AS DATE)                   AS start_date,
    CAST(c.cancellation_date AS DATE)            AS cancellation_date,
    COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) AS monthly_fee,
    c.floor_plan_desk_ids                        AS desk_ids_raw,
    N'tariff_without_financial_account'          AS issue_type,
    N'Tariff exists but has no financial_account_id — Nexudus admin needs to set one' AS issue_detail,
    c.last_synced_at
FROM silver.nexudus_contracts c
LEFT JOIN silver.nexudus_locations loc
    ON loc.source_id = c.location_source_id
INNER JOIN silver.nexudus_tariffs t
    ON  t.source_id  = c.tariff_id
    AND t.is_deleted = 0
    AND t.financial_account_id IS NULL
WHERE c.is_deleted = 0
  AND COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) > 0
  AND (
      c.active = 1
      OR (c.cancelled = 1 AND c.cancellation_date IS NOT NULL)
      OR (c.active = 0 AND c.cancelled = 0 AND CAST(c.start_date AS DATE) > CAST(GETUTCDATE() AS DATE))
  );
GO


-- =============================================================================
-- QA / VALIDATION QUERIES
-- Uncomment and run manually after deployment.
-- =============================================================================

-- ── QA 1: Reconcile against the contract-based forecast for last full month ──
-- The two should be in the same ballpark for a stable month (no one moving
-- in or out mid-month, no large discounts). A delta of >5% suggests either
-- billing adjustments not reflected in contracts, or contracts not yet billed.
/*
WITH last_month AS (
    SELECT FORMAT(DATEADD(MONTH, -1, GETUTCDATE()), 'yyyy-MM') AS period
)
SELECT
    r.location_name,
    r.period,
    r.sold_monthly_revenue                AS invoice_revenue,
    c.sold_monthly_revenue                AS contract_book_revenue,
    r.sold_monthly_revenue - c.sold_monthly_revenue AS delta,
    CAST(
        100.0 * (r.sold_monthly_revenue - c.sold_monthly_revenue)
        / NULLIF(c.sold_monthly_revenue, 0)
        AS DECIMAL(9,2)
    ) AS delta_pct
FROM gold.vw_landlord_revenue_past_location_monthly r
INNER JOIN gold.vw_landlord_contract_book_monthly c
    ON r.location_source_id = c.location_source_id
   AND r.period             = c.period
WHERE r.period = (SELECT period FROM last_month)
ORDER BY ABS(r.sold_monthly_revenue - c.sold_monthly_revenue) DESC;
*/

-- ── QA 2: Sample invoice-line allocations for a single member at one location ──
-- Pick a member who has multi-month billing to verify the day-weighted split.
/*
SELECT TOP 50
    period,
    location_name,
    member_company_name,
    sold_monthly_revenue,
    line_count,
    negative_line_count
FROM gold.vw_landlord_revenue_past_monthly
WHERE location_name LIKE N'%Hoofddorp%'
  AND member_company_name LIKE N'%Cainiao%'
ORDER BY period DESC;
*/

-- ── QA 3: Lines that DIDN'T match the membership-fee filter (audit) ──────────
-- Surfaces line types being excluded — sanity-check the filter doesn't
-- accidentally drop legitimate desk revenue.
/*
SELECT TOP 30
    financial_account_name,
    COUNT(*)                      AS line_count,
    SUM(unit_price * quantity)    AS total_value
FROM silver.nexudus_coworker_invoice_lines il
INNER JOIN silver.nexudus_coworker_invoices i ON i.source_id = il.invoice_source_id
WHERE ISNULL(i.draft, 0) = 0 AND ISNULL(i.void, 0) = 0
  AND LOWER(il.financial_account_name) NOT LIKE N'%membership fee%'
GROUP BY financial_account_name
ORDER BY total_value DESC;
*/

-- ── QA 4: Allocation total = invoice-line total (no money lost in pro-rata) ──
-- For every line, the sum of its monthly allocations should equal its
-- original amount. If the total_days denominator is right this should hold
-- within ±0.01 (rounding to 2 dp).
/*
WITH allocations AS (
    SELECT
        line_source_id,
        SUM(
            line_amount * CAST(overlap_days AS DECIMAL(18,6)) / NULLIF(total_days, 0)
        ) AS allocated_total,
        MAX(line_amount) AS original_total
    FROM (
        -- replicate line_month_allocation here if you want to run this standalone
        SELECT 1 AS line_source_id, 100.0 AS line_amount, 10 AS overlap_days, 30 AS total_days
    ) x
    GROUP BY line_source_id
)
SELECT *, ROUND(allocated_total - original_total, 2) AS rounding_residual FROM allocations
WHERE ABS(allocated_total - original_total) > 0.05;
*/

SELECT
    c.source_id, c.coworker_company, c.tariff_id, t.name AS tariff_name,
    c.active, c.cancelled, c.floor_plan_desk_ids,
    CAST(c.start_date AS DATE) AS start_date,
    CAST(c.cancellation_date AS DATE) AS cancellation_date,
    COALESCE(NULLIF(c.price_with_products, 0), c.price, c.tariff_price, 0) AS fee
FROM silver.nexudus_contracts c
LEFT JOIN silver.nexudus_tariffs t ON t.source_id = c.tariff_id
LEFT JOIN silver.nexudus_locations loc ON loc.source_id = c.location_source_id
WHERE loc.name = 'London - Holborn - 229-231 High Holborn'
  AND c.is_deleted = 0
  AND c.floor_plan_desk_ids IS NOT NULL  -- only contracts with desks
  AND c.floor_plan_desk_ids <> ''
ORDER BY fee DESC, cancellation_date;