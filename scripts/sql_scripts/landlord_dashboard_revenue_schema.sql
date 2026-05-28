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
--   2. otherwise → fall back to due_date and allocate the whole line to
--                  that calendar month (effective_from = month_start of
--                  due_date, effective_to = month_start of next month)
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
        -- Branch 2: due_date present → whole line lands in due_date's calendar month.
        CASE
            WHEN i.invoice_from_date IS NOT NULL AND i.invoice_to_date IS NOT NULL
                THEN CAST(DATEADD(HOUR, 4, i.invoice_from_date) AS DATE)
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
      AND i.location_source_id IS NOT NULL
      -- Must have some kind of dating
      AND (i.invoice_from_date IS NOT NULL OR i.due_date IS NOT NULL)
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


