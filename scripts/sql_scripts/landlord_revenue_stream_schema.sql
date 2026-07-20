-- =============================================================================
-- landlord_revenue_stream_schema.sql            (added 2026-07-16)
--
-- gold.vw_landlord_revenue_stream_past_monthly
--   Invoice-based (BILLED) revenue per (period, location, company), split into
--   two streams so the dashboard can show additional revenue alongside
--   membership revenue:
--     mmrf_billed = MMRF (Monthly Membership Revenues Fee)  — membership-fee accounts
--     marv_billed = MARV (Monthly Additional Revenues Fee)  — ancillary / parking /
--                    storage / meeting-room / day-pass revenue (the "additional
--                    revenues" the membership-fee view deliberately excludes)
--
--   This is the ACTUALS complement of gold.vw_landlord_revenue_past_monthly
--   (which keeps only membership-fee lines). MARV is inherently past/billed —
--   meeting-room bookings and day passes are ad-hoc and NOT forecastable, so
--   this view spans -12..+12 and the dashboard reads MARV from it for
--   past/current months only. Recurring type-4 (parking/storeroom) MARV that
--   IS forecastable comes from the contract-book view
--   (gold.vw_landlord_company_type_book_monthly).
--
--   STREAM CLASSIFIER (on silver invoice-line financial_account_name — validated
--   against live account list 2026-07-16):
--     MMRF     : name LIKE '%membership fee%'  (incl. its DISCOUNT lines)
--     EXCLUDE  : Payments (account_type 2), Service Retainer (deposits,
--                account_type 3), Setup Fees (one-off onboarding), NULL account
--     MARV     : every other revenue account — Ancillary Revenue, Parking Space
--                Fees, Storage Space Fees (incl. their DISCOUNT lines)
--   Account names carry per-location suffixes ((AT),(beyond),(QH),…) so all
--   matching is by substring, not equality.
--
--   Allocation, timezone, invoice-status, and credit-note netting rules are
--   identical to gold.vw_landlord_revenue_past_monthly — see that view's header.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC sp_executesql N'CREATE SCHEMA gold';
GO


CREATE OR ALTER VIEW gold.vw_landlord_revenue_stream_past_monthly
AS
WITH month_offsets AS (
    SELECT -12 AS n
    UNION ALL
    SELECT n + 1 FROM month_offsets WHERE n < 12
),
month_spine AS (
    SELECT
        DATEADD(MONTH, n,     DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)) AS month_start,
        DATEADD(MONTH, n + 1, DATEFROMPARTS(YEAR(GETUTCDATE()), MONTH(GETUTCDATE()), 1)) AS month_end
    FROM month_offsets
),
-- Classify each revenue-bearing invoice line into a stream, dropping
-- non-revenue accounts (payments, service-retainer deposits, setup fees).
filtered_lines AS (
    SELECT
        il.source_id                                  AS line_source_id,
        i.location_source_id,
        i.location_name,
        COALESCE(
            NULLIF(i.coworker_company_name, N''),
            NULLIF(i.bill_to_name,         N''),
            i.coworker_name
        )                                             AS member_company_name,
        i.currency_code,
        CASE
            WHEN LOWER(il.financial_account_name) LIKE N'%membership fee%' THEN 'MMRF'
            ELSE 'MARV'
        END                                           AS revenue_stream,
        CAST(ISNULL(il.unit_price, 0) * ISNULL(il.quantity, 0) AS DECIMAL(18,4)) AS line_amount,
        CASE
            WHEN i.invoice_from_date IS NOT NULL AND i.invoice_to_date IS NOT NULL
                THEN CAST(DATEADD(HOUR, 4, i.invoice_from_date) AS DATE)
            WHEN i.due_date IS NOT NULL
                THEN DATEFROMPARTS(
                    YEAR (CAST(DATEADD(HOUR, 4, i.due_date) AS DATE)),
                    MONTH(CAST(DATEADD(HOUR, 4, i.due_date) AS DATE)), 1)
        END                                           AS effective_from,
        CASE
            WHEN i.invoice_from_date IS NOT NULL AND i.invoice_to_date IS NOT NULL
                THEN CAST(DATEADD(HOUR, 4, i.invoice_to_date) AS DATE)
            WHEN i.due_date IS NOT NULL
                THEN DATEADD(MONTH, 1, DATEFROMPARTS(
                    YEAR (CAST(DATEADD(HOUR, 4, i.due_date) AS DATE)),
                    MONTH(CAST(DATEADD(HOUR, 4, i.due_date) AS DATE)), 1))
        END                                           AS effective_to
    FROM silver.nexudus_coworker_invoice_lines il
    INNER JOIN silver.nexudus_coworker_invoices i
        ON i.source_id = il.invoice_source_id
    WHERE il.financial_account_name IS NOT NULL
      -- Revenue accounts only: drop payments, service-retainer deposits, setup fees.
      AND LOWER(il.financial_account_name) NOT LIKE N'%payment%'
      AND LOWER(il.financial_account_name) NOT LIKE N'%service retainer%'
      AND LOWER(il.financial_account_name) NOT LIKE N'%setup fee%'
      AND LOWER(il.financial_account_name) NOT LIKE N'%deposit%'
      AND ISNULL(i.draft, 0) = 0
      AND ISNULL(i.void,  0) = 0
      AND i.location_source_id IS NOT NULL
      AND (i.invoice_from_date IS NOT NULL OR i.due_date IS NOT NULL)
),
line_month_allocation AS (
    SELECT
        ms.month_start,
        FORMAT(ms.month_start, 'yyyy-MM')             AS period,
        fl.location_source_id,
        fl.location_name,
        fl.member_company_name,
        fl.currency_code,
        fl.revenue_stream,
        fl.line_amount,
        CASE
            WHEN fl.effective_to   <= ms.month_start THEN 0
            WHEN fl.effective_from >= ms.month_end   THEN 0
            ELSE DATEDIFF(DAY,
                CASE WHEN fl.effective_from > ms.month_start THEN fl.effective_from ELSE ms.month_start END,
                CASE WHEN fl.effective_to   < ms.month_end   THEN fl.effective_to   ELSE ms.month_end   END)
        END                                           AS overlap_days,
        DATEDIFF(DAY, fl.effective_from, fl.effective_to) AS total_days
    FROM filtered_lines fl
    INNER JOIN month_spine ms
        ON  fl.effective_from < ms.month_end
        AND fl.effective_to   > ms.month_start
)
SELECT
    lma.period,
    lma.month_start                                   AS month_start_date,
    lma.location_source_id,
    MAX(lma.location_name)                            AS location_name,
    lma.member_company_name,
    MAX(lma.currency_code)                            AS currency_code,
    CAST(SUM(CASE WHEN lma.revenue_stream = 'MMRF'
             THEN lma.line_amount * CAST(lma.overlap_days AS DECIMAL(18,6)) / NULLIF(lma.total_days, 0)
             ELSE 0 END) AS DECIMAL(18,2))            AS mmrf_billed,
    CAST(SUM(CASE WHEN lma.revenue_stream = 'MARV'
             THEN lma.line_amount * CAST(lma.overlap_days AS DECIMAL(18,6)) / NULLIF(lma.total_days, 0)
             ELSE 0 END) AS DECIMAL(18,2))            AS marv_billed
FROM line_month_allocation lma
WHERE lma.total_days   > 0
  AND lma.overlap_days > 0
GROUP BY lma.period, lma.month_start, lma.location_source_id, lma.member_company_name;
GO


-- Convenience roll-up: one row per (period, location) for the chart overlay.
CREATE OR ALTER VIEW gold.vw_landlord_revenue_stream_past_location_monthly
AS
SELECT
    period,
    MIN(month_start_date)                             AS month_start_date,
    location_source_id,
    MAX(location_name)                                AS location_name,
    MAX(currency_code)                                AS currency_code,
    CAST(SUM(mmrf_billed) AS DECIMAL(18,2))           AS mmrf_billed,
    CAST(SUM(marv_billed) AS DECIMAL(18,2))           AS marv_billed
FROM gold.vw_landlord_revenue_stream_past_monthly
GROUP BY period, location_source_id;
GO
