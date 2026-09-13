-- ============================================================================
-- Enterprise Data Warehouse System - Analytical Reporting Suite
-- 3 Business-Critical Analytics Queries Built On Top of Dimensional Model
-- Database Target: DuckDB / ANSI SQL Compatible
-- ============================================================================

-- ============================================================================
-- QUERY 1: Month-over-Month (MoM) Revenue & Profit Margin Trend Analysis
-- Business Value: Tracks financial health, gross/net sales growth, cost efficiency,
-- and percentage change month-over-month.
-- ============================================================================

WITH monthly_sales AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(f.sales_fact_key) AS total_order_items,
        SUM(f.quantity) AS total_units_sold,
        SUM(f.gross_amount) AS total_gross_revenue,
        SUM(f.discount_amount) AS total_discounts,
        SUM(f.net_amount) AS total_net_revenue,
        SUM(f.cost_amount) AS total_cost,
        SUM(f.profit_amount) AS total_profit,
        ROUND((SUM(f.profit_amount) / NULLIF(SUM(f.net_amount), 0)) * 100, 2) AS overall_profit_margin_pct
    FROM fact_sales f
    JOIN dim_date d ON f.order_date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
)
SELECT
    year,
    month,
    month_name,
    total_orders,
    total_units_sold,
    total_gross_revenue,
    total_discounts,
    total_net_revenue,
    LAG(total_net_revenue) OVER (ORDER BY year, month) AS prior_month_net_revenue,
    ROUND(
        (total_net_revenue - LAG(total_net_revenue) OVER (ORDER BY year, month)) 
        / NULLIF(LAG(total_net_revenue) OVER (ORDER BY year, month), 0) * 100,
        2
    ) AS mom_net_revenue_growth_pct,
    total_profit,
    overall_profit_margin_pct
FROM monthly_sales
ORDER BY year, month;


-- ============================================================================
-- QUERY 2: Customer Cohort Retention & Lifetime Value (LTV) Analysis
-- Business Value: Group customers by the month of their first purchase date
-- and analyze cohort growth, cumulative LTV, and retention across periods.
-- ============================================================================

WITH customer_first_purchase AS (
    SELECT
        f.customer_key,
        c.customer_id,
        MIN(d.full_date) AS first_order_date,
        STRFTIME(MIN(d.full_date), '%Y-%m') AS cohort_month
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    JOIN dim_date d ON f.order_date_key = d.date_key
    GROUP BY f.customer_key, c.customer_id
),
cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS total_cohort_customers
    FROM customer_first_purchase
    GROUP BY cohort_month
),
customer_monthly_activity AS (
    SELECT
        cfp.cohort_month,
        STRFTIME(d.full_date, '%Y-%m') AS activity_month,
        f.customer_key,
        SUM(f.net_amount) AS monthly_net_spend
    FROM fact_sales f
    JOIN customer_first_purchase cfp ON f.customer_key = cfp.customer_key
    JOIN dim_date d ON f.order_date_key = d.date_key
    GROUP BY cfp.cohort_month, STRFTIME(d.full_date, '%Y-%m'), f.customer_key
)
SELECT
    act.cohort_month,
    cs.total_cohort_customers,
    act.activity_month,
    COUNT(DISTINCT act.customer_key) AS active_retained_customers,
    ROUND(COUNT(DISTINCT act.customer_key) * 100.0 / cs.total_cohort_customers, 2) AS retention_rate_pct,
    SUM(act.monthly_net_spend) AS cohort_period_revenue,
    ROUND(SUM(act.monthly_net_spend) / cs.total_cohort_customers, 2) AS avg_revenue_per_cohort_member
FROM customer_monthly_activity act
JOIN cohort_size cs ON act.cohort_month = cs.cohort_month
GROUP BY act.cohort_month, cs.total_cohort_customers, act.activity_month
ORDER BY act.cohort_month, act.activity_month;


-- ============================================================================
-- QUERY 3: RFM (Recency, Frequency, Monetary) Customer Segmentation 
-- Business Value: Segment active customer profiles based on recent purchase behavior,
-- transaction count, and total net spend to target VIPs, At-Risk, & New Buyers.
-- ============================================================================

WITH max_warehouse_date AS (
    SELECT MAX(full_date) AS current_date FROM dim_date WHERE date_key IN (SELECT order_date_key FROM fact_sales)
),
customer_rfm_metrics AS (
    SELECT
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        c.customer_tier,
        c.city,
        c.country,
        DATEDIFF('day', MAX(d.full_date), (SELECT current_date FROM max_warehouse_date)) AS recency_days,
        COUNT(DISTINCT f.order_id) AS frequency_orders,
        SUM(f.net_amount) AS monetary_spend,
        SUM(f.profit_amount) AS total_customer_profit
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    JOIN dim_date d ON f.order_date_key = d.date_key
    WHERE c.is_current = TRUE -- Evaluate active demographic profile
    GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_tier, c.city, c.country
),
rfm_scores AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY recency_days ASC) AS r_score,     -- Lower days = higher score
        NTILE(4) OVER (ORDER BY frequency_orders DESC) AS f_score, -- Higher orders = higher score
        NTILE(4) OVER (ORDER BY monetary_spend DESC) AS m_score    -- Higher spend = higher score
    FROM customer_rfm_metrics
),
segmented_customers AS (
    SELECT
        *,
        CASE
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Champions (VIP)'
            WHEN f_score >= 3 AND m_score >= 2 THEN 'Loyal Customers'
            WHEN r_score >= 3 AND f_score <= 2 THEN 'Recent New Customers'
            WHEN r_score <= 2 AND f_score >= 2 THEN 'At Risk Customers'
            ELSE 'Hibernating / Lost'
        END AS rfm_segment
    FROM rfm_scores
)
SELECT
    rfm_segment,
    COUNT(customer_id) AS total_customers,
    ROUND(AVG(recency_days), 1) AS avg_days_since_last_order,
    ROUND(AVG(frequency_orders), 1) AS avg_orders_per_customer,
    ROUND(AVG(monetary_spend), 2) AS avg_customer_spend,
    ROUND(SUM(monetary_spend), 2) AS total_segment_revenue,
    ROUND(SUM(total_customer_profit), 2) AS total_segment_profit
FROM segmented_customers
GROUP BY rfm_segment
ORDER BY total_segment_revenue DESC;
