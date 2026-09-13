-- ============================================================================
-- Enterprise Data Warehouse System - Transformations & Pipeline Logic
-- Architecture: Kimball Star Schema ELT with SCD Type 2 Merge
-- Database Target: DuckDB / ANSI SQL Compatible
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. POPULATE DIM_DATE (Role-playing date dimension)
-- ----------------------------------------------------------------------------

INSERT INTO dim_date
SELECT
    CAST(STRFTIME(d, '%Y%m%d') AS INTEGER) AS date_key,
    CAST(d AS DATE) AS full_date,
    STRFTIME(d, '%A') AS day_name,
    EXTRACT(DOW FROM d) + 1 AS day_of_week,
    EXTRACT(DAY FROM d) AS day_of_month,
    EXTRACT(MONTH FROM d) AS month,
    STRFTIME(d, '%B') AS month_name,
    EXTRACT(QUARTER FROM d) AS quarter,
    EXTRACT(YEAR FROM d) AS year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM GENERATE_SERIES(DATE '2024-01-01', DATE '2026-12-31', INTERVAL '1 day') AS s(d)
ON CONFLICT (date_key) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 2. POPULATE DIM_PRODUCT (Type 1 Dimension - Conformed)
-- ----------------------------------------------------------------------------

-- Clean and deduplicate staging products (taking latest record per product_id)
CREATE TEMP TABLE IF NOT EXISTS temp_stg_products AS
WITH ranked_products AS (
    SELECT
        TRIM(raw_product_id) AS product_id,
        TRIM(product_name) AS product_name,
        COALESCE(TRIM(category), 'Unassigned') AS category,
        COALESCE(TRIM(subcategory), 'General') AS subcategory,
        COALESCE(unit_price, 0.00) AS unit_price,
        COALESCE(cost_price, 0.00) AS cost_price,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY raw_product_id ORDER BY updated_at DESC) AS rn
    FROM stg_products
    WHERE raw_product_id IS NOT NULL AND TRIM(raw_product_id) <> ''
)
SELECT product_id, product_name, category, subcategory, unit_price, cost_price, updated_at
FROM ranked_products
WHERE rn = 1;

-- Insert or update dim_product (Type 1 logic: overwrite attributes)
INSERT INTO dim_product (product_key, product_id, product_name, category, subcategory, unit_price, cost_price, is_active, updated_at)
SELECT
    ROW_NUMBER() OVER () + COALESCE((SELECT MAX(product_key) FROM dim_product), 0) AS product_key,
    stg.product_id,
    stg.product_name,
    stg.category,
    stg.subcategory,
    stg.unit_price,
    stg.cost_price,
    TRUE AS is_active,
    stg.updated_at
FROM temp_stg_products stg
WHERE NOT EXISTS (
    SELECT 1 FROM dim_product dp WHERE dp.product_id = stg.product_id
);

-- Update existing records if prices or details changed (SCD Type 1)
UPDATE dim_product
SET product_name = stg.product_name,
    category = stg.category,
    subcategory = stg.subcategory,
    unit_price = stg.unit_price,
    cost_price = stg.cost_price,
    updated_at = stg.updated_at
FROM temp_stg_products stg
WHERE dim_product.product_id = stg.product_id
  AND (dim_product.unit_price <> stg.unit_price OR dim_product.cost_price <> stg.cost_price OR dim_product.product_name <> stg.product_name);

-- ----------------------------------------------------------------------------
-- 3. POPULATE DIM_CUSTOMER (Slowly Changing Dimension Type 2)
-- ----------------------------------------------------------------------------

-- Step A: Clean and standardize raw customer staging data
CREATE TEMP TABLE IF NOT EXISTS temp_stg_customers AS
WITH cleaned_customers AS (
    SELECT
        TRIM(raw_customer_id) AS customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        COALESCE(TRIM(customer_tier), 'Standard') AS customer_tier,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(country) AS country,
        COALESCE(updated_at, CURRENT_TIMESTAMP) AS updated_at,
        ROW_NUMBER() OVER (PARTITION BY raw_customer_id ORDER BY updated_at DESC) AS rn
    FROM stg_customers
    WHERE raw_customer_id IS NOT NULL AND TRIM(raw_customer_id) <> ''
)
SELECT customer_id, first_name, last_name, email, customer_tier, city, state, country, updated_at
FROM cleaned_customers
WHERE rn = 1;

-- Step B: Identify existing active records that have changed attributes (SCD2 updates)
CREATE TEMP TABLE IF NOT EXISTS temp_customers_to_expire AS
SELECT
    dc.customer_key,
    dc.customer_id,
    stg.updated_at AS effective_end_date
FROM dim_customer dc
JOIN temp_stg_customers stg ON dc.customer_id = stg.customer_id
WHERE dc.is_current = TRUE
  AND (
      dc.first_name <> stg.first_name OR
      dc.last_name <> stg.last_name OR
      dc.email <> stg.email OR
      dc.customer_tier <> stg.customer_tier OR
      COALESCE(dc.city, '') <> COALESCE(stg.city, '') OR
      COALESCE(dc.state, '') <> COALESCE(stg.state, '') OR
      COALESCE(dc.country, '') <> COALESCE(stg.country, '')
  );

-- Expire existing changed records
UPDATE dim_customer
SET effective_end_date = exp.effective_end_date,
    is_current = FALSE
FROM temp_customers_to_expire exp
WHERE dim_customer.customer_key = exp.customer_key;

-- Step C: Insert new versions for changed records AND net-new customer records
CREATE TEMP TABLE IF NOT EXISTS temp_new_customer_records AS
SELECT
    stg.customer_id,
    stg.first_name,
    stg.last_name,
    stg.email,
    stg.customer_tier,
    stg.city,
    stg.state,
    stg.country,
    stg.updated_at AS effective_start_date,
    CAST(NULL AS TIMESTAMP) AS effective_end_date,
    TRUE AS is_current
FROM temp_stg_customers stg
LEFT JOIN dim_customer dc ON stg.customer_id = dc.customer_id AND dc.is_current = TRUE
WHERE dc.customer_key IS NULL -- Net new customers OR old record was expired in Step B above
   OR EXISTS (SELECT 1 FROM temp_customers_to_expire exp WHERE exp.customer_id = stg.customer_id);

INSERT INTO dim_customer (customer_key, customer_id, first_name, last_name, email, customer_tier, city, state, country, effective_start_date, effective_end_date, is_current)
SELECT
    COALESCE((SELECT MAX(customer_key) FROM dim_customer), 0) + ROW_NUMBER() OVER () AS customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    customer_tier,
    city,
    state,
    country,
    effective_start_date,
    effective_end_date,
    is_current
FROM temp_new_customer_records;

-- ----------------------------------------------------------------------------
-- 4. POPULATE FACT_SALES (Transactional Fact Table)
-- ----------------------------------------------------------------------------

-- Filter and enrich raw orders against dimension surrogate keys
CREATE TEMP TABLE IF NOT EXISTS temp_clean_orders AS
WITH deduplicated_orders AS (
    SELECT
        TRIM(o.order_id) AS order_id,
        o.order_line_number,
        TRIM(o.customer_id) AS customer_id,
        TRIM(o.product_id) AS product_id,
        o.order_timestamp,
        o.quantity,
        o.unit_price,
        COALESCE(o.discount_amount, 0.00) AS discount_amount,
        COALESCE(o.tax_amount, 0.00) AS tax_amount,
        ROW_NUMBER() OVER (PARTITION BY o.order_id, o.order_line_number ORDER BY o.order_timestamp DESC) AS rn
    FROM stg_orders o
    WHERE o.order_id IS NOT NULL 
      AND o.customer_id IS NOT NULL 
      AND o.product_id IS NOT NULL
      AND o.quantity > 0
)
SELECT order_id, order_line_number, customer_id, product_id, order_timestamp, quantity, unit_price, discount_amount, tax_amount
FROM deduplicated_orders
WHERE rn = 1;

-- Load into fact_sales with point-in-time customer SCD2 join
INSERT INTO fact_sales (
    sales_fact_key,
    order_id,
    order_line_number,
    customer_key,
    product_key,
    order_date_key,
    quantity,
    unit_price,
    gross_amount,
    discount_amount,
    tax_amount,
    net_amount,
    cost_amount,
    profit_amount,
    profit_margin_pct
)
SELECT
    COALESCE((SELECT MAX(sales_fact_key) FROM fact_sales), 0) + ROW_NUMBER() OVER () AS sales_fact_key,
    o.order_id,
    o.order_line_number,
    c.customer_key,
    p.product_key,
    CAST(STRFTIME(o.order_timestamp, '%Y%m%d') AS INTEGER) AS order_date_key,
    o.quantity,
    o.unit_price,
    ROUND(o.quantity * o.unit_price, 2) AS gross_amount,
    ROUND(o.discount_amount, 2) AS discount_amount,
    ROUND(o.tax_amount, 2) AS tax_amount,
    ROUND((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount, 2) AS net_amount,
    ROUND(o.quantity * p.cost_price, 2) AS cost_amount,
    ROUND(((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount) - (o.quantity * p.cost_price), 2) AS profit_amount,
    ROUND(
        (
            ((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount) - (o.quantity * p.cost_price)
        ) / NULLIF(((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount), 0) * 100,
        2
    ) AS profit_margin_pct
FROM temp_clean_orders o
-- Point-in-Time Join to Dim_Customer (SCD Type 2 logic)
JOIN dim_customer c
  ON o.customer_id = c.customer_id
 AND o.order_timestamp >= c.effective_start_date
 AND (c.effective_end_date IS NULL OR o.order_timestamp < c.effective_end_date)
-- Join to Dim_Product
JOIN dim_product p
  ON o.product_id = p.product_id
-- Avoid duplicating orders already loaded in fact_sales
WHERE NOT EXISTS (
    SELECT 1 FROM fact_sales fs 
    WHERE fs.order_id = o.order_id 
      AND fs.order_line_number = o.order_line_number
);
