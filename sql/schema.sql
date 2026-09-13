-- ============================================================================
-- Enterprise Data Warehouse System - DDL Schema Definition
-- Architecture: Kimball Star Schema Methodology
-- Database Target: DuckDB / ANSI SQL Compatible
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. STAGING LAYER (Raw Ingestion Tables)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stg_customers (
    raw_customer_id   VARCHAR,
    first_name        VARCHAR,
    last_name         VARCHAR,
    email             VARCHAR,
    customer_tier     VARCHAR,
    city              VARCHAR,
    state             VARCHAR,
    country           VARCHAR,
    updated_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_products (
    raw_product_id    VARCHAR,
    product_name      VARCHAR,
    category          VARCHAR,
    subcategory       VARCHAR,
    unit_price        DECIMAL(12, 2),
    cost_price        DECIMAL(12, 2),
    updated_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id          VARCHAR,
    order_line_number INTEGER,
    customer_id       VARCHAR,
    product_id        VARCHAR,
    order_timestamp   TIMESTAMP,
    quantity          INTEGER,
    unit_price        DECIMAL(12, 2),
    discount_amount   DECIMAL(12, 2),
    tax_amount        DECIMAL(12, 2)
);

-- ----------------------------------------------------------------------------
-- 2. DIMENSIONAL DATA WAREHOUSE LAYER (Star Schema)
-- ----------------------------------------------------------------------------

-- Date Dimension (Role-playing dimension)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key          INTEGER PRIMARY KEY, -- Format: YYYYMMDD
    full_date         DATE NOT NULL,
    day_name          VARCHAR(10) NOT NULL,
    day_of_week       INTEGER NOT NULL,   -- 1 = Monday, 7 = Sunday
    day_of_month      INTEGER NOT NULL,
    month             INTEGER NOT NULL,
    month_name        VARCHAR(10) NOT NULL,
    quarter           INTEGER NOT NULL,
    year              INTEGER NOT NULL,
    is_weekend        BOOLEAN NOT NULL
);

-- Customer Dimension (Slowly Changing Dimension Type 2)
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key         BIGINT PRIMARY KEY, -- Surrogate Key
    customer_id          VARCHAR NOT NULL,   -- Natural Key / Business Key
    first_name           VARCHAR NOT NULL,
    last_name            VARCHAR NOT NULL,
    email                VARCHAR NOT NULL,
    customer_tier        VARCHAR NOT NULL,   -- e.g., Bronze, Silver, Gold, Platinum
    city                 VARCHAR,
    state                VARCHAR,
    country              VARCHAR,
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date   TIMESTAMP,           -- NULL indicates current record
    is_current           BOOLEAN NOT NULL     -- TRUE for active profile
);

-- Product Dimension (Type 1 Dimension - Overwrite)
CREATE TABLE IF NOT EXISTS dim_product (
    product_key       BIGINT PRIMARY KEY, -- Surrogate Key
    product_id        VARCHAR NOT NULL UNIQUE, -- Natural Key
    product_name      VARCHAR NOT NULL,
    category          VARCHAR NOT NULL,
    subcategory       VARCHAR NOT NULL,
    unit_price        DECIMAL(12, 2) NOT NULL,
    cost_price        DECIMAL(12, 2) NOT NULL,
    is_active         BOOLEAN DEFAULT TRUE,
    updated_at        TIMESTAMP NOT NULL
);

-- Transactional Sales Fact Table
-- Grain: One record per individual order line item
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_fact_key    BIGINT PRIMARY KEY, -- Surrogate Fact Key
    order_id          VARCHAR NOT NULL,
    order_line_number INTEGER NOT NULL,
    customer_key      BIGINT NOT NULL,    -- Foreign Key to dim_customer (SCD2 point-in-time lookup)
    product_key       BIGINT NOT NULL,    -- Foreign Key to dim_product
    order_date_key    INTEGER NOT NULL,   -- Foreign Key to dim_date
    quantity          INTEGER NOT NULL,
    unit_price        DECIMAL(12, 2) NOT NULL,
    gross_amount      DECIMAL(12, 2) NOT NULL,
    discount_amount   DECIMAL(12, 2) NOT NULL,
    tax_amount        DECIMAL(12, 2) NOT NULL,
    net_amount        DECIMAL(12, 2) NOT NULL, -- Gross - Discount + Tax
    cost_amount       DECIMAL(12, 2) NOT NULL, -- Quantity * Cost Price
    profit_amount     DECIMAL(12, 2) NOT NULL, -- Net Amount - Cost Amount
    profit_margin_pct DECIMAL(5, 2) NOT NULL,  -- (Profit Amount / Net Amount) * 100
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
