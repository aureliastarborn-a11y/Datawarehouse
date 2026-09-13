-- ============================================================================
-- Enterprise Data Warehouse - Google BigQuery Cloud Migration Architecture
-- Target Engine: Google BigQuery
-- Features: Partitioning, Clustering, External GCS Tables, Native BigQuery SQL
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. DATASET CREATION (Run via bq CLI or GCP Console)
-- ----------------------------------------------------------------------------
-- CREATE SCHEMA IF NOT EXISTS `my-gcp-project.staging` OPTIONS(location='US');
-- CREATE SCHEMA IF NOT EXISTS `my-gcp-project.dw` OPTIONS(location='US');
-- CREATE SCHEMA IF NOT EXISTS `my-gcp-project.analytics` OPTIONS(location='US');

-- ----------------------------------------------------------------------------
-- 2. STAGING LAYER (EXTERNAL GCS TABLES)
-- ----------------------------------------------------------------------------

-- External Customer Staging Table over Google Cloud Storage
CREATE OR REPLACE EXTERNAL TABLE `my-gcp-project.staging.stg_customers` (
    raw_customer_id   STRING,
    first_name        STRING,
    last_name         STRING,
    email             STRING,
    customer_tier     STRING,
    city              STRING,
    state             STRING,
    country           STRING,
    updated_at        TIMESTAMP
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://my-company-data-warehouse/raw/customers/*.csv'],
    skip_leading_rows = 1,
    ignore_unknown_values = true
);

-- External Product Staging Table
CREATE OR REPLACE EXTERNAL TABLE `my-gcp-project.staging.stg_products` (
    raw_product_id    STRING,
    product_name      STRING,
    category          STRING,
    subcategory       STRING,
    unit_price        NUMERIC,
    cost_price        NUMERIC,
    updated_at        TIMESTAMP
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://my-company-data-warehouse/raw/products/*.csv'],
    skip_leading_rows = 1,
    ignore_unknown_values = true
);

-- External Orders Staging Table
CREATE OR REPLACE EXTERNAL TABLE `my-gcp-project.staging.stg_orders` (
    order_id          STRING,
    order_line_number INT64,
    customer_id       STRING,
    product_id        STRING,
    order_timestamp   TIMESTAMP,
    quantity          INT64,
    unit_price        NUMERIC,
    discount_amount   NUMERIC,
    tax_amount        NUMERIC
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://my-company-data-warehouse/raw/orders/*.csv'],
    skip_leading_rows = 1,
    ignore_unknown_values = true
);

-- ----------------------------------------------------------------------------
-- 3. DIMENSIONAL LAYER (KIMBALL STAR SCHEMA IN DW DATASET)
-- ----------------------------------------------------------------------------

-- Date Dimension
CREATE TABLE IF NOT EXISTS `my-gcp-project.dw.dim_date` (
    date_key          INT64 PRIMARY KEY NOT ENFORCED,
    full_date         DATE NOT NULL,
    day_name          STRING NOT NULL,
    day_of_week       INT64 NOT NULL,
    day_of_month      INT64 NOT NULL,
    month             INT64 NOT NULL,
    month_name        STRING NOT NULL,
    quarter           INT64 NOT NULL,
    year              INT64 NOT NULL,
    is_weekend        BOOL NOT NULL
);

-- Customer Dimension (Slowly Changing Dimension Type 2)
CREATE TABLE IF NOT EXISTS `my-gcp-project.dw.dim_customer` (
    customer_key         INT64 PRIMARY KEY NOT ENFORCED,
    customer_id          STRING NOT NULL,
    first_name           STRING NOT NULL,
    last_name            STRING NOT NULL,
    email                STRING NOT NULL,
    customer_tier        STRING NOT NULL,
    city                 STRING,
    state                STRING,
    country              STRING,
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date   TIMESTAMP,
    is_current           BOOL NOT NULL
);

-- Product Dimension (Conformed Type 1)
CREATE TABLE IF NOT EXISTS `my-gcp-project.dw.dim_product` (
    product_key       INT64 PRIMARY KEY NOT ENFORCED,
    product_id        STRING NOT NULL,
    product_name      STRING NOT NULL,
    category          STRING NOT NULL,
    subcategory       STRING NOT NULL,
    unit_price        NUMERIC NOT NULL,
    cost_price        NUMERIC NOT NULL,
    is_active         BOOL DEFAULT true,
    updated_at        TIMESTAMP NOT NULL
);

-- Transactional Sales Fact Table
-- Optimized with Range Partitioning on year/month key and Clustering on customer_key & product_key
CREATE TABLE IF NOT EXISTS `my-gcp-project.dw.fact_sales` (
    sales_fact_key    INT64 PRIMARY KEY NOT ENFORCED,
    order_id          STRING NOT NULL,
    order_line_number INT64 NOT NULL,
    customer_key      INT64 NOT NULL REFERENCES `my-gcp-project.dw.dim_customer`(customer_key) NOT ENFORCED,
    product_key       INT64 NOT NULL REFERENCES `my-gcp-project.dw.dim_product`(product_key) NOT ENFORCED,
    order_date_key    INT64 NOT NULL REFERENCES `my-gcp-project.dw.dim_date`(date_key) NOT ENFORCED,
    quantity          INT64 NOT NULL,
    unit_price        NUMERIC NOT NULL,
    gross_amount      NUMERIC NOT NULL,
    discount_amount   NUMERIC NOT NULL,
    tax_amount        NUMERIC NOT NULL,
    net_amount        NUMERIC NOT NULL,
    cost_amount       NUMERIC NOT NULL,
    profit_amount     NUMERIC NOT NULL,
    profit_margin_pct NUMERIC NOT NULL,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY RANGE_BUCKET(order_date_key, GENERATE_ARRAY(20240101, 20261231, 100))
CLUSTER BY customer_key, product_key;
