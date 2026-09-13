-- ============================================================================
-- Enterprise Data Warehouse - Snowflake Cloud Migration Architecture
-- Target Engine: Snowflake Data Cloud
-- Features: External Stages, File Formats, Clustering Keys, COPY INTO Pipelines
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. INFRASTRUCTURE & ENVIRONMENT SETUP
-- ----------------------------------------------------------------------------

-- Create Virtual Warehouse (Auto-suspend after 60s, Auto-resume)
CREATE WAREHOUSE IF NOT EXISTS ECOM_DW_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Virtual Warehouse for E-Commerce Data Warehouse Processing';

-- Create Database
CREATE DATABASE IF NOT EXISTS ECOM_DATA_WAREHOUSE;
USE DATABASE ECOM_DATA_WAREHOUSE;

-- Create Schemas for Multi-Layer Architecture
CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'Raw file ingestion and cleaning layer';
CREATE SCHEMA IF NOT EXISTS DW COMMENT = 'Kimball Dimensional Model (Star Schema)';
CREATE SCHEMA IF NOT EXISTS ANALYTICS COMMENT = 'Business Intelligence & Reporting Marts';

USE WAREHOUSE ECOM_DW_WH;

-- ----------------------------------------------------------------------------
-- 2. FILE FORMAT & EXTERNAL STAGE SETUP
-- ----------------------------------------------------------------------------

USE SCHEMA STAGING;

-- Create Reusable CSV File Format
CREATE OR REPLACE FILE FORMAT CSV_RAW_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS';

-- Create External Stage linked to AWS S3 Bucket
-- (Requires IAM Storage Integration setup in Production)
CREATE OR REPLACE STAGE S3_RAW_STAGE
    URL = 's3://my-company-data-warehouse-bucket/raw/'
    FILE_FORMAT = CSV_RAW_FORMAT;

-- ----------------------------------------------------------------------------
-- 3. STAGING TABLES (RAW INGESTION)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE STAGING.STG_CUSTOMERS (
    RAW_CUSTOMER_ID   VARCHAR(50),
    FIRST_NAME        VARCHAR(100),
    LAST_NAME         VARCHAR(100),
    EMAIL             VARCHAR(255),
    CUSTOMER_TIER     VARCHAR(50),
    CITY              VARCHAR(100),
    STATE             VARCHAR(50),
    COUNTRY           VARCHAR(50),
    UPDATED_AT        TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE STAGING.STG_PRODUCTS (
    RAW_PRODUCT_ID    VARCHAR(50),
    PRODUCT_NAME      VARCHAR(255),
    CATEGORY          VARCHAR(100),
    SUBCATEGORY       VARCHAR(100),
    UNIT_PRICE        NUMBER(12, 2),
    COST_PRICE        NUMBER(12, 2),
    UPDATED_AT        TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE STAGING.STG_ORDERS (
    ORDER_ID          VARCHAR(50),
    ORDER_LINE_NUMBER NUMBER(38, 0),
    CUSTOMER_ID       VARCHAR(50),
    PRODUCT_ID        VARCHAR(50),
    ORDER_TIMESTAMP   TIMESTAMP_NTZ,
    QUANTITY          NUMBER(38, 0),
    UNIT_PRICE        NUMBER(12, 2),
    DISCOUNT_AMOUNT   NUMBER(12, 2),
    TAX_AMOUNT        NUMBER(12, 2)
);

-- ----------------------------------------------------------------------------
-- 4. COPY INTO AUTOMATED INGESTION PIPELINES
-- ----------------------------------------------------------------------------

-- Copy Customer Batches into Staging
COPY INTO STAGING.STG_CUSTOMERS
FROM @S3_RAW_STAGE/customers/
PATTERN = '.*raw_customers.*\\.csv'
ON_ERROR = 'CONTINUE';

-- Copy Products into Staging
COPY INTO STAGING.STG_PRODUCTS
FROM @S3_RAW_STAGE/products/
PATTERN = '.*raw_products.*\\.csv'
ON_ERROR = 'CONTINUE';

-- Copy Orders into Staging
COPY INTO STAGING.STG_ORDERS
FROM @S3_RAW_STAGE/orders/
PATTERN = '.*raw_orders.*\\.csv'
ON_ERROR = 'CONTINUE';

-- ----------------------------------------------------------------------------
-- 5. DIMENSIONAL LAYER (KIMBALL STAR SCHEMA IN DW SCHEMA)
-- ----------------------------------------------------------------------------

USE SCHEMA DW;

-- Date Dimension
CREATE TABLE IF NOT EXISTS DW.DIM_DATE (
    DATE_KEY          NUMBER(8, 0) PRIMARY KEY,
    FULL_DATE         DATE NOT NULL,
    DAY_NAME          VARCHAR(10) NOT NULL,
    DAY_OF_WEEK       NUMBER(2, 0) NOT NULL,
    DAY_OF_MONTH      NUMBER(2, 0) NOT NULL,
    MONTH             NUMBER(2, 0) NOT NULL,
    MONTH_NAME        VARCHAR(10) NOT NULL,
    QUARTER           NUMBER(1, 0) NOT NULL,
    YEAR              NUMBER(4, 0) NOT NULL,
    IS_WEEKEND        BOOLEAN NOT NULL
);

-- Customer Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS DW.DIM_CUSTOMER (
    CUSTOMER_KEY         NUMBER(38, 0) PRIMARY KEY,
    CUSTOMER_ID          VARCHAR(50) NOT NULL,
    FIRST_NAME           VARCHAR(100) NOT NULL,
    LAST_NAME            VARCHAR(100) NOT NULL,
    EMAIL                VARCHAR(255) NOT NULL,
    CUSTOMER_TIER        VARCHAR(50) NOT NULL,
    CITY                 VARCHAR(100),
    STATE                VARCHAR(50),
    COUNTRY              VARCHAR(50),
    EFFECTIVE_START_DATE TIMESTAMP_NTZ NOT NULL,
    EFFECTIVE_END_DATE   TIMESTAMP_NTZ,
    IS_CURRENT           BOOLEAN NOT NULL
);

-- Product Dimension (Conformed SCD Type 1)
CREATE TABLE IF NOT EXISTS DW.DIM_PRODUCT (
    PRODUCT_KEY       NUMBER(38, 0) PRIMARY KEY,
    PRODUCT_ID        VARCHAR(50) NOT NULL,
    PRODUCT_NAME      VARCHAR(255) NOT NULL,
    CATEGORY          VARCHAR(100) NOT NULL,
    SUBCATEGORY       VARCHAR(100) NOT NULL,
    UNIT_PRICE        NUMBER(12, 2) NOT NULL,
    COST_PRICE        NUMBER(12, 2) NOT NULL,
    IS_ACTIVE         BOOLEAN DEFAULT TRUE,
    UPDATED_AT        TIMESTAMP_NTZ NOT NULL
);

-- Transactional Sales Fact Table
-- Micro-partition Optimization: Clustered by ORDER_DATE_KEY for fast range queries
CREATE TABLE IF NOT EXISTS DW.FACT_SALES (
    SALES_FACT_KEY    NUMBER(38, 0) PRIMARY KEY,
    ORDER_ID          VARCHAR(50) NOT NULL,
    ORDER_LINE_NUMBER NUMBER(38, 0) NOT NULL,
    CUSTOMER_KEY      NUMBER(38, 0) NOT NULL FOREIGN KEY REFERENCES DW.DIM_CUSTOMER(CUSTOMER_KEY),
    PRODUCT_KEY       NUMBER(38, 0) NOT NULL FOREIGN KEY REFERENCES DW.DIM_PRODUCT(PRODUCT_KEY),
    ORDER_DATE_KEY    NUMBER(8, 0) NOT NULL FOREIGN KEY REFERENCES DW.DIM_DATE(DATE_KEY),
    QUANTITY          NUMBER(38, 0) NOT NULL,
    UNIT_PRICE        NUMBER(12, 2) NOT NULL,
    GROSS_AMOUNT      NUMBER(12, 2) NOT NULL,
    DISCOUNT_AMOUNT   NUMBER(12, 2) NOT NULL,
    TAX_AMOUNT        NUMBER(12, 2) NOT NULL,
    NET_AMOUNT        NUMBER(12, 2) NOT NULL,
    COST_AMOUNT       NUMBER(12, 2) NOT NULL,
    PROFIT_AMOUNT     NUMBER(12, 2) NOT NULL,
    PROFIT_MARGIN_PCT NUMBER(5, 2) NOT NULL,
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ORDER_DATE_KEY);
