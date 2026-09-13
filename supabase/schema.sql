-- ============================================================================
-- Real Data Warehouse - Supabase PostgreSQL DDL
-- Architecture: Kimball Star Schema with ETL/ELT Pipeline
-- Target: Supabase (PostgreSQL 15+)
-- Domains: Stocks/Finance, Weather, News
-- ============================================================================

-- ============================================================================
-- 0. EXTENSIONS
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- 1. STAGING / RAW INGESTION LAYER
-- ============================================================================

-- Raw stock price data from Alpha Vantage API
CREATE TABLE IF NOT EXISTS raw_stock_prices (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    ticker          VARCHAR(10) NOT NULL,
    trade_date      DATE NOT NULL,
    open_price      NUMERIC(12, 4),
    high_price      NUMERIC(12, 4),
    low_price       NUMERIC(12, 4),
    close_price     NUMERIC(12, 4),
    volume          BIGINT,
    source          VARCHAR(50) DEFAULT 'alpha_vantage',
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    pipeline_run_id UUID,
    UNIQUE(ticker, trade_date, source)
);

-- Raw weather observation data from OpenWeatherMap API
CREATE TABLE IF NOT EXISTS raw_weather_observations (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    country_code    VARCHAR(10),
    observation_dt  TIMESTAMPTZ NOT NULL,
    temp_celsius    NUMERIC(6, 2),
    feels_like_c    NUMERIC(6, 2),
    temp_min_c      NUMERIC(6, 2),
    temp_max_c      NUMERIC(6, 2),
    pressure_hpa    INTEGER,
    humidity_pct    INTEGER,
    wind_speed_ms   NUMERIC(6, 2),
    wind_deg        INTEGER,
    clouds_pct      INTEGER,
    weather_main    VARCHAR(50),
    weather_desc    VARCHAR(200),
    visibility_m    INTEGER,
    rain_1h_mm      NUMERIC(8, 2) DEFAULT 0,
    snow_1h_mm      NUMERIC(8, 2) DEFAULT 0,
    latitude        NUMERIC(10, 6),
    longitude       NUMERIC(10, 6),
    source          VARCHAR(50) DEFAULT 'openweathermap',
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    pipeline_run_id UUID
);

-- Raw news articles from NewsAPI
CREATE TABLE IF NOT EXISTS raw_news_articles (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    source_id       VARCHAR(100),
    source_name     VARCHAR(200),
    author          VARCHAR(300),
    title           TEXT NOT NULL,
    description     TEXT,
    url             TEXT,
    url_to_image    TEXT,
    published_at    TIMESTAMPTZ,
    content         TEXT,
    topic           VARCHAR(100),
    source_api      VARCHAR(50) DEFAULT 'newsapi',
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    pipeline_run_id UUID,
    UNIQUE(url)
);

-- Pipeline audit/lineage table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    run_type        VARCHAR(50) NOT NULL,       -- 'full', 'incremental', 'stocks', 'weather', 'news'
    status          VARCHAR(20) NOT NULL,       -- 'running', 'completed', 'failed'
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    records_extracted   INTEGER DEFAULT 0,
    records_loaded      INTEGER DEFAULT 0,
    records_transformed INTEGER DEFAULT 0,
    records_rejected    INTEGER DEFAULT 0,
    error_message   TEXT,
    metadata        JSONB DEFAULT '{}'::JSONB
);

-- Data quality issue log
CREATE TABLE IF NOT EXISTS data_quality_log (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    pipeline_run_id UUID REFERENCES pipeline_runs(id),
    table_name      VARCHAR(100) NOT NULL,
    check_name      VARCHAR(200) NOT NULL,
    check_type      VARCHAR(50) NOT NULL,       -- 'null_check', 'range_check', 'duplicate_check', 'referential_integrity'
    records_checked INTEGER DEFAULT 0,
    records_failed  INTEGER DEFAULT 0,
    severity        VARCHAR(20) DEFAULT 'warning',  -- 'info', 'warning', 'error', 'critical'
    details         TEXT,
    checked_at      TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================================
-- 2. KIMBALL STAR SCHEMA - DIMENSION TABLES
-- ============================================================================

-- Date Dimension (Role-playing dimension, pre-populated)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,        -- Format: YYYYMMDD
    full_date       DATE NOT NULL UNIQUE,
    day_name        VARCHAR(10) NOT NULL,
    day_of_week     INTEGER NOT NULL,           -- 1=Monday, 7=Sunday (ISO)
    day_of_month    INTEGER NOT NULL,
    day_of_year     INTEGER NOT NULL,
    week_of_year    INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    quarter         INTEGER NOT NULL,
    year            INTEGER NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_month_start  BOOLEAN NOT NULL,
    is_month_end    BOOLEAN NOT NULL,
    fiscal_quarter  INTEGER NOT NULL,
    fiscal_year     INTEGER NOT NULL
);

-- Company / Ticker Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_company (
    company_key         BIGSERIAL PRIMARY KEY,
    ticker              VARCHAR(10) NOT NULL,
    company_name        VARCHAR(200),
    sector              VARCHAR(100),
    industry            VARCHAR(200),
    market_cap_tier     VARCHAR(20),            -- 'mega', 'large', 'mid', 'small', 'micro'
    effective_start_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_end_date   TIMESTAMPTZ,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Location Dimension (for weather data)
CREATE TABLE IF NOT EXISTS dim_location (
    location_key    BIGSERIAL PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    country_code    VARCHAR(10),
    latitude        NUMERIC(10, 6),
    longitude       NUMERIC(10, 6),
    timezone        VARCHAR(50),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(city, country_code)
);

-- News Source Dimension
CREATE TABLE IF NOT EXISTS dim_news_source (
    source_key      BIGSERIAL PRIMARY KEY,
    source_id       VARCHAR(100),
    source_name     VARCHAR(200) NOT NULL,
    source_url      TEXT,
    category        VARCHAR(100),
    language        VARCHAR(10) DEFAULT 'en',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_name)
);


-- ============================================================================
-- 3. KIMBALL STAR SCHEMA - FACT TABLES
-- ============================================================================

-- Stock Price Fact Table
-- Grain: One row per ticker per trading day
CREATE TABLE IF NOT EXISTS fact_stock_prices (
    stock_fact_key      BIGSERIAL PRIMARY KEY,
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    company_key         BIGINT NOT NULL REFERENCES dim_company(company_key),
    open_price          NUMERIC(12, 4) NOT NULL,
    high_price          NUMERIC(12, 4) NOT NULL,
    low_price           NUMERIC(12, 4) NOT NULL,
    close_price         NUMERIC(12, 4) NOT NULL,
    volume              BIGINT NOT NULL,
    daily_return_pct    NUMERIC(8, 4),          -- (close - prev_close) / prev_close * 100
    daily_range         NUMERIC(12, 4),         -- high - low
    daily_range_pct     NUMERIC(8, 4),          -- (high - low) / low * 100
    avg_price           NUMERIC(12, 4),         -- (open + high + low + close) / 4
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date_key, company_key)
);

-- Weather Reading Fact Table
-- Grain: One row per city per observation timestamp
CREATE TABLE IF NOT EXISTS fact_weather_readings (
    weather_fact_key    BIGSERIAL PRIMARY KEY,
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    location_key        BIGINT NOT NULL REFERENCES dim_location(location_key),
    observation_hour    INTEGER NOT NULL,        -- 0-23
    temp_celsius        NUMERIC(6, 2) NOT NULL,
    feels_like_c        NUMERIC(6, 2),
    temp_min_c          NUMERIC(6, 2),
    temp_max_c          NUMERIC(6, 2),
    pressure_hpa        INTEGER,
    humidity_pct        INTEGER,
    wind_speed_ms       NUMERIC(6, 2),
    wind_deg            INTEGER,
    clouds_pct          INTEGER,
    weather_main        VARCHAR(50),
    weather_desc        VARCHAR(200),
    visibility_m        INTEGER,
    rain_1h_mm          NUMERIC(8, 2) DEFAULT 0,
    snow_1h_mm          NUMERIC(8, 2) DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- News Article Fact Table
-- Grain: One row per article
CREATE TABLE IF NOT EXISTS fact_news_articles (
    news_fact_key       BIGSERIAL PRIMARY KEY,
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    source_key          BIGINT NOT NULL REFERENCES dim_news_source(source_key),
    title               TEXT NOT NULL,
    description         TEXT,
    author              VARCHAR(300),
    url                 TEXT,
    topic               VARCHAR(100),
    title_word_count    INTEGER,
    has_image           BOOLEAN DEFAULT FALSE,
    published_hour      INTEGER,                -- 0-23
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(url)
);


-- ============================================================================
-- 4. OLAP AGGREGATION VIEWS
-- ============================================================================

-- Monthly Stock Performance Rollup
CREATE OR REPLACE VIEW olap_stock_monthly_summary AS
SELECT
    d.year,
    d.month,
    d.month_name,
    c.ticker,
    c.company_name,
    COUNT(*) AS trading_days,
    ROUND(AVG(f.close_price), 4) AS avg_close_price,
    ROUND(MIN(f.low_price), 4) AS monthly_low,
    ROUND(MAX(f.high_price), 4) AS monthly_high,
    ROUND(SUM(f.volume), 0) AS total_volume,
    ROUND(AVG(f.daily_return_pct), 4) AS avg_daily_return_pct,
    ROUND(AVG(f.daily_range_pct), 4) AS avg_volatility_pct,
    ROUND(
        (MAX(f.close_price) - MIN(f.open_price)) / NULLIF(MIN(f.open_price), 0) * 100,
        4
    ) AS monthly_return_pct
FROM fact_stock_prices f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_company c ON f.company_key = c.company_key AND c.is_current = TRUE
GROUP BY d.year, d.month, d.month_name, c.ticker, c.company_name
ORDER BY d.year DESC, d.month DESC, c.ticker;


-- Weather Daily Summary by City
CREATE OR REPLACE VIEW olap_weather_daily_summary AS
SELECT
    d.full_date,
    d.day_name,
    d.month_name,
    d.year,
    l.city,
    l.country_code,
    COUNT(*) AS observation_count,
    ROUND(AVG(f.temp_celsius), 2) AS avg_temp_c,
    ROUND(MIN(f.temp_min_c), 2) AS daily_min_temp_c,
    ROUND(MAX(f.temp_max_c), 2) AS daily_max_temp_c,
    ROUND(AVG(f.humidity_pct), 1) AS avg_humidity_pct,
    ROUND(AVG(f.wind_speed_ms), 2) AS avg_wind_speed_ms,
    ROUND(AVG(f.pressure_hpa), 0) AS avg_pressure_hpa,
    ROUND(SUM(f.rain_1h_mm), 2) AS total_rain_mm,
    ROUND(SUM(f.snow_1h_mm), 2) AS total_snow_mm,
    MODE() WITHIN GROUP (ORDER BY f.weather_main) AS dominant_weather
FROM fact_weather_readings f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY d.full_date, d.day_name, d.month_name, d.year, l.city, l.country_code
ORDER BY d.full_date DESC, l.city;


-- News Volume & Source Analytics
CREATE OR REPLACE VIEW olap_news_daily_summary AS
SELECT
    d.full_date,
    d.day_name,
    d.year,
    d.month,
    f.topic,
    s.source_name,
    COUNT(*) AS article_count,
    ROUND(AVG(f.title_word_count), 1) AS avg_title_length,
    SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END) AS articles_with_images,
    ROUND(
        SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0) * 100,
        1
    ) AS image_coverage_pct
FROM fact_news_articles f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_news_source s ON f.source_key = s.source_key
GROUP BY d.full_date, d.day_name, d.year, d.month, f.topic, s.source_name
ORDER BY d.full_date DESC;


-- Cross-Domain Dashboard View
CREATE OR REPLACE VIEW olap_cross_domain_daily AS
SELECT
    d.full_date,
    d.day_name,
    d.month_name,
    d.year,
    d.quarter,
    -- Stock metrics (averaged across all tracked tickers)
    COALESCE(stock.avg_close, 0) AS stocks_avg_close,
    COALESCE(stock.total_volume, 0) AS stocks_total_volume,
    COALESCE(stock.avg_return_pct, 0) AS stocks_avg_return_pct,
    -- Weather metrics (averaged across all tracked cities)
    COALESCE(weather.avg_temp, 0) AS weather_avg_temp_c,
    COALESCE(weather.avg_humidity, 0) AS weather_avg_humidity_pct,
    -- News metrics
    COALESCE(news.article_count, 0) AS news_article_count
FROM dim_date d
LEFT JOIN (
    SELECT
        f.date_key,
        ROUND(AVG(f.close_price), 4) AS avg_close,
        SUM(f.volume) AS total_volume,
        ROUND(AVG(f.daily_return_pct), 4) AS avg_return_pct
    FROM fact_stock_prices f
    GROUP BY f.date_key
) stock ON d.date_key = stock.date_key
LEFT JOIN (
    SELECT
        f.date_key,
        ROUND(AVG(f.temp_celsius), 2) AS avg_temp,
        ROUND(AVG(f.humidity_pct), 1) AS avg_humidity
    FROM fact_weather_readings f
    GROUP BY f.date_key
) weather ON d.date_key = weather.date_key
LEFT JOIN (
    SELECT
        f.date_key,
        COUNT(*) AS article_count
    FROM fact_news_articles f
    GROUP BY f.date_key
) news ON d.date_key = news.date_key
WHERE (stock.date_key IS NOT NULL OR weather.date_key IS NOT NULL OR news.date_key IS NOT NULL)
ORDER BY d.full_date DESC;


-- ============================================================================
-- 5. POPULATE DIM_DATE (2020 - 2030)
-- ============================================================================

INSERT INTO dim_date (
    date_key, full_date, day_name, day_of_week, day_of_month, day_of_year,
    week_of_year, month, month_name, quarter, year, is_weekend,
    is_month_start, is_month_end, fiscal_quarter, fiscal_year
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d::DATE AS full_date,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(ISODOW FROM d)::INTEGER AS day_of_week,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(DOY FROM d)::INTEGER AS day_of_year,
    EXTRACT(WEEK FROM d)::INTEGER AS week_of_year,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(DAY FROM d) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
    CASE WHEN d = (DATE_TRUNC('MONTH', d) + INTERVAL '1 MONTH - 1 DAY')::DATE THEN TRUE ELSE FALSE END AS is_month_end,
    -- Fiscal year starts April (common in many orgs)
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (4,5,6) THEN 1
        WHEN EXTRACT(MONTH FROM d) IN (7,8,9) THEN 2
        WHEN EXTRACT(MONTH FROM d) IN (10,11,12) THEN 3
        ELSE 4
    END AS fiscal_quarter,
    CASE
        WHEN EXTRACT(MONTH FROM d) >= 4 THEN EXTRACT(YEAR FROM d)::INTEGER
        ELSE EXTRACT(YEAR FROM d)::INTEGER - 1
    END AS fiscal_year
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS s(d)
ON CONFLICT (date_key) DO NOTHING;


-- ============================================================================
-- 6. INDEXES FOR OLAP PERFORMANCE
-- ============================================================================

-- Staging indexes
CREATE INDEX IF NOT EXISTS idx_raw_stock_ticker_date ON raw_stock_prices(ticker, trade_date);
CREATE INDEX IF NOT EXISTS idx_raw_weather_city_dt ON raw_weather_observations(city, observation_dt);
CREATE INDEX IF NOT EXISTS idx_raw_news_published ON raw_news_articles(published_at);

-- Fact table indexes
CREATE INDEX IF NOT EXISTS idx_fact_stock_date ON fact_stock_prices(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_stock_company ON fact_stock_prices(company_key);
CREATE INDEX IF NOT EXISTS idx_fact_weather_date ON fact_weather_readings(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_weather_location ON fact_weather_readings(location_key);
CREATE INDEX IF NOT EXISTS idx_fact_news_date ON fact_news_articles(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_news_source ON fact_news_articles(source_key);

-- Dimension indexes
CREATE INDEX IF NOT EXISTS idx_dim_company_ticker ON dim_company(ticker, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_location_city ON dim_location(city);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month);

-- Pipeline audit indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status, started_at);
CREATE INDEX IF NOT EXISTS idx_dq_log_run ON data_quality_log(pipeline_run_id);


-- ============================================================================
-- 7. REAL-TIME E-COMMERCE STAR SCHEMA TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS stg_customers (
    raw_customer_id VARCHAR(100),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(200),
    customer_tier   VARCHAR(50),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg_products (
    raw_product_id  VARCHAR(100),
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    unit_price      NUMERIC(12, 2),
    cost_price      NUMERIC(12, 2),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id            VARCHAR(100),
    order_line_number   INTEGER,
    customer_id         VARCHAR(100),
    product_id          VARCHAR(100),
    quantity            INTEGER,
    unit_price          NUMERIC(12, 2),
    discount_amount     NUMERIC(12, 2) DEFAULT 0,
    tax_amount          NUMERIC(12, 2) DEFAULT 0,
    order_timestamp     TIMESTAMPTZ,
    pipeline_run_id     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key            BIGSERIAL PRIMARY KEY,
    customer_id             VARCHAR(100) NOT NULL,
    first_name              VARCHAR(100),
    last_name               VARCHAR(100),
    email                   VARCHAR(200),
    customer_tier           VARCHAR(50),
    city                    VARCHAR(100),
    state                   VARCHAR(100),
    country                 VARCHAR(100),
    effective_start_date    TIMESTAMPTZ DEFAULT NOW(),
    effective_end_date      TIMESTAMPTZ,
    is_current              BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key     BIGSERIAL PRIMARY KEY,
    product_id      VARCHAR(100) NOT NULL UNIQUE,
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    unit_price      NUMERIC(12, 2),
    cost_price      NUMERIC(12, 2),
    is_active       BOOLEAN DEFAULT TRUE,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_fact_key      BIGSERIAL PRIMARY KEY,
    order_id            VARCHAR(100) NOT NULL,
    order_line_number   INTEGER NOT NULL,
    customer_key        BIGINT REFERENCES dim_customer(customer_key),
    product_key         BIGINT REFERENCES dim_product(product_key),
    order_date_key      INTEGER REFERENCES dim_date(date_key),
    quantity            INTEGER NOT NULL,
    unit_price          NUMERIC(12, 2) NOT NULL,
    gross_amount        NUMERIC(12, 2) NOT NULL,
    discount_amount     NUMERIC(12, 2) DEFAULT 0,
    tax_amount          NUMERIC(12, 2) DEFAULT 0,
    net_amount          NUMERIC(12, 2) NOT NULL,
    cost_amount         NUMERIC(12, 2) NOT NULL,
    profit_amount       NUMERIC(12, 2) NOT NULL,
    profit_margin_pct   NUMERIC(5, 2) NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(order_date_key);

