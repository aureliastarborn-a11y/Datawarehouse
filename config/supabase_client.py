"""
Data Warehouse - Database Client Configuration
Supports two modes:
  1. Supabase (PostgreSQL) — when credentials are configured
  2. Local DuckDB — fallback when Supabase is not configured
"""

import os
import sys

from dotenv import load_dotenv

# Load environment variables from root .env
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_project_root, ".env"))

# ============================================================================
# Mode Detection
# ============================================================================

_LOCAL_DB_PATH = os.path.join(_project_root, "warehouse_olap.duckdb")


def is_supabase_configured() -> bool:
    """Check if Supabase credentials are properly configured."""
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    db_url = os.getenv("SUPABASE_DB_URL", "")
    return bool(
        url and "your-project" not in url
        and key and "your-supabase" not in key
        and db_url and "your-project" not in db_url
    )


def get_mode() -> str:
    """Returns 'supabase' or 'local' depending on config."""
    return "supabase" if is_supabase_configured() else "local"


# ============================================================================
# Environment Variable Accessors
# ============================================================================

def get_supabase_url() -> str:
    return os.getenv("SUPABASE_URL", "")

def get_supabase_key() -> str:
    return os.getenv("SUPABASE_KEY", "")

def get_supabase_db_url() -> str:
    return os.getenv("SUPABASE_DB_URL", "")

def get_api_key() -> str:
    return os.getenv("API_KEY", "dw-secret-key-2024")

def get_stock_tickers() -> list:
    return [t.strip() for t in os.getenv("STOCK_TICKERS", "AAPL,MSFT,GOOGL").split(",") if t.strip()]

def get_weather_cities() -> list:
    return [c.strip() for c in os.getenv("WEATHER_CITIES", "New York,London,Tokyo").split(",") if c.strip()]

def get_news_topics() -> list:
    return [t.strip() for t in os.getenv("NEWS_TOPICS", "technology,business").split(",") if t.strip()]

def get_alpha_vantage_key() -> str:
    return os.getenv("ALPHA_VANTAGE_API_KEY", "demo")

def get_openweathermap_key() -> str:
    return os.getenv("OPENWEATHERMAP_API_KEY", "demo")

def get_newsapi_key() -> str:
    return os.getenv("NEWSAPI_API_KEY", "demo")


# ============================================================================
# DuckDB Local Connection (Fallback Mode)
# ============================================================================

def get_duckdb_connection(read_only: bool = False):
    """Returns a DuckDB connection for local mode, with automatic fallback for process locks."""
    import duckdb

    try:
        conn = duckdb.connect(_LOCAL_DB_PATH, read_only=read_only)
        try:
            conn.execute("INSTALL 'uuid'; LOAD 'uuid';")
        except Exception:
            pass
        return conn
    except Exception as e:
        if "used by another process" in str(e) or "IO Error" in str(e):
            # Fallback to read-only connection
            conn = duckdb.connect(_LOCAL_DB_PATH, read_only=True)
            return conn
        raise e


# ============================================================================
# Supabase PostgreSQL Connection
# ============================================================================

def get_pg_connection():
    """Returns a direct PostgreSQL connection to Supabase."""
    if not is_supabase_configured():
        return None

    import psycopg2
    db_url = get_supabase_db_url()
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
        return None


# ============================================================================
# Unified SQL Execution (works in both modes)
# ============================================================================

def execute_sql(sql: str, params=None, fetch: bool = False):
    """Execute SQL against the active database (Supabase or DuckDB)."""
    mode = get_mode()

    if mode == "supabase":
        return _execute_pg(sql, params, fetch)
    else:
        return _execute_duckdb(sql, params, fetch)


def _execute_pg(sql, params, fetch):
    """Execute SQL against Supabase PostgreSQL."""
    import psycopg2.extras
    conn = get_pg_connection()
    if conn is None:
        raise ConnectionError("Cannot connect to Supabase PostgreSQL.")
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            if fetch:
                results = cur.fetchall()
                conn.commit()
                return results
            conn.commit()
            return None
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def _execute_duckdb(sql, params, fetch):
    """Execute SQL against local DuckDB."""
    conn = get_duckdb_connection()

    try:
        if params:
            converted_sql = sql.replace("%s", "?")
            if fetch:
                result = conn.execute(converted_sql, list(params))
                rows = result.fetchall()
                cols = [desc[0] for desc in result.description]
                return [dict(zip(cols, row)) for row in rows]
            else:
                conn.execute(converted_sql, list(params))
                return None
        else:
            if fetch:
                result = conn.execute(sql)
                rows = result.fetchall()
                cols = [desc[0] for desc in result.description]
                return [dict(zip(cols, row)) for row in rows]
            else:
                conn.execute(sql)
                return None
    finally:
        conn.close()


def execute_sql_df(sql: str, params=None):
    """Execute SQL and return results as a pandas DataFrame."""
    import pandas as pd
    mode = get_mode()

    if mode == "supabase":
        conn = get_pg_connection()
        if conn is None:
            raise ConnectionError("Cannot connect to Supabase PostgreSQL.")
        try:
            df = pd.read_sql(sql, conn, params=params)
            return df
        finally:
            conn.close()
    else:
        conn = get_duckdb_connection()
        try:
            if params:
                converted_sql = sql.replace("%s", "?")
                return conn.execute(converted_sql, list(params)).df()
            else:
                return conn.execute(sql).df()
        finally:
            conn.close()


# ============================================================================
# Schema Initialization
# ============================================================================

def init_schema():
    """Initialize the database schema (Supabase or local DuckDB)."""
    mode = get_mode()
    print(f"[INIT] Database mode: {mode.upper()}")

    if mode == "supabase":
        return _init_supabase_schema()
    else:
        return _init_duckdb_schema()


def _init_supabase_schema():
    """Initialize schema on Supabase PostgreSQL."""
    schema_path = os.path.join(_project_root, "supabase", "schema.sql")
    if not os.path.exists(schema_path):
        print(f"[ERROR] Schema file not found at {schema_path}")
        return False

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn = get_pg_connection()
    if conn is None:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
        print("[OK] Supabase schema initialized successfully!")
        return True
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Schema initialization failed: {e}")
        return False
    finally:
        conn.close()


def _init_duckdb_schema():
    """Initialize schema on local DuckDB (adapted from PostgreSQL DDL)."""
    conn = get_duckdb_connection()

    try:
        # ---- Staging Tables ----
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_stock_prices (
                id VARCHAR DEFAULT uuid()::VARCHAR,
                ticker VARCHAR(10) NOT NULL,
                trade_date DATE NOT NULL,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                volume BIGINT,
                source VARCHAR(50) DEFAULT 'alpha_vantage',
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pipeline_run_id VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_weather_observations (
                id VARCHAR DEFAULT uuid()::VARCHAR,
                city VARCHAR(100) NOT NULL,
                country_code VARCHAR(10),
                observation_dt TIMESTAMP NOT NULL,
                temp_celsius DOUBLE,
                feels_like_c DOUBLE,
                temp_min_c DOUBLE,
                temp_max_c DOUBLE,
                pressure_hpa INTEGER,
                humidity_pct INTEGER,
                wind_speed_ms DOUBLE,
                wind_deg INTEGER,
                clouds_pct INTEGER,
                weather_main VARCHAR(50),
                weather_desc VARCHAR(200),
                visibility_m INTEGER,
                rain_1h_mm DOUBLE DEFAULT 0,
                snow_1h_mm DOUBLE DEFAULT 0,
                latitude DOUBLE,
                longitude DOUBLE,
                source VARCHAR(50) DEFAULT 'openweathermap',
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pipeline_run_id VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_news_articles (
                id VARCHAR DEFAULT uuid()::VARCHAR,
                source_id VARCHAR(100),
                source_name VARCHAR(200),
                author VARCHAR(300),
                title VARCHAR NOT NULL,
                description VARCHAR,
                url VARCHAR,
                url_to_image VARCHAR,
                published_at TIMESTAMP,
                content VARCHAR,
                topic VARCHAR(100),
                source_api VARCHAR(50) DEFAULT 'newsapi',
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pipeline_run_id VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id VARCHAR NOT NULL,
                run_type VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                records_extracted INTEGER DEFAULT 0,
                records_loaded INTEGER DEFAULT 0,
                records_transformed INTEGER DEFAULT 0,
                records_rejected INTEGER DEFAULT 0,
                error_message VARCHAR,
                metadata VARCHAR DEFAULT '{}'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_log (
                id VARCHAR DEFAULT uuid()::VARCHAR,
                pipeline_run_id VARCHAR,
                table_name VARCHAR(100) NOT NULL,
                check_name VARCHAR(200) NOT NULL,
                check_type VARCHAR(50) NOT NULL,
                records_checked INTEGER DEFAULT 0,
                records_failed INTEGER DEFAULT 0,
                severity VARCHAR(20) DEFAULT 'warning',
                details VARCHAR,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ---- Dimension Tables ----
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_date (
                date_key INTEGER PRIMARY KEY,
                full_date DATE NOT NULL,
                day_name VARCHAR(10) NOT NULL,
                day_of_week INTEGER NOT NULL,
                day_of_month INTEGER NOT NULL,
                day_of_year INTEGER NOT NULL,
                week_of_year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                month_name VARCHAR(10) NOT NULL,
                quarter INTEGER NOT NULL,
                year INTEGER NOT NULL,
                is_weekend BOOLEAN NOT NULL,
                is_month_start BOOLEAN NOT NULL,
                is_month_end BOOLEAN NOT NULL,
                fiscal_quarter INTEGER NOT NULL,
                fiscal_year INTEGER NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_company (
                company_key BIGINT PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                company_name VARCHAR(200),
                sector VARCHAR(100),
                industry VARCHAR(200),
                market_cap_tier VARCHAR(20),
                effective_start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                effective_end_date TIMESTAMP,
                is_current BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_company_key START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_location (
                location_key BIGINT PRIMARY KEY,
                city VARCHAR(100) NOT NULL,
                country_code VARCHAR(10),
                latitude DOUBLE,
                longitude DOUBLE,
                timezone VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_location_key START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_news_source (
                source_key BIGINT PRIMARY KEY,
                source_id VARCHAR(100),
                source_name VARCHAR(200) NOT NULL,
                source_url VARCHAR,
                category VARCHAR(100),
                language VARCHAR(10) DEFAULT 'en',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_news_source_key START 1
        """)

        # ---- Fact Tables ----
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_stock_prices (
                stock_fact_key BIGINT PRIMARY KEY,
                date_key INTEGER NOT NULL,
                company_key BIGINT NOT NULL,
                open_price DOUBLE NOT NULL,
                high_price DOUBLE NOT NULL,
                low_price DOUBLE NOT NULL,
                close_price DOUBLE NOT NULL,
                volume BIGINT NOT NULL,
                daily_return_pct DOUBLE,
                daily_range DOUBLE,
                daily_range_pct DOUBLE,
                avg_price DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_stock_fact START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_weather_readings (
                weather_fact_key BIGINT PRIMARY KEY,
                date_key INTEGER NOT NULL,
                location_key BIGINT NOT NULL,
                observation_hour INTEGER NOT NULL,
                temp_celsius DOUBLE NOT NULL,
                feels_like_c DOUBLE,
                temp_min_c DOUBLE,
                temp_max_c DOUBLE,
                pressure_hpa INTEGER,
                humidity_pct INTEGER,
                wind_speed_ms DOUBLE,
                wind_deg INTEGER,
                clouds_pct INTEGER,
                weather_main VARCHAR(50),
                weather_desc VARCHAR(200),
                visibility_m INTEGER,
                rain_1h_mm DOUBLE DEFAULT 0,
                snow_1h_mm DOUBLE DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_weather_fact START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_news_articles (
                news_fact_key BIGINT PRIMARY KEY,
                date_key INTEGER NOT NULL,
                source_key BIGINT NOT NULL,
                title VARCHAR NOT NULL,
                description VARCHAR,
                author VARCHAR(300),
                url VARCHAR,
                topic VARCHAR(100),
                title_word_count INTEGER,
                has_image BOOLEAN DEFAULT FALSE,
                published_hour INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_news_fact START 1
        """)

        # ---- E-Commerce Staging & Kimball Star Schema Tables ----
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_customers (
                raw_customer_id VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                customer_tier VARCHAR,
                city VARCHAR,
                state VARCHAR,
                country VARCHAR,
                updated_at TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_products (
                raw_product_id VARCHAR,
                product_name VARCHAR,
                category VARCHAR,
                subcategory VARCHAR,
                unit_price DECIMAL(12, 2),
                cost_price DECIMAL(12, 2),
                updated_at TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_orders (
                order_id VARCHAR,
                order_line_number INTEGER,
                customer_id VARCHAR,
                product_id VARCHAR,
                order_timestamp TIMESTAMP,
                quantity INTEGER,
                unit_price DECIMAL(12, 2),
                discount_amount DECIMAL(12, 2),
                tax_amount DECIMAL(12, 2)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_key BIGINT PRIMARY KEY,
                customer_id VARCHAR NOT NULL,
                first_name VARCHAR NOT NULL,
                last_name VARCHAR NOT NULL,
                email VARCHAR NOT NULL,
                customer_tier VARCHAR NOT NULL,
                city VARCHAR,
                state VARCHAR,
                country VARCHAR,
                effective_start_date TIMESTAMP NOT NULL,
                effective_end_date TIMESTAMP,
                is_current BOOLEAN NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_product (
                product_key BIGINT PRIMARY KEY,
                product_id VARCHAR NOT NULL UNIQUE,
                product_name VARCHAR NOT NULL,
                category VARCHAR NOT NULL,
                subcategory VARCHAR NOT NULL,
                unit_price DECIMAL(12, 2) NOT NULL,
                cost_price DECIMAL(12, 2) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMP NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                sales_fact_key BIGINT PRIMARY KEY,
                order_id VARCHAR NOT NULL,
                order_line_number INTEGER NOT NULL,
                customer_key BIGINT NOT NULL,
                product_key BIGINT NOT NULL,
                order_date_key INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(12, 2) NOT NULL,
                gross_amount DECIMAL(12, 2) NOT NULL,
                discount_amount DECIMAL(12, 2) NOT NULL,
                tax_amount DECIMAL(12, 2) NOT NULL,
                net_amount DECIMAL(12, 2) NOT NULL,
                cost_amount DECIMAL(12, 2) NOT NULL,
                profit_amount DECIMAL(12, 2) NOT NULL,
                profit_margin_pct DECIMAL(5, 2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ---- Populate dim_date (2020-2030) ----
        existing = conn.execute("SELECT COUNT(*) AS cnt FROM dim_date").fetchone()[0]
        if existing == 0:
            conn.execute("""
                INSERT INTO dim_date
                SELECT
                    CAST(STRFTIME(d, '%Y%m%d') AS INTEGER) AS date_key,
                    CAST(d AS DATE) AS full_date,
                    STRFTIME(d, '%A') AS day_name,
                    EXTRACT(ISODOW FROM d)::INTEGER AS day_of_week,
                    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
                    EXTRACT(DOY FROM d)::INTEGER AS day_of_year,
                    EXTRACT(WEEK FROM d)::INTEGER AS week_of_year,
                    EXTRACT(MONTH FROM d)::INTEGER AS month,
                    STRFTIME(d, '%B') AS month_name,
                    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
                    EXTRACT(YEAR FROM d)::INTEGER AS year,
                    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
                    CASE WHEN EXTRACT(DAY FROM d) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
                    CASE WHEN d = LAST_DAY(d) THEN TRUE ELSE FALSE END AS is_month_end,
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
                FROM GENERATE_SERIES(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') AS s(d)
            """)
            date_count = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
            print(f"  [OK] dim_date populated with {date_count} rows (2020-2030)")

        # ---- OLAP Views ----
        conn.execute("""
            CREATE OR REPLACE VIEW olap_stock_monthly_summary AS
            SELECT
                d.year, d.month, d.month_name, c.ticker, c.company_name,
                COUNT(*) AS trading_days,
                ROUND(AVG(f.close_price), 4) AS avg_close_price,
                ROUND(MIN(f.low_price), 4) AS monthly_low,
                ROUND(MAX(f.high_price), 4) AS monthly_high,
                ROUND(SUM(f.volume)::DOUBLE, 0) AS total_volume,
                ROUND(AVG(f.daily_return_pct), 4) AS avg_daily_return_pct,
                ROUND(AVG(f.daily_range_pct), 4) AS avg_volatility_pct,
                ROUND((MAX(f.close_price) - MIN(f.open_price)) / NULLIF(MIN(f.open_price), 0) * 100, 4) AS monthly_return_pct
            FROM fact_stock_prices f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_company c ON f.company_key = c.company_key AND c.is_current = TRUE
            GROUP BY d.year, d.month, d.month_name, c.ticker, c.company_name
            ORDER BY d.year DESC, d.month DESC, c.ticker
        """)

        conn.execute("""
            CREATE OR REPLACE VIEW olap_weather_daily_summary AS
            SELECT
                d.full_date, d.day_name, d.month_name, d.year,
                l.city, l.country_code,
                COUNT(*) AS observation_count,
                ROUND(AVG(f.temp_celsius), 2) AS avg_temp_c,
                ROUND(MIN(f.temp_min_c), 2) AS daily_min_temp_c,
                ROUND(MAX(f.temp_max_c), 2) AS daily_max_temp_c,
                ROUND(AVG(f.humidity_pct), 1) AS avg_humidity_pct,
                ROUND(AVG(f.wind_speed_ms), 2) AS avg_wind_speed_ms,
                ROUND(AVG(f.pressure_hpa)::DOUBLE, 0) AS avg_pressure_hpa,
                ROUND(SUM(f.rain_1h_mm), 2) AS total_rain_mm,
                ROUND(SUM(f.snow_1h_mm), 2) AS total_snow_mm,
                MODE(f.weather_main) AS dominant_weather
            FROM fact_weather_readings f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_location l ON f.location_key = l.location_key
            GROUP BY d.full_date, d.day_name, d.month_name, d.year, l.city, l.country_code
            ORDER BY d.full_date DESC, l.city
        """)

        conn.execute("""
            CREATE OR REPLACE VIEW olap_news_daily_summary AS
            SELECT
                d.full_date, d.day_name, d.year, d.month,
                f.topic, s.source_name,
                COUNT(*) AS article_count,
                ROUND(AVG(f.title_word_count), 1) AS avg_title_length,
                SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END) AS articles_with_images,
                ROUND(SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) * 100, 1) AS image_coverage_pct
            FROM fact_news_articles f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_news_source s ON f.source_key = s.source_key
            GROUP BY d.full_date, d.day_name, d.year, d.month, f.topic, s.source_name
            ORDER BY d.full_date DESC
        """)

        conn.execute("""
            CREATE OR REPLACE VIEW olap_cross_domain_daily AS
            SELECT
                d.full_date, d.day_name, d.month_name, d.year, d.quarter,
                COALESCE(stock.avg_close, 0) AS stocks_avg_close,
                COALESCE(stock.total_volume, 0) AS stocks_total_volume,
                COALESCE(stock.avg_return_pct, 0) AS stocks_avg_return_pct,
                COALESCE(weather.avg_temp, 0) AS weather_avg_temp_c,
                COALESCE(weather.avg_humidity, 0) AS weather_avg_humidity_pct,
                COALESCE(news.article_count, 0) AS news_article_count
            FROM dim_date d
            LEFT JOIN (
                SELECT f.date_key,
                    ROUND(AVG(f.close_price), 4) AS avg_close,
                    SUM(f.volume) AS total_volume,
                    ROUND(AVG(f.daily_return_pct), 4) AS avg_return_pct
                FROM fact_stock_prices f GROUP BY f.date_key
            ) stock ON d.date_key = stock.date_key
            LEFT JOIN (
                SELECT f.date_key,
                    ROUND(AVG(f.temp_celsius), 2) AS avg_temp,
                    ROUND(AVG(f.humidity_pct), 1) AS avg_humidity
                FROM fact_weather_readings f GROUP BY f.date_key
            ) weather ON d.date_key = weather.date_key
            LEFT JOIN (
                SELECT f.date_key, COUNT(*) AS article_count
                FROM fact_news_articles f GROUP BY f.date_key
            ) news ON d.date_key = news.date_key
            WHERE (stock.date_key IS NOT NULL OR weather.date_key IS NOT NULL OR news.date_key IS NOT NULL)
            ORDER BY d.full_date DESC
        """)

        print("[OK] DuckDB local schema initialized successfully!")
        return True

    except Exception as e:
        print(f"[ERROR] DuckDB schema initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_config() -> dict:
    """Validate all configuration and return status report."""
    status = {
        "mode": get_mode(),
        "supabase_url": bool(get_supabase_url() and "your-project" not in get_supabase_url()),
        "supabase_key": bool(get_supabase_key() and "your-supabase" not in get_supabase_key()),
        "supabase_db_url": bool(get_supabase_db_url() and "your-project" not in get_supabase_db_url()),
        "alpha_vantage_key": get_alpha_vantage_key() != "demo",
        "openweathermap_key": get_openweathermap_key() != "demo",
        "newsapi_key": get_newsapi_key() != "demo",
        "stock_tickers": get_stock_tickers(),
        "weather_cities": get_weather_cities(),
        "news_topics": get_news_topics(),
    }
    return status


if __name__ == "__main__":
    print("=" * 60)
    print("  Data Warehouse - Configuration Validator")
    print("=" * 60)
    config = validate_config()
    for key, val in config.items():
        icon = "[OK]" if val else "[  ]"
        print(f"  {icon} {key}: {val}")
