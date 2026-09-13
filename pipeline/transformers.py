"""
Data Warehouse - ELT Transformation Engine
Transforms raw staging data into Kimball Star Schema dimensions & facts.
Supports both Supabase PostgreSQL and local DuckDB modes.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.supabase_client import get_mode, get_duckdb_connection, get_pg_connection
from pipeline.loaders import log_data_quality_issue


# ============================================================================
# DATA QUALITY GATES
# ============================================================================

def run_data_quality_gates(pipeline_run_id: str = None) -> dict:
    """Execute data quality validation checks across all staging tables."""
    print("\n  [DQ] Running Data Quality Gates...")
    results = {"checks": 0, "passed": 0, "failed": 0, "warnings": 0}
    mode = get_mode()

    if mode == "local":
        conn = get_duckdb_connection()
    else:
        conn = get_pg_connection()
        if conn is None:
            print("  [ERROR] Cannot run DQ gates -- no database connection.")
            return results

    try:
        if mode == "local":
            cur = conn
            def fetchone_val(sql):
                return conn.execute(sql).fetchone()[0]
        else:
            cur = conn.cursor()
            def fetchone_val(sql):
                cur.execute(sql)
                return cur.fetchone()[0]

        # Gate 1: Null tickers
        null_tickers = fetchone_val("SELECT COUNT(*) FROM raw_stock_prices WHERE ticker IS NULL OR ticker = ''")
        results["checks"] += 1
        if null_tickers == 0:
            results["passed"] += 1
            print(f"  [DQ PASS] Gate 1: No null tickers in raw_stock_prices")
        else:
            results["warnings"] += 1
            print(f"  [DQ WARN] Gate 1: {null_tickers} null tickers")

        if pipeline_run_id:
            log_data_quality_issue(pipeline_run_id, "raw_stock_prices", "null_ticker_check",
                "null_check", records_failed=null_tickers,
                severity="warning" if null_tickers > 0 else "info")

        # Gate 2: Negative prices
        null_prices = fetchone_val(
            "SELECT COUNT(*) FROM raw_stock_prices WHERE close_price < 0 OR open_price < 0")
        results["checks"] += 1
        if null_prices == 0:
            results["passed"] += 1
            print(f"  [DQ PASS] Gate 2: No negative stock prices")
        else:
            results["failed"] += 1
            print(f"  [DQ FAIL] Gate 2: {null_prices} negative price records")

        if pipeline_run_id:
            log_data_quality_issue(pipeline_run_id, "raw_stock_prices", "negative_price_check",
                "range_check", records_failed=null_prices,
                severity="error" if null_prices > 0 else "info")

        # Gate 3: Weather temperature range
        temp_outliers = fetchone_val(
            "SELECT COUNT(*) FROM raw_weather_observations WHERE temp_celsius < -90 OR temp_celsius > 60")
        results["checks"] += 1
        if temp_outliers == 0:
            results["passed"] += 1
            print(f"  [DQ PASS] Gate 3: Weather temperatures within valid range")
        else:
            results["warnings"] += 1
            print(f"  [DQ WARN] Gate 3: {temp_outliers} extreme temperature records")

        # Gate 4: Null news titles
        null_titles = fetchone_val(
            "SELECT COUNT(*) FROM raw_news_articles WHERE title IS NULL OR TRIM(title) = ''")
        results["checks"] += 1
        if null_titles == 0:
            results["passed"] += 1
            print(f"  [DQ PASS] Gate 4: No null titles in news articles")
        else:
            results["warnings"] += 1
            print(f"  [DQ WARN] Gate 4: {null_titles} null/empty titles")

        # Gate 5: Record counts
        stock_count = fetchone_val("SELECT COUNT(*) FROM raw_stock_prices")
        weather_count = fetchone_val("SELECT COUNT(*) FROM raw_weather_observations")
        news_count = fetchone_val("SELECT COUNT(*) FROM raw_news_articles")
        results["checks"] += 1
        results["passed"] += 1
        print(f"  [DQ INFO] Gate 5: Staging counts - Stocks: {stock_count}, Weather: {weather_count}, News: {news_count}")

        if mode == "supabase":
            conn.commit()
            cur.close()

    except Exception as e:
        if mode == "supabase":
            conn.rollback()
        print(f"  [ERROR] Data quality gate failed: {e}")
    finally:
        if mode == "supabase":
            conn.close()

    print(f"  [DQ SUMMARY] {results['checks']} checks | {results['passed']} passed | {results['warnings']} warnings | {results['failed']} failed")
    return results


# ============================================================================
# STOCK TRANSFORMATIONS
# ============================================================================

def transform_stock_data(pipeline_run_id: str = None) -> int:
    """Transform raw stock data into dim_company + fact_stock_prices."""
    print("\n  [TRANSFORM] Running stock data ELT transformations...")
    mode = get_mode()
    transformed = 0

    if mode == "local":
        conn = get_duckdb_connection()
        try:
            # Step 1: Populate dim_company
            conn.execute("""
                INSERT INTO dim_company (company_key, ticker, company_name, effective_start_date, is_current)
                SELECT
                    NEXTVAL('seq_company_key'),
                    r.ticker,
                    r.ticker,
                    MIN(r.ingested_at),
                    TRUE
                FROM (SELECT DISTINCT ticker, MIN(ingested_at) AS ingested_at FROM raw_stock_prices GROUP BY ticker) r
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_company dc WHERE dc.ticker = r.ticker AND dc.is_current = TRUE
                )
                GROUP BY r.ticker
            """)
            new_companies = conn.execute(
                "SELECT COUNT(DISTINCT ticker) FROM dim_company WHERE is_current = TRUE"
            ).fetchone()[0]
            print(f"  [OK] dim_company: {new_companies} total tickers")

            # Step 2: Populate fact_stock_prices
            conn.execute("""
                INSERT INTO fact_stock_prices (
                    stock_fact_key, date_key, company_key, open_price, high_price, low_price,
                    close_price, volume, daily_return_pct, daily_range, daily_range_pct, avg_price
                )
                SELECT
                    NEXTVAL('seq_stock_fact'),
                    CAST(STRFTIME(r.trade_date, '%Y%m%d') AS INTEGER),
                    dc.company_key,
                    r.open_price,
                    r.high_price,
                    r.low_price,
                    r.close_price,
                    r.volume,
                    ROUND(
                        (r.close_price - COALESCE(
                            LAG(r.close_price) OVER (PARTITION BY r.ticker ORDER BY r.trade_date),
                            r.close_price
                        )) / NULLIF(
                            LAG(r.close_price) OVER (PARTITION BY r.ticker ORDER BY r.trade_date), 0
                        ) * 100, 4
                    ),
                    ROUND(r.high_price - r.low_price, 4),
                    ROUND((r.high_price - r.low_price) / NULLIF(r.low_price, 0) * 100, 4),
                    ROUND((r.open_price + r.high_price + r.low_price + r.close_price) / 4, 4)
                FROM raw_stock_prices r
                JOIN dim_company dc ON r.ticker = dc.ticker AND dc.is_current = TRUE
                WHERE r.close_price > 0 AND r.volume >= 0
                  AND NOT EXISTS (
                      SELECT 1 FROM fact_stock_prices f
                      JOIN dim_company dc2 ON f.company_key = dc2.company_key
                      WHERE dc2.ticker = r.ticker
                        AND f.date_key = CAST(STRFTIME(r.trade_date, '%Y%m%d') AS INTEGER)
                  )
            """)
            transformed = conn.execute("SELECT COUNT(*) FROM fact_stock_prices").fetchone()[0]
            print(f"  [OK] fact_stock_prices: {transformed} total records")

        except Exception as e:
            print(f"  [ERROR] Stock transformation failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        # Supabase PostgreSQL mode
        conn = get_pg_connection()
        if conn is None:
            return 0
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dim_company (ticker, company_name, effective_start_date, is_current)
                    SELECT DISTINCT r.ticker, r.ticker, MIN(r.ingested_at), TRUE
                    FROM raw_stock_prices r
                    WHERE NOT EXISTS (SELECT 1 FROM dim_company dc WHERE dc.ticker = r.ticker AND dc.is_current = TRUE)
                    GROUP BY r.ticker
                """)

                cur.execute("""
                    INSERT INTO fact_stock_prices (
                        date_key, company_key, open_price, high_price, low_price,
                        close_price, volume, daily_return_pct, daily_range, daily_range_pct, avg_price
                    )
                    SELECT
                        TO_CHAR(r.trade_date, 'YYYYMMDD')::INTEGER,
                        dc.company_key, r.open_price, r.high_price, r.low_price,
                        r.close_price, r.volume,
                        ROUND((r.close_price - COALESCE(LAG(r.close_price) OVER (PARTITION BY r.ticker ORDER BY r.trade_date), r.close_price))
                            / NULLIF(LAG(r.close_price) OVER (PARTITION BY r.ticker ORDER BY r.trade_date), 0) * 100, 4),
                        ROUND(r.high_price - r.low_price, 4),
                        ROUND((r.high_price - r.low_price) / NULLIF(r.low_price, 0) * 100, 4),
                        ROUND((r.open_price + r.high_price + r.low_price + r.close_price) / 4, 4)
                    FROM raw_stock_prices r
                    JOIN dim_company dc ON r.ticker = dc.ticker AND dc.is_current = TRUE
                    JOIN dim_date dd ON dd.date_key = TO_CHAR(r.trade_date, 'YYYYMMDD')::INTEGER
                    WHERE r.close_price > 0
                    ON CONFLICT (date_key, company_key) DO UPDATE SET
                        close_price = EXCLUDED.close_price, volume = EXCLUDED.volume
                """)
                transformed = cur.rowcount
            conn.commit()
            print(f"  [OK] fact_stock_prices: {transformed} records upserted")
        except Exception as e:
            conn.rollback()
            print(f"  [ERROR] Stock transformation failed: {e}")
        finally:
            conn.close()

    return transformed


# ============================================================================
# WEATHER TRANSFORMATIONS
# ============================================================================

def transform_weather_data(pipeline_run_id: str = None) -> int:
    """Transform raw weather data into dim_location + fact_weather_readings."""
    print("\n  [TRANSFORM] Running weather data ELT transformations...")
    mode = get_mode()
    transformed = 0

    if mode == "local":
        conn = get_duckdb_connection()
        try:
            # Step 1: dim_location
            conn.execute("""
                INSERT INTO dim_location (location_key, city, country_code, latitude, longitude)
                SELECT
                    NEXTVAL('seq_location_key'),
                    r.city, r.country_code, r.latitude, r.longitude
                FROM (
                    SELECT DISTINCT city, country_code,
                        FIRST(latitude) AS latitude, FIRST(longitude) AS longitude
                    FROM raw_weather_observations
                    WHERE city IS NOT NULL
                    GROUP BY city, country_code
                ) r
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_location dl WHERE dl.city = r.city AND dl.country_code = r.country_code
                )
            """)
            loc_count = conn.execute("SELECT COUNT(*) FROM dim_location").fetchone()[0]
            print(f"  [OK] dim_location: {loc_count} total locations")

            # Step 2: fact_weather_readings
            conn.execute("""
                INSERT INTO fact_weather_readings (
                    weather_fact_key, date_key, location_key, observation_hour,
                    temp_celsius, feels_like_c, temp_min_c, temp_max_c,
                    pressure_hpa, humidity_pct, wind_speed_ms, wind_deg,
                    clouds_pct, weather_main, weather_desc, visibility_m,
                    rain_1h_mm, snow_1h_mm
                )
                SELECT
                    NEXTVAL('seq_weather_fact'),
                    CAST(STRFTIME(CAST(r.observation_dt AS DATE), '%Y%m%d') AS INTEGER),
                    dl.location_key,
                    EXTRACT(HOUR FROM r.observation_dt)::INTEGER,
                    r.temp_celsius, r.feels_like_c, r.temp_min_c, r.temp_max_c,
                    r.pressure_hpa, r.humidity_pct, r.wind_speed_ms, r.wind_deg,
                    r.clouds_pct, r.weather_main, r.weather_desc, r.visibility_m,
                    COALESCE(r.rain_1h_mm, 0), COALESCE(r.snow_1h_mm, 0)
                FROM raw_weather_observations r
                JOIN dim_location dl ON r.city = dl.city AND r.country_code = dl.country_code
                WHERE r.temp_celsius BETWEEN -90 AND 60
                  AND r.id NOT IN (SELECT id FROM raw_weather_observations rw2
                      WHERE EXISTS (
                          SELECT 1 FROM fact_weather_readings fw
                          JOIN dim_location dl2 ON fw.location_key = dl2.location_key
                          WHERE dl2.city = rw2.city
                            AND fw.date_key = CAST(STRFTIME(CAST(rw2.observation_dt AS DATE), '%Y%m%d') AS INTEGER)
                            AND fw.observation_hour = EXTRACT(HOUR FROM rw2.observation_dt)::INTEGER
                      ))
            """)
            transformed = conn.execute("SELECT COUNT(*) FROM fact_weather_readings").fetchone()[0]
            print(f"  [OK] fact_weather_readings: {transformed} total records")

        except Exception as e:
            print(f"  [ERROR] Weather transformation failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        conn = get_pg_connection()
        if conn is None:
            return 0
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dim_location (city, country_code, latitude, longitude)
                    SELECT DISTINCT r.city, r.country_code, r.latitude, r.longitude
                    FROM raw_weather_observations r WHERE r.city IS NOT NULL
                    ON CONFLICT (city, country_code) DO UPDATE SET latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
                """)
                cur.execute("""
                    INSERT INTO fact_weather_readings (
                        date_key, location_key, observation_hour,
                        temp_celsius, feels_like_c, temp_min_c, temp_max_c,
                        pressure_hpa, humidity_pct, wind_speed_ms, wind_deg,
                        clouds_pct, weather_main, weather_desc, visibility_m, rain_1h_mm, snow_1h_mm
                    )
                    SELECT
                        TO_CHAR(r.observation_dt::DATE, 'YYYYMMDD')::INTEGER,
                        dl.location_key, EXTRACT(HOUR FROM r.observation_dt)::INTEGER,
                        r.temp_celsius, r.feels_like_c, r.temp_min_c, r.temp_max_c,
                        r.pressure_hpa, r.humidity_pct, r.wind_speed_ms, r.wind_deg,
                        r.clouds_pct, r.weather_main, r.weather_desc, r.visibility_m,
                        COALESCE(r.rain_1h_mm, 0), COALESCE(r.snow_1h_mm, 0)
                    FROM raw_weather_observations r
                    JOIN dim_location dl ON r.city = dl.city AND r.country_code = dl.country_code
                    JOIN dim_date dd ON dd.date_key = TO_CHAR(r.observation_dt::DATE, 'YYYYMMDD')::INTEGER
                    WHERE r.temp_celsius BETWEEN -90 AND 60
                """)
                transformed = cur.rowcount
            conn.commit()
            print(f"  [OK] fact_weather_readings: {transformed} records")
        except Exception as e:
            conn.rollback()
            print(f"  [ERROR] Weather transformation failed: {e}")
        finally:
            conn.close()

    return transformed


# ============================================================================
# NEWS TRANSFORMATIONS
# ============================================================================

def transform_news_data(pipeline_run_id: str = None) -> int:
    """Transform raw news articles into dim_news_source + fact_news_articles."""
    print("\n  [TRANSFORM] Running news data ELT transformations...")
    mode = get_mode()
    transformed = 0

    if mode == "local":
        conn = get_duckdb_connection()
        try:
            # Step 1: dim_news_source
            conn.execute("""
                INSERT INTO dim_news_source (source_key, source_id, source_name)
                SELECT
                    NEXTVAL('seq_news_source_key'),
                    COALESCE(NULLIF(r.source_id, ''), LOWER(REPLACE(r.source_name, ' ', '-'))),
                    r.source_name
                FROM (SELECT DISTINCT source_id, source_name FROM raw_news_articles
                      WHERE source_name IS NOT NULL AND TRIM(source_name) != '') r
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_news_source dns WHERE dns.source_name = r.source_name
                )
            """)
            src_count = conn.execute("SELECT COUNT(*) FROM dim_news_source").fetchone()[0]
            print(f"  [OK] dim_news_source: {src_count} total sources")

            # Step 2: fact_news_articles
            conn.execute("""
                INSERT INTO fact_news_articles (
                    news_fact_key, date_key, source_key, title, description, author,
                    url, topic, title_word_count, has_image, published_hour
                )
                SELECT
                    NEXTVAL('seq_news_fact'),
                    CAST(STRFTIME(CAST(r.published_at AS DATE), '%Y%m%d') AS INTEGER),
                    dns.source_key,
                    r.title, r.description, r.author, r.url, r.topic,
                    LENGTH(TRIM(r.title)) - LENGTH(REPLACE(TRIM(r.title), ' ', '')) + 1,
                    CASE WHEN r.url_to_image IS NOT NULL AND TRIM(r.url_to_image) != '' THEN TRUE ELSE FALSE END,
                    EXTRACT(HOUR FROM r.published_at)::INTEGER
                FROM raw_news_articles r
                JOIN dim_news_source dns ON r.source_name = dns.source_name
                WHERE r.title IS NOT NULL AND TRIM(r.title) != '' AND r.published_at IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM fact_news_articles f WHERE f.url = r.url
                  )
            """)
            transformed = conn.execute("SELECT COUNT(*) FROM fact_news_articles").fetchone()[0]
            print(f"  [OK] fact_news_articles: {transformed} total records")

        except Exception as e:
            print(f"  [ERROR] News transformation failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        conn = get_pg_connection()
        if conn is None:
            return 0
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dim_news_source (source_id, source_name)
                    SELECT DISTINCT COALESCE(NULLIF(r.source_id, ''), LOWER(REPLACE(r.source_name, ' ', '-'))), r.source_name
                    FROM raw_news_articles r WHERE r.source_name IS NOT NULL AND TRIM(r.source_name) != ''
                    ON CONFLICT (source_name) DO NOTHING
                """)
                cur.execute("""
                    INSERT INTO fact_news_articles (
                        date_key, source_key, title, description, author,
                        url, topic, title_word_count, has_image, published_hour
                    )
                    SELECT
                        TO_CHAR(r.published_at::DATE, 'YYYYMMDD')::INTEGER,
                        dns.source_key, r.title, r.description, r.author, r.url, r.topic,
                        ARRAY_LENGTH(STRING_TO_ARRAY(TRIM(r.title), ' '), 1),
                        CASE WHEN r.url_to_image IS NOT NULL AND TRIM(r.url_to_image) != '' THEN TRUE ELSE FALSE END,
                        EXTRACT(HOUR FROM r.published_at)::INTEGER
                    FROM raw_news_articles r
                    JOIN dim_news_source dns ON r.source_name = dns.source_name
                    JOIN dim_date dd ON dd.date_key = TO_CHAR(r.published_at::DATE, 'YYYYMMDD')::INTEGER
                    WHERE r.title IS NOT NULL AND TRIM(r.title) != '' AND r.published_at IS NOT NULL
                    ON CONFLICT (url) DO UPDATE SET title = EXCLUDED.title, description = EXCLUDED.description
                """)
                transformed = cur.rowcount
            conn.commit()
            print(f"  [OK] fact_news_articles: {transformed} records")
        except Exception as e:
            conn.rollback()
            print(f"  [ERROR] News transformation failed: {e}")
        finally:
            conn.close()

    return transformed


# ============================================================================
# FULL TRANSFORMATION PIPELINE
# ============================================================================

def run_all_transformations(pipeline_run_id: str = None) -> dict:
    """Execute all ELT transformations in sequence."""
    print("\n" + "=" * 60)
    print("  ELT TRANSFORMATION ENGINE -- FULL RUN")
    print("=" * 60)

    summary = {
        "data_quality": {},
        "stocks_transformed": 0,
        "weather_transformed": 0,
        "news_transformed": 0,
        "total_transformed": 0,
    }

    summary["data_quality"] = run_data_quality_gates(pipeline_run_id)
    summary["stocks_transformed"] = transform_stock_data(pipeline_run_id)
    summary["weather_transformed"] = transform_weather_data(pipeline_run_id)
    summary["news_transformed"] = transform_news_data(pipeline_run_id)

    summary["total_transformed"] = (
        summary["stocks_transformed"] +
        summary["weather_transformed"] +
        summary["news_transformed"]
    )

    print(f"\n  [TRANSFORM SUMMARY] Total transformed: {summary['total_transformed']}")
    return summary
