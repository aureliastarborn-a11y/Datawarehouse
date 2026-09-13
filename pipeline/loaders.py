"""
Data Warehouse - Staging Layer Loaders
Loads extracted data into raw/staging tables and manages audit logging.
Supports both Supabase PostgreSQL and local DuckDB modes.
"""

import uuid
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.supabase_client import get_mode, execute_sql, get_duckdb_connection, get_pg_connection


# ============================================================================
# Pipeline Audit Logging
# ============================================================================

def create_pipeline_run(run_type: str, metadata: dict = None) -> str:
    """Create a new pipeline run record and return its UUID."""
    run_id = str(uuid.uuid4())
    mode = get_mode()

    if mode == "supabase":
        sql = """
            INSERT INTO pipeline_runs (id, run_type, status, metadata)
            VALUES (%s, %s, 'running', %s::jsonb)
        """
        execute_sql(sql, (run_id, run_type, json.dumps(metadata or {})))
    else:
        conn = get_duckdb_connection()
        conn.execute(
            "INSERT INTO pipeline_runs (id, run_type, status, metadata) VALUES (?, ?, 'running', ?)",
            [run_id, run_type, json.dumps(metadata or {})]
        )

    print(f"  [AUDIT] Pipeline run created: {run_id[:8]}... (type={run_type})")
    return run_id


def update_pipeline_run(
    run_id: str,
    status: str,
    records_extracted: int = 0,
    records_loaded: int = 0,
    records_transformed: int = 0,
    records_rejected: int = 0,
    error_message: str = None
):
    """Update an existing pipeline run with final status and metrics."""
    mode = get_mode()

    if mode == "supabase":
        sql = """
            UPDATE pipeline_runs
            SET status = %s, completed_at = NOW(),
                records_extracted = %s, records_loaded = %s,
                records_transformed = %s, records_rejected = %s, error_message = %s
            WHERE id = %s
        """
        execute_sql(sql, (status, records_extracted, records_loaded,
                          records_transformed, records_rejected, error_message, run_id))
    else:
        conn = get_duckdb_connection()
        conn.execute(
            """UPDATE pipeline_runs
               SET status = ?, completed_at = CURRENT_TIMESTAMP,
                   records_extracted = ?, records_loaded = ?,
                   records_transformed = ?, records_rejected = ?, error_message = ?
               WHERE id = ?""",
            [status, records_extracted, records_loaded,
             records_transformed, records_rejected, error_message, run_id]
        )

    print(f"  [AUDIT] Pipeline run {run_id[:8]}... updated: status={status}, loaded={records_loaded}")


def log_data_quality_issue(
    pipeline_run_id: str,
    table_name: str,
    check_name: str,
    check_type: str,
    records_checked: int = 0,
    records_failed: int = 0,
    severity: str = "warning",
    details: str = None
):
    """Log a data quality check result."""
    mode = get_mode()

    if mode == "supabase":
        sql = """
            INSERT INTO data_quality_log
                (pipeline_run_id, table_name, check_name, check_type,
                 records_checked, records_failed, severity, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_sql(sql, (pipeline_run_id, table_name, check_name, check_type,
                          records_checked, records_failed, severity, details))
    else:
        conn = get_duckdb_connection()
        conn.execute(
            """INSERT INTO data_quality_log
                   (pipeline_run_id, table_name, check_name, check_type,
                    records_checked, records_failed, severity, details)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [pipeline_run_id, table_name, check_name, check_type,
             records_checked, records_failed, severity, details]
        )

    # Dispatch real-time Webhook notification alert
    try:
        from pipeline.notifications import send_quality_failure_alert
        send_quality_failure_alert(table_name, check_name, records_failed, details or "", pipeline_run_id)
    except Exception as notify_err:
        print(f"[NOTIFICATION ERROR] Quality alert trigger failed: {notify_err}")


# ============================================================================
# Stock Data Loader
# ============================================================================

def load_raw_stocks(records: List[Dict[str, Any]], pipeline_run_id: str = None) -> int:
    """Load extracted stock price data into raw_stock_prices staging table."""
    if not records:
        print("  [LOAD] No stock records to load.")
        return 0

    print(f"  [LOAD] Loading {len(records)} stock records into raw_stock_prices...")
    mode = get_mode()
    loaded_count = 0

    if mode == "local":
        conn = get_duckdb_connection()
        for record in records:
            try:
                # Check for existing record
                exists = conn.execute(
                    "SELECT COUNT(*) FROM raw_stock_prices WHERE ticker = ? AND trade_date = ?",
                    [record["ticker"], record["trade_date"]]
                ).fetchone()[0]

                if exists > 0:
                    conn.execute(
                        """UPDATE raw_stock_prices
                           SET open_price=?, high_price=?, low_price=?, close_price=?,
                               volume=?, ingested_at=CURRENT_TIMESTAMP, pipeline_run_id=?
                           WHERE ticker=? AND trade_date=?""",
                        [record["open_price"], record["high_price"], record["low_price"],
                         record["close_price"], record["volume"], pipeline_run_id,
                         record["ticker"], record["trade_date"]]
                    )
                else:
                    conn.execute(
                        """INSERT INTO raw_stock_prices
                               (ticker, trade_date, open_price, high_price, low_price,
                                close_price, volume, source, pipeline_run_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        [record["ticker"], record["trade_date"],
                         record["open_price"], record["high_price"],
                         record["low_price"], record["close_price"],
                         record["volume"], record.get("source", "alpha_vantage"),
                         pipeline_run_id]
                    )
                loaded_count += 1
            except Exception as e:
                print(f"  [WARNING] Failed to load stock {record.get('ticker')} {record.get('trade_date')}: {e}")
    else:
        # Supabase/PostgreSQL mode
        conn = get_pg_connection()
        if conn is None:
            return 0
        try:
            with conn.cursor() as cur:
                for record in records:
                    try:
                        cur.execute("""
                            INSERT INTO raw_stock_prices
                                (ticker, trade_date, open_price, high_price, low_price,
                                 close_price, volume, source, pipeline_run_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ticker, trade_date, source) DO UPDATE SET
                                open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price,
                                low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price,
                                volume = EXCLUDED.volume, ingested_at = NOW(),
                                pipeline_run_id = EXCLUDED.pipeline_run_id
                        """, (record["ticker"], record["trade_date"],
                              record["open_price"], record["high_price"],
                              record["low_price"], record["close_price"],
                              record["volume"], record.get("source", "alpha_vantage"),
                              pipeline_run_id))
                        loaded_count += 1
                    except Exception as e:
                        print(f"  [WARNING] Failed: {e}")
                        conn.rollback()
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"  [ERROR] Stock loading failed: {e}")
        finally:
            conn.close()

    print(f"  [OK] Loaded {loaded_count}/{len(records)} stock records.")
    return loaded_count


# ============================================================================
# Weather Data Loader
# ============================================================================

def load_raw_weather(records: List[Dict[str, Any]], pipeline_run_id: str = None) -> int:
    """Load extracted weather data into raw_weather_observations."""
    if not records:
        print("  [LOAD] No weather records to load.")
        return 0

    print(f"  [LOAD] Loading {len(records)} weather records...")
    mode = get_mode()
    loaded_count = 0

    if mode == "local":
        conn = get_duckdb_connection()
        for record in records:
            try:
                conn.execute(
                    """INSERT INTO raw_weather_observations
                           (city, country_code, observation_dt, temp_celsius, feels_like_c,
                            temp_min_c, temp_max_c, pressure_hpa, humidity_pct,
                            wind_speed_ms, wind_deg, clouds_pct, weather_main, weather_desc,
                            visibility_m, rain_1h_mm, snow_1h_mm, latitude, longitude,
                            source, pipeline_run_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [record["city"], record.get("country_code", ""),
                     record["observation_dt"], record["temp_celsius"],
                     record.get("feels_like_c", 0), record.get("temp_min_c", 0),
                     record.get("temp_max_c", 0), record.get("pressure_hpa", 0),
                     record.get("humidity_pct", 0), record.get("wind_speed_ms", 0),
                     record.get("wind_deg", 0), record.get("clouds_pct", 0),
                     record.get("weather_main", ""), record.get("weather_desc", ""),
                     record.get("visibility_m", 0), record.get("rain_1h_mm", 0),
                     record.get("snow_1h_mm", 0), record.get("latitude", 0),
                     record.get("longitude", 0), record.get("source", "openweathermap"),
                     pipeline_run_id]
                )
                loaded_count += 1
            except Exception as e:
                print(f"  [WARNING] Failed weather load for {record.get('city')}: {e}")
    else:
        conn = get_pg_connection()
        if conn is None:
            return 0
        try:
            with conn.cursor() as cur:
                for record in records:
                    try:
                        published_at = record.get("observation_dt")
                        cur.execute("""
                            INSERT INTO raw_weather_observations
                                (city, country_code, observation_dt, temp_celsius, feels_like_c,
                                 temp_min_c, temp_max_c, pressure_hpa, humidity_pct,
                                 wind_speed_ms, wind_deg, clouds_pct, weather_main, weather_desc,
                                 visibility_m, rain_1h_mm, snow_1h_mm, latitude, longitude,
                                 source, pipeline_run_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (record["city"], record.get("country_code", ""),
                              published_at, record["temp_celsius"],
                              record.get("feels_like_c", 0), record.get("temp_min_c", 0),
                              record.get("temp_max_c", 0), record.get("pressure_hpa", 0),
                              record.get("humidity_pct", 0), record.get("wind_speed_ms", 0),
                              record.get("wind_deg", 0), record.get("clouds_pct", 0),
                              record.get("weather_main", ""), record.get("weather_desc", ""),
                              record.get("visibility_m", 0), record.get("rain_1h_mm", 0),
                              record.get("snow_1h_mm", 0), record.get("latitude", 0),
                              record.get("longitude", 0), record.get("source", "openweathermap"),
                              pipeline_run_id))
                        loaded_count += 1
                    except Exception as e:
                        print(f"  [WARNING] Failed: {e}")
                        conn.rollback()
            conn.commit()
        except Exception as e:
            conn.rollback()
        finally:
            conn.close()

    print(f"  [OK] Loaded {loaded_count}/{len(records)} weather records.")
    return loaded_count


# ============================================================================
# News Data Loader
# ============================================================================

def load_raw_news(records: List[Dict[str, Any]], pipeline_run_id: str = None) -> int:
    """Load extracted news articles into raw_news_articles."""
    if not records:
        print("  [LOAD] No news records to load.")
        return 0

    print(f"  [LOAD] Loading {len(records)} news records...")
    mode = get_mode()
    loaded_count = 0

    if mode == "local":
        conn = get_duckdb_connection()
        for record in records:
            try:
                url = record.get("url", "")
                # Check for duplicate URL
                exists = conn.execute(
                    "SELECT COUNT(*) FROM raw_news_articles WHERE url = ?", [url]
                ).fetchone()[0]

                if exists > 0:
                    conn.execute(
                        """UPDATE raw_news_articles
                           SET title=?, description=?, content=?,
                               ingested_at=CURRENT_TIMESTAMP, pipeline_run_id=?
                           WHERE url=?""",
                        [record["title"], record.get("description", ""),
                         record.get("content", ""), pipeline_run_id, url]
                    )
                else:
                    published_at = record.get("published_at", "")
                    if published_at and isinstance(published_at, str) and published_at.endswith("Z"):
                        published_at = published_at.replace("Z", "+00:00")

                    conn.execute(
                        """INSERT INTO raw_news_articles
                               (source_id, source_name, author, title, description, url,
                                url_to_image, published_at, content, topic, source_api, pipeline_run_id)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                        [record.get("source_id", ""), record.get("source_name", "Unknown"),
                         record.get("author", ""), record["title"],
                         record.get("description", ""), url,
                         record.get("url_to_image", ""),
                         published_at if published_at else None,
                         record.get("content", ""), record.get("topic", ""),
                         record.get("source_api", "newsapi"), pipeline_run_id]
                    )
                loaded_count += 1
            except Exception as e:
                print(f"  [WARNING] Failed news load: {e}")
    else:
        conn = get_pg_connection()
        if conn is None:
            return 0
        try:
            with conn.cursor() as cur:
                for record in records:
                    try:
                        published_at = record.get("published_at")
                        if published_at and isinstance(published_at, str) and published_at.endswith("Z"):
                            published_at = published_at.replace("Z", "+00:00")
                        cur.execute("""
                            INSERT INTO raw_news_articles
                                (source_id, source_name, author, title, description, url,
                                 url_to_image, published_at, content, topic, source_api, pipeline_run_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (url) DO UPDATE SET
                                title = EXCLUDED.title, description = EXCLUDED.description,
                                content = EXCLUDED.content, ingested_at = NOW(),
                                pipeline_run_id = EXCLUDED.pipeline_run_id
                        """, (record.get("source_id", ""), record.get("source_name", "Unknown"),
                              record.get("author", ""), record["title"],
                              record.get("description", ""), record.get("url", ""),
                              record.get("url_to_image", ""), published_at,
                              record.get("content", ""), record.get("topic", ""),
                              record.get("source_api", "newsapi"), pipeline_run_id))
                        loaded_count += 1
                    except Exception as e:
                        print(f"  [WARNING] Failed: {e}")
                        conn.rollback()
            conn.commit()
        except Exception as e:
            conn.rollback()
        finally:
            conn.close()

    print(f"  [OK] Loaded {loaded_count}/{len(records)} news records.")
    return loaded_count
