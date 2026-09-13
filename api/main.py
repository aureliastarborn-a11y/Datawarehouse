"""
Data Warehouse - FastAPI OLAP API Layer
Production API backend for data ingestion, pipeline triggers, and OLAP analytics.
"""

import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Depends, HTTPException, Security, status, BackgroundTasks, UploadFile, File
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import pandas as pd
import io

from config.supabase_client import (
    get_api_key,
    execute_sql,
    execute_sql_df,
    validate_config,
)
from pipeline.orchestrator import (
    run_full_pipeline,
    run_stock_pipeline,
    run_weather_pipeline,
    run_news_pipeline,
)
from pipeline.realtime_engine import realtime_engine


# ============================================================================
# FastAPI App Configuration
# ============================================================================

app = FastAPI(
    title="Data Warehouse OLAP API",
    description=(
        "Production FastAPI backend for a real Data Warehouse system. "
        "Provides ETL/ELT pipeline triggers, OLAP analytics endpoints, "
        "and data exploration capabilities. "
        "Powered by Supabase (PostgreSQL) with Kimball Star Schema."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount dbt Interactive Documentation Site at /dbt-docs
TARGET_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "target")
os.makedirs(TARGET_DIR, exist_ok=True)
try:
    from pipeline.dbt_runner import generate_dbt_docs_site
    generate_dbt_docs_site()
    app.mount("/dbt-docs", StaticFiles(directory=TARGET_DIR, html=True), name="dbt-docs")
except Exception as dbt_mount_err:
    print(f"[DBT DOCS MOUNT WARNING] {dbt_mount_err}")

# API Key Security
api_key_header_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(x_api_key: Optional[str] = Security(api_key_header_scheme)):
    """Verify the API key from the X-API-Key header."""
    expected = get_api_key()
    if not x_api_key or x_api_key != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header."
        )
    return x_api_key


# ============================================================================
# Request/Response Schemas
# ============================================================================

class PipelineResponse(BaseModel):
    status: str
    message: str
    run_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class AnalyticsResponse(BaseModel):
    domain: str
    engine: str = "Supabase PostgreSQL + Kimball Star Schema"
    generated_at: str
    data: Any


class TableInfoResponse(BaseModel):
    table_name: str
    row_count: int
    columns: List[str]


# ============================================================================
# HEALTH & STATUS ENDPOINTS
# ============================================================================

@app.get("/")
def root():
    """Service health check and status overview."""
    config = validate_config()
    return {
        "status": "online",
        "service": "Data Warehouse OLAP API",
        "version": "2.0.0",
        "engine": "Supabase PostgreSQL",
        "architecture": "Kimball Star Schema",
        "domains": ["stocks", "weather", "news"],
        "docs": "/docs",
        "config_status": {
            "supabase_connected": config["supabase_url"] and config["supabase_db_url"],
            "alpha_vantage_configured": config["alpha_vantage_key"],
            "openweathermap_configured": config["openweathermap_key"],
            "newsapi_configured": config["newsapi_key"],
        },
        "tracked": {
            "stock_tickers": config["stock_tickers"],
            "weather_cities": config["weather_cities"],
            "news_topics": config["news_topics"],
        }
    }


@app.get("/health")
def health_check():
    """Database connectivity health check."""
    try:
        result = execute_sql("SELECT 1 AS check", fetch=True)
        return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}


# ============================================================================
# PIPELINE TRIGGER ENDPOINTS
# ============================================================================

@app.post("/api/v1/pipeline/run-all", dependencies=[Depends(verify_api_key)], response_model=PipelineResponse)
def trigger_full_pipeline(background_tasks: BackgroundTasks):
    """Trigger the full ETL/ELT pipeline for all domains (stocks, weather, news)."""
    background_tasks.add_task(run_full_pipeline)
    return PipelineResponse(
        status="accepted",
        message="Full pipeline triggered in background. Check /api/v1/pipeline/history for status.",
    )


@app.post("/api/v1/pipeline/stocks", dependencies=[Depends(verify_api_key)], response_model=PipelineResponse)
def trigger_stock_pipeline(background_tasks: BackgroundTasks):
    """Trigger stock data extraction and transformation pipeline."""
    background_tasks.add_task(run_stock_pipeline)
    return PipelineResponse(
        status="accepted",
        message="Stock pipeline triggered in background.",
    )


@app.post("/api/v1/pipeline/weather", dependencies=[Depends(verify_api_key)], response_model=PipelineResponse)
def trigger_weather_pipeline(background_tasks: BackgroundTasks):
    """Trigger weather data extraction and transformation pipeline."""
    background_tasks.add_task(run_weather_pipeline)
    return PipelineResponse(
        status="accepted",
        message="Weather pipeline triggered in background.",
    )


@app.post("/api/v1/pipeline/news", dependencies=[Depends(verify_api_key)], response_model=PipelineResponse)
def trigger_news_pipeline(background_tasks: BackgroundTasks):
    """Trigger news data extraction and transformation pipeline."""
    background_tasks.add_task(run_news_pipeline)
    return PipelineResponse(
        status="accepted",
        message="News pipeline triggered in background.",
    )


@app.get("/api/v1/pipeline/history", dependencies=[Depends(verify_api_key)])
def get_pipeline_history(limit: int = 20):
    """Get pipeline execution audit trail."""
    try:
        results = execute_sql("""
            SELECT
                id, run_type, status, started_at, completed_at,
                records_extracted, records_loaded, records_transformed,
                records_rejected, error_message
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT %s
        """, (limit,), fetch=True)
        return {
            "total_runs": len(results),
            "history": [dict(r) for r in results]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch pipeline history: {e}")


# ============================================================================
# REAL-TIME AUTOMATED DATA INGESTION ENDPOINTS
# ============================================================================

@app.post("/api/v1/ingest/csv", dependencies=[Depends(verify_api_key)])
async def ingest_csv_file(file: UploadFile = File(...)):
    """
    Real-Time CSV Upload Endpoint:
    Receives raw CSV file upload, runs data quality gates, updates SCD2 customer/product
    dimensions, populates Kimball fact_sales, and stores records in database instantly.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")

    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        if df.empty:
            raise HTTPException(status_code=400, detail="Uploaded CSV file is empty.")

        summary = realtime_engine.process_raw_dataframe(df, source_type=f"api_upload:{file.filename}")
        return {
            "message": f"Successfully processed {len(df)} records in real-time.",
            "summary": summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Real-time CSV ingestion failed: {e}")


@app.post("/api/v1/ingest/json", dependencies=[Depends(verify_api_key)])
def ingest_json_payload(records: List[Dict[str, Any]]):
    """
    Real-Time JSON Stream Ingestion Endpoint:
    Receives a list of order/customer/product records, executes immediate ELT,
    and updates the database in real-time.
    """
    if not records:
        raise HTTPException(status_code=400, detail="Record list cannot be empty.")

    try:
        df = pd.DataFrame(records)
        summary = realtime_engine.process_raw_dataframe(df, source_type="api_json_stream")
        return {
            "message": f"Successfully ingested {len(records)} streaming records in real-time.",
            "summary": summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Real-time JSON ingestion failed: {e}")


@app.get("/api/v1/realtime/stream-status", dependencies=[Depends(verify_api_key)])
def get_realtime_stream_status():
    """Get live real-time Data Warehouse status, row counts, and audit metrics."""
    try:
        customers = execute_sql("SELECT COUNT(*) AS cnt FROM dim_customer", fetch=True)[0]["cnt"]
        products = execute_sql("SELECT COUNT(*) AS cnt FROM dim_product", fetch=True)[0]["cnt"]
        facts = execute_sql("SELECT COUNT(*) AS cnt FROM fact_sales", fetch=True)[0]["cnt"]
        quality_issues = execute_sql("SELECT COUNT(*) AS cnt FROM data_quality_log", fetch=True)[0]["cnt"]

        latest_run = execute_sql("""
            SELECT id, run_type, status, started_at, completed_at, records_loaded
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1
        """, fetch=True)

        return {
            "status": "active",
            "mode": "realtime_event_driven",
            "timestamp": datetime.now().isoformat(),
            "warehouse_row_counts": {
                "dim_customer_scd2": customers,
                "dim_product": products,
                "fact_sales": facts,
            },
            "total_quality_check_issues": quality_issues,
            "latest_pipeline_run": dict(latest_run[0]) if latest_run else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch stream status: {e}")


@app.get("/api/v1/pipeline/quality-log", dependencies=[Depends(verify_api_key)])
def get_data_quality_log(limit: int = 50):
    """Get data quality check audit log."""
    try:
        results = execute_sql("""
            SELECT
                dql.id, dql.table_name, dql.check_name, dql.check_type,
                dql.records_checked, dql.records_failed, dql.severity,
                dql.details, dql.checked_at,
                pr.run_type AS pipeline_run_type
            FROM data_quality_log dql
            LEFT JOIN pipeline_runs pr ON dql.pipeline_run_id = pr.id
            ORDER BY dql.checked_at DESC
            LIMIT %s
        """, (limit,), fetch=True)
        return {
            "total_checks": len(results),
            "quality_log": [dict(r) for r in results]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch quality log: {e}")


# ============================================================================
# OLAP ANALYTICS ENDPOINTS
# ============================================================================

@app.get("/api/v1/analytics/stocks", dependencies=[Depends(verify_api_key)])
def get_stock_analytics():
    """Get stock OLAP analytics — monthly summaries, top performers, volatility."""
    try:
        # Monthly summary from OLAP view
        monthly = execute_sql("""
            SELECT * FROM olap_stock_monthly_summary
            ORDER BY year DESC, month DESC, ticker
            LIMIT 100
        """, fetch=True)

        # Overall KPIs
        kpis = execute_sql("""
            SELECT
                COUNT(DISTINCT company_key) AS total_companies,
                COUNT(*) AS total_trading_records,
                MIN(dd.full_date) AS data_from,
                MAX(dd.full_date) AS data_to,
                ROUND(AVG(close_price)::NUMERIC, 2) AS avg_close_price,
                ROUND(AVG(daily_return_pct)::NUMERIC, 4) AS avg_daily_return_pct,
                ROUND(AVG(daily_range_pct)::NUMERIC, 4) AS avg_volatility_pct
            FROM fact_stock_prices f
            JOIN dim_date dd ON f.date_key = dd.date_key
        """, fetch=True)

        return {
            "domain": "stocks",
            "engine": "Supabase PostgreSQL + Kimball Star Schema",
            "generated_at": datetime.now().isoformat(),
            "kpis": dict(kpis[0]) if kpis else {},
            "monthly_summary": [dict(r) for r in monthly],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stock analytics failed: {e}")


@app.get("/api/v1/analytics/weather", dependencies=[Depends(verify_api_key)])
def get_weather_analytics():
    """Get weather OLAP analytics — temperature trends, city comparisons."""
    try:
        # Daily summary from OLAP view
        daily = execute_sql("""
            SELECT * FROM olap_weather_daily_summary
            ORDER BY full_date DESC, city
            LIMIT 100
        """, fetch=True)

        # Overall KPIs
        kpis = execute_sql("""
            SELECT
                COUNT(DISTINCT location_key) AS total_cities,
                COUNT(*) AS total_observations,
                ROUND(AVG(temp_celsius)::NUMERIC, 2) AS avg_global_temp_c,
                ROUND(MIN(temp_celsius)::NUMERIC, 2) AS min_recorded_temp_c,
                ROUND(MAX(temp_celsius)::NUMERIC, 2) AS max_recorded_temp_c,
                ROUND(AVG(humidity_pct)::NUMERIC, 1) AS avg_humidity_pct,
                ROUND(AVG(wind_speed_ms)::NUMERIC, 2) AS avg_wind_speed_ms
            FROM fact_weather_readings
        """, fetch=True)

        return {
            "domain": "weather",
            "engine": "Supabase PostgreSQL + Kimball Star Schema",
            "generated_at": datetime.now().isoformat(),
            "kpis": dict(kpis[0]) if kpis else {},
            "daily_summary": [dict(r) for r in daily],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Weather analytics failed: {e}")


@app.get("/api/v1/analytics/news", dependencies=[Depends(verify_api_key)])
def get_news_analytics():
    """Get news OLAP analytics — volume by topic, source distribution."""
    try:
        # Daily summary from OLAP view
        daily = execute_sql("""
            SELECT * FROM olap_news_daily_summary
            ORDER BY full_date DESC
            LIMIT 100
        """, fetch=True)

        # Overall KPIs
        kpis = execute_sql("""
            SELECT
                COUNT(*) AS total_articles,
                COUNT(DISTINCT source_key) AS total_sources,
                COUNT(DISTINCT topic) AS total_topics,
                ROUND(AVG(title_word_count)::NUMERIC, 1) AS avg_title_length,
                SUM(CASE WHEN has_image THEN 1 ELSE 0 END) AS articles_with_images
            FROM fact_news_articles
        """, fetch=True)

        # Top sources
        top_sources = execute_sql("""
            SELECT
                dns.source_name,
                COUNT(*) AS article_count
            FROM fact_news_articles f
            JOIN dim_news_source dns ON f.source_key = dns.source_key
            GROUP BY dns.source_name
            ORDER BY article_count DESC
            LIMIT 10
        """, fetch=True)

        return {
            "domain": "news",
            "engine": "Supabase PostgreSQL + Kimball Star Schema",
            "generated_at": datetime.now().isoformat(),
            "kpis": dict(kpis[0]) if kpis else {},
            "top_sources": [dict(r) for r in top_sources],
            "daily_summary": [dict(r) for r in daily],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"News analytics failed: {e}")


@app.get("/api/v1/analytics/cross-domain", dependencies=[Depends(verify_api_key)])
def get_cross_domain_analytics():
    """Get cross-domain correlation analytics — stocks vs weather vs news."""
    try:
        results = execute_sql("""
            SELECT * FROM olap_cross_domain_daily
            ORDER BY full_date DESC
            LIMIT 60
        """, fetch=True)

        return {
            "domain": "cross-domain",
            "engine": "Supabase PostgreSQL + Kimball Star Schema",
            "generated_at": datetime.now().isoformat(),
            "daily_correlation": [dict(r) for r in results],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cross-domain analytics failed: {e}")


# ============================================================================
# MACHINE LEARNING & PREDICTIVE ANALYTICS ENDPOINTS
# ============================================================================

@app.get("/api/v1/ml/forecast/stocks/{ticker}", dependencies=[Depends(verify_api_key)])
def get_stock_forecast(ticker: str, days: int = 14):
    """Predict stock prices and volatility confidence intervals (1-30 days ahead)."""
    try:
        from ml.predictor import forecast_stock_prices
        return forecast_stock_prices(ticker, days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ML Stock forecast failed: {e}")


@app.get("/api/v1/ml/anomalies/weather", dependencies=[Depends(verify_api_key)])
def get_weather_anomalies(threshold_zscore: float = 1.5):
    """Detect statistical weather anomalies using Z-score outlier detection."""
    try:
        from ml.predictor import detect_weather_anomalies
        return detect_weather_anomalies(threshold_zscore)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ML Weather anomaly detection failed: {e}")


@app.get("/api/v1/ml/sentiment/news", dependencies=[Depends(verify_api_key)])
def get_news_sentiment_analysis():
    """Analyze news headlines for sentiment orientation and topic polarity."""
    try:
        from ml.predictor import analyze_news_sentiment
        return analyze_news_sentiment()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ML News sentiment analysis failed: {e}")


@app.get("/api/v1/ml/churn-risk", dependencies=[Depends(verify_api_key)])
def get_customer_churn_risk_analysis():
    """Predict customer churn probabilities and identify high-risk customer profiles."""
    try:
        from ml.predictor import predict_customer_churn_risk
        return predict_customer_churn_risk()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ML Customer Churn prediction failed: {e}")


@app.get("/api/v1/ml/revenue-forecast", dependencies=[Depends(verify_api_key)])
def get_sales_revenue_forecast(forecast_months: int = 3):
    """Predict monthly net revenue, profit margins, and unit demand using Holt-Winters smoothing."""
    try:
        from ml.predictor import forecast_sales_revenue
        return forecast_sales_revenue(forecast_months)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ML Sales Revenue forecast failed: {e}")


@app.get("/api/v1/ml/order-anomalies", dependencies=[Depends(verify_api_key)])
def get_order_transaction_anomalies(zscore_threshold: float = 1.8):
    """Detect statistical order amount anomalies and price irregularities."""
    try:
        from ml.predictor import detect_order_anomalies
        return detect_order_anomalies(zscore_threshold)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ML Order anomaly detection failed: {e}")


# ============================================================================
# REAL-TIME WEBHOOK & ALERT NOTIFICATION ENDPOINTS
# ============================================================================

@app.post("/api/v1/notifications/test", dependencies=[Depends(verify_api_key)])
def trigger_test_notification(title: str = "System Health Check", alert_type: str = "info"):
    """Trigger a test Webhook notification alert (Slack / Discord / Console)."""
    try:
        from pipeline.notifications import send_webhook_notification
        res = send_webhook_notification(
            title=title,
            message="Test notification alert dispatched from FastAPI Real-Time DW Endpoint.",
            alert_type=alert_type,
            metadata={"environment": "Production", "test_status": "Success"}
        )
        return {"status": "dispatched", "notification": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Test notification failed: {e}")


@app.get("/api/v1/notifications/history", dependencies=[Depends(verify_api_key)])
def get_recent_notifications(limit: int = 20):
    """Get history of dispatched real-time alert notifications."""
    try:
        from pipeline.notifications import get_notification_history
        return {
            "total_alerts": len(get_notification_history(limit)),
            "alerts": get_notification_history(limit)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch notification history: {e}")


# ============================================================================
# SUPABASE POSTGRESQL CLOUD REPLICATION ENDPOINTS
# ============================================================================

@app.post("/api/v1/cloud/sync-now", dependencies=[Depends(verify_api_key)])
def trigger_cloud_sync_now():
    """Trigger immediate synchronization of local DuckDB tables to Supabase PostgreSQL Cloud."""
    try:
        from pipeline.cloud_sync import sync_duckdb_to_supabase
        res = sync_duckdb_to_supabase()
        return {"message": "Cloud synchronization triggered.", "sync_summary": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cloud sync trigger failed: {e}")


@app.get("/api/v1/cloud/sync-status", dependencies=[Depends(verify_api_key)])
def get_cloud_replication_status():
    """Get current status of Supabase PostgreSQL Cloud Replication."""
    try:
        from pipeline.cloud_sync import get_cloud_sync_status, is_cloud_sync_configured
        status = get_cloud_sync_status()
        status["is_configured"] = is_cloud_sync_configured()
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch cloud sync status: {e}")


# ============================================================================
# dbt INCREMENTAL TESTS & LIVE DOCS SERVER ENDPOINTS
# ============================================================================

@app.post("/api/v1/dbt/run-tests", dependencies=[Depends(verify_api_key)])
def trigger_dbt_tests(run_id: Optional[str] = None):
    """Programmatically execute dbt model assertions (uniqueness, referential integrity, non-null)."""
    try:
        from pipeline.dbt_runner import run_dbt_tests
        res = run_dbt_tests(run_id)
        return {"message": "dbt model test suite executed.", "summary": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dbt test execution failed: {e}")


@app.post("/api/v1/dbt/generate-docs", dependencies=[Depends(verify_api_key)])
def trigger_dbt_docs_generation():
    """Re-compile and update the interactive dbt documentation static site."""
    try:
        from pipeline.dbt_runner import generate_dbt_docs_site
        res = generate_dbt_docs_site()
        return {"message": "dbt documentation site generated.", "summary": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dbt docs generation failed: {e}")


@app.get("/api/v1/dbt/test-results", dependencies=[Depends(verify_api_key)])
def get_dbt_test_results():
    """Retrieve the latest dbt test assertion results."""
    try:
        from pipeline.dbt_runner import get_latest_dbt_test_results
        return get_latest_dbt_test_results()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch dbt test results: {e}")


# ============================================================================
# DATA EXPLORATION ENDPOINTS
# ============================================================================

@app.get("/api/v1/warehouse/tables", dependencies=[Depends(verify_api_key)])
def list_warehouse_tables():
    """List all data warehouse tables with row counts."""
    try:
        tables = [
            "raw_stock_prices", "raw_weather_observations", "raw_news_articles",
            "dim_date", "dim_company", "dim_location", "dim_news_source",
            "fact_stock_prices", "fact_weather_readings", "fact_news_articles",
            "pipeline_runs", "data_quality_log",
        ]
        table_info = []
        for table in tables:
            try:
                result = execute_sql(f"SELECT COUNT(*) AS cnt FROM {table}", fetch=True)
                count = result[0]["cnt"] if result else 0
                table_info.append({"table_name": table, "row_count": count})
            except Exception:
                table_info.append({"table_name": table, "row_count": -1, "error": "table not found"})

        return {
            "warehouse_tables": table_info,
            "total_tables": len(tables),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {e}")


@app.get("/api/v1/warehouse/table/{table_name}", dependencies=[Depends(verify_api_key)])
def query_table(table_name: str, limit: int = 50):
    """Query a specific warehouse table (read-only, with limit)."""
    allowed_tables = {
        "raw_stock_prices", "raw_weather_observations", "raw_news_articles",
        "dim_date", "dim_company", "dim_location", "dim_news_source",
        "fact_stock_prices", "fact_weather_readings", "fact_news_articles",
        "pipeline_runs", "data_quality_log",
    }

    if table_name not in allowed_tables:
        raise HTTPException(status_code=400, detail=f"Table '{table_name}' not found. Allowed: {sorted(allowed_tables)}")

    try:
        limit = min(limit, 500)  # Cap at 500 rows
        results = execute_sql(f"SELECT * FROM {table_name} LIMIT %s", (limit,), fetch=True)
        return {
            "table_name": table_name,
            "row_count": len(results),
            "data": [dict(r) for r in results],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query {table_name}: {e}")
