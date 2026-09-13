"""
Data Warehouse - Pipeline Orchestrator
Coordinates the full Extract -> Load -> Transform pipeline lifecycle.
Supports manual triggers, scheduled runs, and auto-detection of data changes.
"""

import sys
import os
import time
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.supabase_client import (
    get_alpha_vantage_key,
    get_openweathermap_key,
    get_newsapi_key,
    get_stock_tickers,
    get_weather_cities,
    get_news_topics,
)
from pipeline.extractors import (
    extract_multiple_stocks,
    extract_multiple_weather,
    extract_multiple_news,
)
from pipeline.loaders import (
    create_pipeline_run,
    update_pipeline_run,
    load_raw_stocks,
    load_raw_weather,
    load_raw_news,
)
from pipeline.transformers import run_all_transformations


# ============================================================================
# FULL PIPELINE EXECUTION
# ============================================================================

def run_full_pipeline() -> dict:
    """
    Execute the complete ETL/ELT pipeline:
    1. EXTRACT: Pull data from external APIs (or synthetic fallback)
    2. LOAD: Insert raw data into staging tables
    3. TRANSFORM: Run ELT to populate star schema dimensions & facts

    Returns:
        Pipeline execution summary dict.
    """
    print("\n" + "=" * 70)
    print("  DATA WAREHOUSE - FULL PIPELINE EXECUTION")
    print(f"  Started at: {datetime.now().isoformat()}")
    print("=" * 70)

    # Create audit record
    run_id = create_pipeline_run("full", metadata={
        "tickers": get_stock_tickers(),
        "cities": get_weather_cities(),
        "topics": get_news_topics(),
    })

    summary = {
        "run_id": run_id,
        "status": "running",
        "extracted": {"stocks": 0, "weather": 0, "news": 0},
        "loaded": {"stocks": 0, "weather": 0, "news": 0},
        "transformed": {},
    }

    total_extracted = 0
    total_loaded = 0

    try:
        # =====================================================================
        # PHASE 1: EXTRACT - Pull from External APIs (or Fallback)
        # =====================================================================
        print("\n" + "-" * 50)
        print("  PHASE 1: EXTRACT (External API Data Pulling)")
        print("-" * 50)

        # Extract Stocks
        alpha_key = get_alpha_vantage_key()
        tickers = get_stock_tickers()
        stock_records = extract_multiple_stocks(tickers, alpha_key, delay_seconds=2.0)
        summary["extracted"]["stocks"] = len(stock_records)
        total_extracted += len(stock_records)

        # Extract Weather
        owm_key = get_openweathermap_key()
        cities = get_weather_cities()
        weather_records = extract_multiple_weather(cities, owm_key, delay_seconds=0.5)
        summary["extracted"]["weather"] = len(weather_records)
        total_extracted += len(weather_records)

        # Extract News
        news_key = get_newsapi_key()
        topics = get_news_topics()
        news_records = extract_multiple_news(topics, news_key, delay_seconds=0.5)
        summary["extracted"]["news"] = len(news_records)
        total_extracted += len(news_records)

        print(f"\n  [EXTRACT COMPLETE] Total records extracted: {total_extracted}")

        # =====================================================================
        # PHASE 2: LOAD - Insert into Staging Tables
        # =====================================================================
        print("\n" + "-" * 50)
        print("  PHASE 2: LOAD (Staging Layer Ingestion)")
        print("-" * 50)

        loaded_stocks = load_raw_stocks(stock_records, run_id)
        loaded_weather = load_raw_weather(weather_records, run_id)
        loaded_news = load_raw_news(news_records, run_id)

        summary["loaded"]["stocks"] = loaded_stocks
        summary["loaded"]["weather"] = loaded_weather
        summary["loaded"]["news"] = loaded_news
        total_loaded = loaded_stocks + loaded_weather + loaded_news

        print(f"\n  [LOAD COMPLETE] Total records loaded: {total_loaded}")

        # =====================================================================
        # PHASE 3: TRANSFORM - ELT into Star Schema
        # =====================================================================
        print("\n" + "-" * 50)
        print("  PHASE 3: TRANSFORM (ELT -> Star Schema)")
        print("-" * 50)

        transform_summary = run_all_transformations(run_id)
        summary["transformed"] = transform_summary

        # =====================================================================
        # PIPELINE SUCCESS
        # =====================================================================
        summary["status"] = "completed"
        update_pipeline_run(
            run_id,
            status="completed",
            records_extracted=total_extracted,
            records_loaded=total_loaded,
            records_transformed=transform_summary.get("total_transformed", 0),
        )

        print("\n" + "=" * 70)
        print("  PIPELINE COMPLETED SUCCESSFULLY")
        print(f"  Finished at: {datetime.now().isoformat()}")
        print(f"  Extracted: {total_extracted} | Loaded: {total_loaded} | Transformed: {transform_summary.get('total_transformed', 0)}")
        print("=" * 70)

    except Exception as e:
        summary["status"] = "failed"
        summary["error"] = str(e)
        update_pipeline_run(
            run_id,
            status="failed",
            records_extracted=total_extracted,
            records_loaded=total_loaded,
            error_message=str(e),
        )
        print(f"\n  [PIPELINE FAILED] {e}")

    return summary


# ============================================================================
# DOMAIN-SPECIFIC PIPELINES
# ============================================================================

def run_stock_pipeline() -> dict:
    """Run pipeline for stocks only."""
    print("\n[PIPELINE] Running stock-only pipeline...")
    run_id = create_pipeline_run("stocks")

    try:
        alpha_key = get_alpha_vantage_key()
        tickers = get_stock_tickers()
        records = extract_multiple_stocks(tickers, alpha_key, delay_seconds=2.0)
        loaded = load_raw_stocks(records, run_id)
        from pipeline.transformers import transform_stock_data
        transformed = transform_stock_data(run_id)

        update_pipeline_run(run_id, "completed",
            records_extracted=len(records), records_loaded=loaded, records_transformed=transformed)

        return {"run_id": run_id, "status": "completed", "extracted": len(records),
                "loaded": loaded, "transformed": transformed}
    except Exception as e:
        update_pipeline_run(run_id, "failed", error_message=str(e))
        return {"run_id": run_id, "status": "failed", "error": str(e)}


def run_weather_pipeline() -> dict:
    """Run pipeline for weather only."""
    print("\n[PIPELINE] Running weather-only pipeline...")
    run_id = create_pipeline_run("weather")

    try:
        owm_key = get_openweathermap_key()
        cities = get_weather_cities()
        records = extract_multiple_weather(cities, owm_key)
        loaded = load_raw_weather(records, run_id)
        from pipeline.transformers import transform_weather_data
        transformed = transform_weather_data(run_id)

        update_pipeline_run(run_id, "completed",
            records_extracted=len(records), records_loaded=loaded, records_transformed=transformed)

        return {"run_id": run_id, "status": "completed", "extracted": len(records),
                "loaded": loaded, "transformed": transformed}
    except Exception as e:
        update_pipeline_run(run_id, "failed", error_message=str(e))
        return {"run_id": run_id, "status": "failed", "error": str(e)}


def run_news_pipeline() -> dict:
    """Run pipeline for news only."""
    print("\n[PIPELINE] Running news-only pipeline...")
    run_id = create_pipeline_run("news")

    try:
        news_key = get_newsapi_key()
        topics = get_news_topics()
        records = extract_multiple_news(topics, news_key)
        loaded = load_raw_news(records, run_id)
        from pipeline.transformers import transform_news_data
        transformed = transform_news_data(run_id)

        update_pipeline_run(run_id, "completed",
            records_extracted=len(records), records_loaded=loaded, records_transformed=transformed)

        return {"run_id": run_id, "status": "completed", "extracted": len(records),
                "loaded": loaded, "transformed": transformed}
    except Exception as e:
        update_pipeline_run(run_id, "failed", error_message=str(e))
        return {"run_id": run_id, "status": "failed", "error": str(e)}


# ============================================================================
# SCHEDULER (APScheduler-based auto-trigger)
# ============================================================================

_scheduler = None

def start_scheduler(interval_minutes: int = 60):
    """
    Start an APScheduler background scheduler that runs the full pipeline
    at the specified interval.
    """
    global _scheduler

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.interval import IntervalTrigger
    except ImportError:
        print("[WARNING] APScheduler not installed. Run: pip install apscheduler")
        return None

    if _scheduler is not None:
        print("[SCHEDULER] Scheduler already running.")
        return _scheduler

    _scheduler = BackgroundScheduler()
    _scheduler.add_job(
        run_full_pipeline,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="full_pipeline_job",
        name="Full Data Warehouse Pipeline",
        replace_existing=True,
    )
    _scheduler.start()
    print(f"[SCHEDULER] Pipeline scheduler started - runs every {interval_minutes} minutes")
    return _scheduler


def stop_scheduler():
    """Stop the background scheduler."""
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown()
        _scheduler = None
        print("[SCHEDULER] Pipeline scheduler stopped.")


# ============================================================================
# CLI ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Data Warehouse Pipeline Orchestrator")
    parser.add_argument("--full", action="store_true", help="Run full pipeline (all domains)")
    parser.add_argument("--stocks", action="store_true", help="Run stock pipeline only")
    parser.add_argument("--weather", action="store_true", help="Run weather pipeline only")
    parser.add_argument("--news", action="store_true", help="Run news pipeline only")
    parser.add_argument("--schedule", type=int, default=0, help="Start scheduler with interval in minutes")

    args = parser.parse_args()

    if args.stocks:
        run_stock_pipeline()
    elif args.weather:
        run_weather_pipeline()
    elif args.news:
        run_news_pipeline()
    elif args.schedule > 0:
        start_scheduler(args.schedule)
        print("Scheduler running. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            stop_scheduler()
    else:
        # Default: run full pipeline
        run_full_pipeline()
