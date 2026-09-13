#!/usr/bin/env python3
"""
Data Warehouse - Main Entry Point
Initializes schema, runs pipelines, and starts the API server.

Usage:
    python run.py                    # Start API server (default)
    python run.py --init-schema      # Initialize Supabase database schema
    python run.py --run-pipeline     # Run full ETL/ELT pipeline once
    python run.py --serve            # Start FastAPI server
    python run.py --all              # Init schema + run pipeline + start server
    python run.py --status           # Check configuration status
"""

import os
import sys
import argparse

# Ensure project root is in path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def print_banner():
    print("""
+==============================================================+
|               DATA WAREHOUSE SYSTEM v2.0                     |
|  ----------------------------------------------------------- |
|   Backend: Supabase (PostgreSQL)                             |
|   Architecture: Kimball Star Schema                          |
|   Pipeline: ETL/ELT with OLAP Processing                    |
|   Domains: Stocks - Weather - News                           |
|   API: FastAPI + Swagger UI                                  |
+==============================================================+
    """)


def cmd_status():
    """Check configuration and connectivity status."""
    from config.supabase_client import validate_config

    print("=" * 55)
    print("  Configuration Status")
    print("=" * 55)

    config = validate_config()

    checks = [
        ("Supabase URL", config["supabase_url"]),
        ("Supabase Key", config["supabase_key"]),
        ("Supabase DB URL", config["supabase_db_url"]),
        ("Alpha Vantage API Key", config["alpha_vantage_key"]),
        ("OpenWeatherMap API Key", config["openweathermap_key"]),
        ("NewsAPI API Key", config["newsapi_key"]),
    ]

    for name, ok in checks:
        icon = "[OK]" if ok else "[  ]"
        status_text = "Configured" if ok else "NOT SET (update .env)"
        print(f"  {icon} {name}: {status_text}")

    print(f"\n  Stock Tickers: {', '.join(config['stock_tickers'])}")
    print(f"  Weather Cities: {', '.join(config['weather_cities'])}")
    print(f"  News Topics: {', '.join(config['news_topics'])}")

    # Test database connectivity
    print("\n  Testing database connection...")
    try:
        from config.supabase_client import execute_sql
        result = execute_sql("SELECT 1 AS check", fetch=True)
        if result:
            print("  [OK] Database connection: SUCCESS")
        else:
            print("  [  ] Database connection: FAILED (no result)")
    except Exception as e:
        print(f"  [  ] Database connection: FAILED ({e})")


def cmd_init_schema():
    """Initialize the Supabase database schema."""
    print("\n[INIT] Initializing Supabase database schema...")
    from config.supabase_client import init_schema
    success = init_schema()
    if success:
        print("[OK] Schema initialization complete!")
    else:
        print("[FAIL] Schema initialization failed. Check your Supabase credentials in .env")
        sys.exit(1)


def cmd_run_pipeline():
    """Run the full ETL/ELT pipeline."""
    from pipeline.orchestrator import run_full_pipeline
    result = run_full_pipeline()
    return result


def cmd_serve(host: str = "0.0.0.0", port: int = 8000):
    """Start the FastAPI server with optional background scheduler."""
    import uvicorn

    # Start background scheduler if configured
    auto_trigger = os.getenv("PIPELINE_AUTO_TRIGGER", "false").lower() == "true"
    schedule_minutes = int(os.getenv("PIPELINE_SCHEDULE_MINUTES", "60"))

    if auto_trigger:
        from pipeline.orchestrator import start_scheduler
        start_scheduler(schedule_minutes)
        print(f"[SCHEDULER] Auto-pipeline enabled — every {schedule_minutes} minutes")

    print(f"\n[SERVER] Starting FastAPI server on {host}:{port}")
    print(f"[SERVER] API Docs: http://localhost:{port}/docs")
    print(f"[SERVER] ReDoc:    http://localhost:{port}/redoc\n")

    uvicorn.run(
        "api.main:app",
        host=host,
        port=port,
        reload=False,
        log_level="info",
    )


def main():
    print_banner()

    parser = argparse.ArgumentParser(
        description="Data Warehouse System - Main Entry Point"
    )
    parser.add_argument("--status", action="store_true", help="Check configuration status")
    parser.add_argument("--init-schema", action="store_true", help="Initialize Supabase database schema")
    parser.add_argument("--run-pipeline", action="store_true", help="Run full ETL/ELT pipeline")
    parser.add_argument("--realtime", action="store_true", help="Start Real-Time automated DW ingestion daemon")
    parser.add_argument("--serve", action="store_true", help="Start FastAPI API server")
    parser.add_argument("--all", action="store_true", help="Init schema + run pipeline + start server")
    parser.add_argument("--host", default="0.0.0.0", help="API server host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="API server port (default: 8000)")

    args = parser.parse_args()

    if args.status:
        cmd_status()
    elif args.init_schema:
        cmd_init_schema()
    elif args.run_pipeline:
        cmd_run_pipeline()
    elif args.realtime:
        from scripts.run_realtime_dw import main as start_realtime
        start_realtime()
    elif args.serve:
        cmd_serve(args.host, args.port)
    elif args.all:
        cmd_init_schema()
        cmd_run_pipeline()
        cmd_serve(args.host, args.port)
    else:
        # Default: show status and start server
        cmd_status()
        print()
        cmd_serve(args.host, args.port)


if __name__ == "__main__":
    main()
