#!/usr/bin/env python3
"""
Real-Time Automated Data Warehouse Launcher
Starts the API ingestion server and directory file watcher concurrently.
"""

import sys
import os
import time
import threading

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


def start_watcher_thread():
    """Run directory watcher daemon in a background thread."""
    from pipeline.watcher import start_file_watcher
    start_file_watcher(poll_interval_seconds=1.5)


def main():
    print("""
+==============================================================+
|             REAL-TIME DATA WAREHOUSE DAEMON                  |
| ------------------------------------------------------------ |
|  - Ingestion: Automated File Watcher (data/incoming/)        |
|  - API: Real-Time Streaming Endpoints (/api/v1/ingest/*)     |
|  - Engine: Kimball Star Schema (SCD Type 2 + Quality Gates) |
|  - Database: DuckDB / Supabase Persistent Storage            |
+==============================================================+
    """)

    # Ensure incoming directory exists
    incoming_dir = os.path.join(PROJECT_ROOT, "data", "incoming")
    os.makedirs(incoming_dir, exist_ok=True)

    print(f"[REALTIME DAEMON] Monitoring incoming directory: {os.path.abspath(incoming_dir)}")
    print("[REALTIME DAEMON] Drop any CSV or JSON file into data/incoming/ to test instant ingestion.\n")

    # Start watcher background thread
    watcher_thread = threading.Thread(target=start_watcher_thread, daemon=True)
    watcher_thread.start()

    # Start background Supabase Cloud Replicator thread
    try:
        from pipeline.cloud_sync import start_background_cloud_replicator
        start_background_cloud_replicator(interval_seconds=30)
    except Exception as sync_err:
        print(f"[REALTIME DAEMON WARNING] Cloud replicator failed to start: {sync_err}")

    # Start FastAPI server
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    main()
