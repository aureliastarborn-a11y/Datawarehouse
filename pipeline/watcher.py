"""
Data Warehouse - Automated Directory Watcher
Monitors data/incoming/ directory for new data uploads (CSV, JSON, Parquet).
Triggers real-time ELT pipeline instantly upon file detection and archives processed files.
"""

import sys
import os
import time
import shutil
import pandas as pd
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.realtime_engine import realtime_engine

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INCOMING_DIR = os.path.join(PROJECT_ROOT, "data", "incoming")
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "data", "archive")
FAILED_DIR = os.path.join(PROJECT_ROOT, "data", "failed")

# Ensure required ingestion directories exist
for path in (INCOMING_DIR, ARCHIVE_DIR, FAILED_DIR):
    os.makedirs(path, exist_ok=True)


def process_incoming_file(filepath: str) -> Optional[dict]:
    """Process a single incoming data file through the real-time ELT engine."""
    filename = os.path.basename(filepath)
    print(f"\n[REALTIME WATCHER] File detected: {filename}")
    print(f"[REALTIME WATCHER] Processing through Data Quality Gates & Kimball Star Schema...")

    try:
        # Read file based on extension
        if filename.endswith(".csv"):
            df = pd.read_csv(filepath)
        elif filename.endswith(".json"):
            df = pd.read_json(filepath)
        elif filename.endswith(".parquet"):
            df = pd.read_parquet(filepath)
        else:
            print(f"[REALTIME WATCHER] Unsupported file extension: {filename}")
            return None

        if df.empty:
            print(f"[REALTIME WATCHER] Skipping empty file: {filename}")
            dest = os.path.join(FAILED_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}")
            shutil.move(filepath, dest)
            return None

        # Execute ELT Pipeline
        result = realtime_engine.process_raw_dataframe(df, source_type=f"file_drop:{filename}")

        # Move to archive directory upon success
        timestamp_prefix = datetime.now().strftime('%Y%m%d_%H%M%S')
        dest = os.path.join(ARCHIVE_DIR, f"{timestamp_prefix}_{filename}")
        shutil.move(filepath, dest)

        print(f"[REALTIME WATCHER] [SUCCESS] Ingested {result['submitted_rows']} rows. Status: {result['status']}. Moved to {dest}")
        return result

    except Exception as e:
        print(f"[REALTIME WATCHER] [ERROR] Ingestion failed for {filename}: {e}")
        dest = os.path.join(FAILED_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}")
        try:
            shutil.move(filepath, dest)
        except Exception:
            pass
        return {"status": "failed", "error": str(e)}


def start_file_watcher(poll_interval_seconds: float = 2.0):
    """
    Start continuous directory polling daemon for real-time automated ingestion.
    """
    print("=" * 70)
    print("  REAL-TIME AUTOMATED FILE WATCHER STARTED")
    print(f"  Monitoring Directory: {INCOMING_DIR}")
    print(f"  Poll Interval: {poll_interval_seconds}s")
    print("=" * 70)

    try:
        while True:
            files = [
                os.path.join(INCOMING_DIR, f) for f in os.listdir(INCOMING_DIR)
                if os.path.isfile(os.path.join(INCOMING_DIR, f)) and not f.startswith(".")
            ]

            for filepath in sorted(files):
                process_incoming_file(filepath)

            time.sleep(poll_interval_seconds)
    except KeyboardInterrupt:
        print("\n[REALTIME WATCHER] File watcher stopped by user.")


if __name__ == "__main__":
    start_file_watcher()
