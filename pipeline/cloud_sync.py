"""
Data Warehouse - Supabase PostgreSQL Cloud Replication & Sync Engine
Synchronizes transformed Kimball Star Schema tables (dim_customer, dim_product, dim_date, fact_sales)
and staging tables from local DuckDB database into Supabase PostgreSQL cloud instance.
"""

import sys
import os
import time
import threading
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.supabase_client import is_supabase_configured, get_pg_connection, get_duckdb_connection, execute_sql_df

# Global Sync Audit State
LAST_SYNC_STATUS: Dict[str, Any] = {
    "status": "idle",
    "last_sync_timestamp": None,
    "tables_synced": [],
    "total_rows_replicated": 0,
    "mode": "local_dry_run",
    "execution_time_ms": 0.0
}

_replicator_thread = None
_stop_replicator = False


def is_cloud_sync_configured() -> bool:
    """Check if Supabase PostgreSQL cloud credentials are set."""
    return is_supabase_configured()


def sync_duckdb_to_supabase() -> Dict[str, Any]:
    """
    Synchronize all dimensional, fact, and audit tables from local DuckDB to Supabase PostgreSQL.
    Performs upserts / bulk replacement to keep remote BI dashboards live.
    """
    global LAST_SYNC_STATUS
    start_time = time.time()
    timestamp = datetime.now().isoformat()
    cloud_active = is_cloud_sync_configured()

    tables_to_sync = [
        "stg_customers", "stg_products", "stg_orders",
        "dim_date", "dim_customer", "dim_product", "fact_sales",
        "pipeline_runs", "data_quality_log"
    ]

    summary = {
        "status": "running",
        "timestamp": timestamp,
        "mode": "supabase_cloud" if cloud_active else "local_dry_run",
        "tables_synced": [],
        "total_rows_replicated": 0,
        "execution_time_ms": 0.0
    }

    try:
        if cloud_active:
            pg_conn = get_pg_connection()
            if pg_conn is None:
                raise ConnectionError("Failed to establish Supabase PostgreSQL connection.")

            try:
                with pg_conn.cursor() as cur:
                    for table in tables_to_sync:
                        duck_conn = get_duckdb_connection()
                        try:
                            df = duck_conn.execute(f"SELECT * FROM {table}").df()
                        finally:
                            duck_conn.close()
                        if df.empty:
                            continue

                        # Delete & Bulk insert for clean dimensional synchronization
                        cur.execute(f"TRUNCATE TABLE {table} CASCADE;")
                        
                        import psycopg2.extras

                        cols = list(df.columns)
                        col_names = ", ".join(cols)
                        insert_sql = f"INSERT INTO {table} ({col_names}) VALUES %s"

                        records = [tuple(None if pd.isna(val) else val for val in row) for row in df.to_numpy()]
                        psycopg2.extras.execute_values(cur, insert_sql, records, page_size=1000)

                        summary["tables_synced"].append({"table_name": table, "rows": len(df)})
                        summary["total_rows_replicated"] += len(df)

                pg_conn.commit()
                print(f"[CLOUD SYNC SUCCESS] Replicated {summary['total_rows_replicated']} rows to Supabase PostgreSQL.")
            except Exception as e:
                pg_conn.rollback()
                raise e
            finally:
                pg_conn.close()
        else:
            # Fallback local dry-run replication validation
            for table in tables_to_sync:
                try:
                    df = execute_sql_df(f"SELECT * FROM {table}")
                    summary["tables_synced"].append({"table_name": table, "rows": len(df)})
                    summary["total_rows_replicated"] += len(df)
                except Exception:
                    pass
            print(f"[CLOUD SYNC DRY-RUN] Checked {summary['total_rows_replicated']} local rows ready for Cloud Replication.")

        summary["status"] = "completed"
        summary["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)

    except Exception as e:
        summary["status"] = "failed"
        summary["error"] = str(e)
        summary["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)
        print(f"[CLOUD SYNC ERROR] Synchronization failed: {e}")

    LAST_SYNC_STATUS = summary
    return summary


def get_cloud_sync_status() -> Dict[str, Any]:
    """Return the current cloud sync audit state."""
    return LAST_SYNC_STATUS


def start_background_cloud_replicator(interval_seconds: int = 30):
    """
    Start continuous background replication thread polling DuckDB and syncing to Supabase.
    """
    global _replicator_thread, _stop_replicator

    if _replicator_thread is not None and _replicator_thread.is_alive():
        print("[CLOUD REPLICATOR] Background replicator thread already running.")
        return

    _stop_replicator = False

    def _replication_loop():
        print(f"[CLOUD REPLICATOR] Background Cloud Sync daemon started (Interval: {interval_seconds}s)...")
        while not _stop_replicator:
            try:
                sync_duckdb_to_supabase()
            except Exception as e:
                print(f"[CLOUD REPLICATOR WARNING] Loop error: {e}")
            time.sleep(interval_seconds)

    _replicator_thread = threading.Thread(target=_replication_loop, daemon=True)
    _replicator_thread.start()


def stop_background_cloud_replicator():
    """Stop the background replication thread."""
    global _stop_replicator
    _stop_replicator = True
    print("[CLOUD REPLICATOR] Background cloud replicator daemon stopped.")
