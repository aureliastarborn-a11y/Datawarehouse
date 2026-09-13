"""
Data Warehouse - One-Click Supabase PostgreSQL Setup & Migration Tool
Tests Supabase cloud connection, executes PostgreSQL table DDL schema,
and performs initial replication of local DuckDB tables to Supabase Cloud.
"""

import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from config.supabase_client import (
    is_supabase_configured,
    _init_supabase_schema,
    get_pg_connection
)
from pipeline.cloud_sync import sync_duckdb_to_supabase


def main():
    print("======================================================================")
    print("     SUPABASE POSTGRESQL CLOUD SETUP & REPLICATION TOOL")
    print("======================================================================")

    if not is_supabase_configured():
        print("\n[CONFIG NOTICE] Supabase credentials are currently in DEMO / LOCAL mode.")
        print("To connect your Supabase PostgreSQL Database for cloud storage:")
        print("  1. Open your project '.env' file")
        print("  2. Replace SUPABASE_URL, SUPABASE_KEY, and SUPABASE_DB_URL with your Supabase credentials:")
        print("     SUPABASE_URL=https://your-project-id.supabase.co")
        print("     SUPABASE_KEY=your-supabase-service-role-key")
        print("     SUPABASE_DB_URL=postgresql://postgres.your-project-id:password@aws-0-region.pooler.supabase.com:6543/postgres")
        print("  3. Re-run 'python scripts/setup_supabase.py'\n")
        return

    print("\n--- 1. Testing Connection to Supabase PostgreSQL Cloud ---")
    conn = get_pg_connection()
    if conn is None:
        print("[ERROR] Could not establish connection to Supabase PostgreSQL.")
        return
    print("[SUCCESS] Successfully connected to Supabase PostgreSQL Cloud!")
    conn.close()

    print("\n--- 2. Initializing Supabase PostgreSQL Database DDL Schema ---")
    schema_ok = _init_supabase_schema()
    if not schema_ok:
        print("[ERROR] Failed to execute Supabase PostgreSQL DDL schema.")
        return
    print("[SUCCESS] All staging, dimensional (SCD2), and fact tables created on Supabase PostgreSQL!")

    print("\n--- 3. Replicating Local Data to Supabase PostgreSQL Cloud ---")
    sync_res = sync_duckdb_to_supabase()
    print(f"[SUCCESS] Cloud Replication Complete: {sync_res.get('total_rows_replicated')} rows replicated ({sync_res.get('execution_time_ms')}ms)")

    print("\n======================================================================")
    print("  SUPABASE CLOUD WAREHOUSE IS FULLY CONNECTED & IN SYNC!")
    print("======================================================================")


if __name__ == "__main__":
    main()
