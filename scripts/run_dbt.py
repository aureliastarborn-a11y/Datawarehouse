#!/usr/bin/env python3
"""
Enterprise Data Warehouse System - dbt Orchestrator
Executes dbt compilation, dbt run (staging, core marts, analytics marts),
and dbt test assertions against DuckDB database.
"""

import os
import sys
import subprocess

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run_cmd(cmd):
    print(f"\n🚀 Executing: {cmd}")
    res = subprocess.run(cmd, shell=True, cwd=BASE_DIR)
    if res.returncode != 0:
        print(f"❌ Command failed with return code {res.returncode}")
        return False
    return True

def main():
    print("=" * 80)
    print("  EXCUTING DBT DATA WAREHOUSE PIPELINE & TESTS")
    print("=" * 80)
    
    # 1. Run raw ingestion script first to ensure warehouse.duckdb has raw seeds
    raw_pipeline = os.path.join(BASE_DIR, "scripts", "run_pipeline.py")
    subprocess.run(f'python "{raw_pipeline}"', shell=True, cwd=BASE_DIR)

    # 2. dbt debug
    print("\n1. Verifying dbt connection setup...")
    run_cmd("dbt debug --profiles-dir .")

    # 3. dbt run
    print("\n2. Executing dbt transformations (staging, core, analytics)...")
    run_cmd("dbt run --profiles-dir .")

    # 4. dbt test
    print("\n3. Executing dbt test assertions (unique, not_null, referential integrity)...")
    run_cmd("dbt test --profiles-dir .")

    print("\n✨ dbt Pipeline Execution & Quality Testing Completed!")

if __name__ == "__main__":
    main()
