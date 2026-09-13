#!/usr/bin/env python3
"""
Enterprise Data Warehouse System - BI Web Dashboard Runner
Launches Streamlit Web Dashboard connected directly to DuckDB OLAP Engine.
"""

import os
import sys
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_PATH = os.path.join(BASE_DIR, "dashboard", "app.py")

def main():
    print("=" * 80)
    print("  LAUNCHING STREAMLIT EXECUTIVE BI DASHBOARD")
    print("=" * 80)
    print(f"Connecting to DuckDB Data Warehouse at: warehouse.duckdb")
    print(f"Launching Streamlit Web App from: {APP_PATH}")
    
    cmd = f'streamlit run "{APP_PATH}"'
    subprocess.run(cmd, shell=True, cwd=BASE_DIR)

if __name__ == "__main__":
    main()
