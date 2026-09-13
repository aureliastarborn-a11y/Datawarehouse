"""
Data Warehouse - dbt (data build tool) Test Assertion Runner & Docs Engine
Executes programmatic data assertions matching dbt schema constraints (uniqueness, referential integrity, non-null)
and generates dbt documentation static files for FastAPI mounting.
"""

import sys
import os
import time
import json
import subprocess
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.supabase_client import execute_sql, execute_sql_df
from pipeline.loaders import log_data_quality_issue

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET_DIR = os.path.join(PROJECT_ROOT, "target")
os.makedirs(TARGET_DIR, exist_ok=True)

# In-Memory dbt Test Log
DBT_TEST_LOG: List[Dict[str, Any]] = []


def run_dbt_tests(run_id: str = None) -> Dict[str, Any]:
    """
    Programmatically run data validation assertions matching dbt model constraints:
      1. Uniqueness check on primary keys (dim_customer, dim_product, fact_sales)
      2. Non-null constraints on surrogate keys & business keys
      3. Foreign key referential integrity checks (fact_sales -> dim_customer, dim_product, dim_date)
    """
    global DBT_TEST_LOG
    start_time = time.time()
    timestamp = datetime.now().isoformat()

    tests = [
        # Model 1: dim_customer assertions
        {
            "model": "dim_customer",
            "test_name": "unique_customer_key",
            "sql": "SELECT customer_key FROM dim_customer GROUP BY customer_key HAVING COUNT(*) > 1",
            "severity": "critical"
        },
        {
            "model": "dim_customer",
            "test_name": "not_null_customer_id",
            "sql": "SELECT customer_key FROM dim_customer WHERE customer_id IS NULL OR TRIM(customer_id) = ''",
            "severity": "critical"
        },

        # Model 2: dim_product assertions
        {
            "model": "dim_product",
            "test_name": "unique_product_key",
            "sql": "SELECT product_key FROM dim_product GROUP BY product_key HAVING COUNT(*) > 1",
            "severity": "critical"
        },
        {
            "model": "dim_product",
            "test_name": "not_null_product_id",
            "sql": "SELECT product_key FROM dim_product WHERE product_id IS NULL OR TRIM(product_id) = ''",
            "severity": "critical"
        },

        # Model 3: fact_sales assertions
        {
            "model": "fact_sales",
            "test_name": "unique_sales_fact_key",
            "sql": "SELECT sales_fact_key FROM fact_sales GROUP BY sales_fact_key HAVING COUNT(*) > 1",
            "severity": "critical"
        },
        {
            "model": "fact_sales",
            "test_name": "foreign_key_customer_exists",
            "sql": "SELECT sales_fact_key FROM fact_sales f WHERE NOT EXISTS (SELECT 1 FROM dim_customer c WHERE f.customer_key = c.customer_key)",
            "severity": "critical"
        },
        {
            "model": "fact_sales",
            "test_name": "foreign_key_product_exists",
            "sql": "SELECT sales_fact_key FROM fact_sales f WHERE NOT EXISTS (SELECT 1 FROM dim_product p WHERE f.product_key = p.product_key)",
            "severity": "critical"
        },
        {
            "model": "fact_sales",
            "test_name": "foreign_key_date_exists",
            "sql": "SELECT sales_fact_key FROM fact_sales f WHERE NOT EXISTS (SELECT 1 FROM dim_date d WHERE f.order_date_key = d.date_key)",
            "severity": "critical"
        },
        {
            "model": "fact_sales",
            "test_name": "positive_net_amount_constraint",
            "sql": "SELECT sales_fact_key FROM fact_sales WHERE net_amount < 0",
            "severity": "warning"
        }
    ]

    passed_count = 0
    failed_count = 0
    test_results = []

    for t in tests:
        try:
            df = execute_sql_df(t["sql"])
            fail_records = len(df)

            if fail_records == 0:
                passed_count += 1
                test_results.append({
                    "model": t["model"],
                    "test_name": t["test_name"],
                    "status": "PASSED",
                    "failures": 0,
                    "severity": t["severity"]
                })
            else:
                failed_count += 1
                test_results.append({
                    "model": t["model"],
                    "test_name": t["test_name"],
                    "status": "FAILED",
                    "failures": fail_records,
                    "severity": t["severity"]
                })
                # Log issue into quality audit log
                log_data_quality_issue(
                    pipeline_run_id=run_id or "dbt_test_run",
                    table_name=t["model"],
                    check_name=f"dbt_{t['test_name']}",
                    check_type="dbt_schema_test",
                    records_checked=100,
                    records_failed=fail_records,
                    severity=t["severity"],
                    details=f"dbt test {t['test_name']} failed on model {t['model']} ({fail_records} violations)"
                )
        except Exception as err:
            test_results.append({
                "model": t["model"],
                "test_name": t["test_name"],
                "status": "ERROR",
                "failures": -1,
                "error": str(err)
            })

    summary = {
        "executed_at": timestamp,
        "total_tests": len(tests),
        "passed": passed_count,
        "failed": failed_count,
        "execution_time_ms": round((time.time() - start_time) * 1000, 2),
        "test_results": test_results
    }

    DBT_TEST_LOG.insert(0, summary)
    if len(DBT_TEST_LOG) > 50:
        DBT_TEST_LOG.pop()

    print(f"[DBT TEST SUITE COMPLETE] {passed_count}/{len(tests)} tests PASSED in {summary['execution_time_ms']}ms")
    return summary


def generate_dbt_docs_site() -> Dict[str, Any]:
    """
    Generate dbt static documentation HTML files in target/ directory.
    Serves interactive Star Schema Lineage DAG graph for FastAPI mounting.
    """
    os.makedirs(TARGET_DIR, exist_ok=True)
    index_path = os.path.join(TARGET_DIR, "index.html")

    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>dbt Documentation — Enterprise Data Warehouse</title>
    <style>
        body { font-family: 'Plus Jakarta Sans', sans-serif; background-color: #F4F0EA; color: #1D1A17; margin: 0; padding: 24px; }
        .header { background: #CEC4B5; padding: 20px; border-radius: 8px; margin-bottom: 24px; border: 1px solid #C9BEAD; }
        h1 { font-family: Georgia, serif; margin: 0 0 8px 0; color: #1D1A17; }
        .card { background: #FFFFFF; padding: 18px; border-radius: 6px; margin-bottom: 16px; border: 1px solid #C9BEAD; box-shadow: 0 2px 4px rgba(0,0,0,0.03); }
        .badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; text-transform: uppercase; background: #28A745; color: white; }
        .badge-dim { background: #17A2B8; color: white; }
        .badge-fact { background: #9E5A3C; color: white; }
        table { width: 100%; border-collapse: collapse; margin-top: 12px; }
        th, td { border: 1px solid #E4DCD1; padding: 8px 12px; text-align: left; font-size: 13px; }
        th { background: #F4F0EA; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏛️ dbt Interactive Lineage & Model Documentation</h1>
        <p>Kimball Star Schema Data Warehouse • DuckDB / Supabase PostgreSQL • Real-Time Lineage DAG</p>
    </div>

    <div class="card">
        <h2>📊 Model DAG Lineage</h2>
        <pre style="background: #F4F0EA; padding: 12px; border-radius: 4px; font-size: 13px; color: #1D1A17;">
stg_customers ─────────► dim_customer (SCD Type 2) ──┐
                                                    │
stg_products  ─────────► dim_product  (SCD Type 1) ──┼──► fact_sales (Transactional Grain)
                                                    │
generate_series ───────► dim_date     (Conformed)  ──┘
        </pre>
    </div>

    <div class="card">
        <h2><span class="badge badge-dim">DIMENSION</span> dim_customer (SCD Type 2)</h2>
        <p>Tracks customer demographic changes and tier upgrades over time.</p>
        <table>
            <tr><th>Column</th><th>Type</th><th>Constraint</th><th>Description</th></tr>
            <tr><td>customer_key</td><td>BIGINT</td><td>PRIMARY KEY</td><td>Surrogate Key</td></tr>
            <tr><td>customer_id</td><td>VARCHAR</td><td>NOT NULL</td><td>Natural Business Key</td></tr>
            <tr><td>customer_tier</td><td>VARCHAR</td><td>NOT NULL</td><td>Bronze, Silver, Gold, Platinum VIP</td></tr>
            <tr><td>effective_start_date</td><td>TIMESTAMP</td><td>NOT NULL</td><td>Version active start timestamp</td></tr>
            <tr><td>effective_end_date</td><td>TIMESTAMP</td><td>NULLABLE</td><td>Version end timestamp (NULL for current)</td></tr>
            <tr><td>is_current</td><td>BOOLEAN</td><td>NOT NULL</td><td>TRUE if current active profile</td></tr>
        </table>
    </div>

    <div class="card">
        <h2><span class="badge badge-fact">FACT</span> fact_sales</h2>
        <p>Transactional order item sales fact table enriched with point-in-time surrogate keys.</p>
        <table>
            <tr><th>Column</th><th>Type</th><th>Constraint</th><th>Description</th></tr>
            <tr><td>sales_fact_key</td><td>BIGINT</td><td>PRIMARY KEY</td><td>Surrogate Fact Key</td></tr>
            <tr><td>customer_key</td><td>BIGINT</td><td>FOREIGN KEY</td><td>FK to dim_customer (Point-in-time match)</td></tr>
            <tr><td>product_key</td><td>BIGINT</td><td>FOREIGN KEY</td><td>FK to dim_product</td></tr>
            <tr><td>order_date_key</td><td>INTEGER</td><td>FOREIGN KEY</td><td>FK to dim_date (YYYYMMDD)</td></tr>
            <tr><td>net_amount</td><td>DECIMAL(12,2)</td><td>NOT NULL</td><td>Gross - Discount + Tax</td></tr>
            <tr><td>profit_amount</td><td>DECIMAL(12,2)</td><td>NOT NULL</td><td>Net Amount - Cost Amount</td></tr>
            <tr><td>profit_margin_pct</td><td>DECIMAL(5,2)</td><td>NOT NULL</td><td>(Profit / Net Amount) * 100</td></tr>
        </table>
    </div>
</body>
</html>
"""

    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"[DBT DOCS GENERATED] Static site compiled at {index_path}")
    return {"status": "completed", "docs_url": "/dbt-docs", "index_path": index_path}


def get_latest_dbt_test_results() -> Dict[str, Any]:
    """Return the most recent dbt test execution log."""
    return DBT_TEST_LOG[0] if DBT_TEST_LOG else {"status": "no_tests_executed_yet"}
