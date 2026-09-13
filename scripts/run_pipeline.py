#!/usr/bin/env python3
"""
Enterprise Data Warehouse System - Automated Pipeline Runner & Orchestrator
Executes end-to-end ELT, Data Quality Gate Validation, SCD Type 2 Merges,
and Business Analytical SQL Reports.
"""

import os
import sys
import duckdb
from datetime import datetime

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
SQL_DIR = os.path.join(BASE_DIR, "sql")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")

def print_header(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def print_table(cursor, title="Query Results"):
    print(f"\n[REPORT] --- {title} ---")
    rows = cursor.fetchall()
    if not rows:
        print("(No records returned)")
        return
    colnames = [desc[0] for desc in cursor.description]
    
    # Calculate column widths
    col_widths = [len(cn) for cn in colnames]
    for row in rows:
        for idx, val in enumerate(row):
            col_widths[idx] = max(col_widths[idx], len(str(val) if val is not None else "NULL"))
            
    header = " | ".join(cn.ljust(col_widths[idx]) for idx, cn in enumerate(colnames))
    divider = "-+-".join("-" * col_widths[idx] for idx in range(len(colnames)))
    print(header)
    print(divider)
    for row in rows[:15]: # Print first 15 rows for clear console preview
        line = " | ".join((str(val) if val is not None else "NULL").ljust(col_widths[idx]) for idx, val in enumerate(row))
        print(line)
    if len(rows) > 15:
        print(f"... ({len(rows) - 15} more rows omitted for console brevity)")

def run_pipeline():
    print_header("1. INITIALIZING MOCK DATA GENERATION")
    generate_script = os.path.join(BASE_DIR, "scripts", "generate_mock_data.py")
    os.system(f'python "{generate_script}"')

    print_header("2. CONNECTING TO DUCKDB DATA WAREHOUSE")
    # Remove existing DB if present for fresh execution
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        
    con = duckdb.connect(DB_PATH)
    print(f"Connected to database at: {DB_PATH}")

    print_header("3. EXECUTING SCHEMA DDL (STAGING & STAR SCHEMA)")
    schema_sql_path = os.path.join(SQL_DIR, "schema.sql")
    with open(schema_sql_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    con.execute(schema_sql)
    print("[OK] Staging and Data Warehouse DDL tables created successfully.")

    print_header("4. INGESTING RAW CSV DATA INTO STAGING TABLES")
    prod_csv = os.path.join(DATA_DIR, "raw_products.csv")
    cust_b1_csv = os.path.join(DATA_DIR, "raw_customers_batch1.csv")
    cust_b2_csv = os.path.join(DATA_DIR, "raw_customers_batch2.csv")
    orders_csv = os.path.join(DATA_DIR, "raw_orders.csv")

    con.execute(f"COPY stg_products FROM '{prod_csv}' (HEADER, DELIMITER ',');")
    con.execute(f"COPY stg_customers FROM '{cust_b1_csv}' (HEADER, DELIMITER ',');")
    con.execute(f"COPY stg_orders FROM '{orders_csv}' (HEADER, DELIMITER ',');")
    print("[OK] Ingested raw datasets into stg_products, stg_customers, stg_orders.")

    print_header("5. EXECUTING INITIAL TRANSFORMATIONS & DIMENSIONAL LOADING")
    transform_sql_path = os.path.join(SQL_DIR, "transformations.sql")
    with open(transform_sql_path, "r", encoding="utf-8") as f:
        transform_sql = f.read()
        
    con.execute(transform_sql)
    print("[OK] Executed Dim_Date, Dim_Product, Dim_Customer (Batch 1), and Fact_Sales initial pipeline.")

    print_header("6. DATA QUALITY VALIDATION GATES")
    # Gate 1: Check for NULL Surrogate Keys in Dimensions
    null_cust_keys = con.execute("SELECT COUNT(*) FROM dim_customer WHERE customer_key IS NULL").fetchone()[0]
    null_prod_keys = con.execute("SELECT COUNT(*) FROM dim_product WHERE product_key IS NULL").fetchone()[0]
    print(f"[CHECK] Quality Gate 1 - Null Surrogate Keys: Customer={null_cust_keys}, Product={null_prod_keys}")

    # Gate 2: Verify Referential Integrity in Fact Table
    orphan_facts = con.execute("""
        SELECT COUNT(*) 
        FROM fact_sales f 
        LEFT JOIN dim_customer c ON f.customer_key = c.customer_key 
        WHERE c.customer_key IS NULL
    """).fetchone()[0]
    print(f"[CHECK] Quality Gate 2 - Fact Customer Orphan Records: {orphan_facts}")

    # Gate 3: Corrupt Data Filtering Verification
    invalid_qty_facts = con.execute("SELECT COUNT(*) FROM fact_sales WHERE quantity <= 0").fetchone()[0]
    print(f"[CHECK] Quality Gate 3 - Invalid Quantity Filter Verification (quantity <= 0): {invalid_qty_facts}")

    print_header("7. TESTING SCD TYPE 2 CUSTOMER UPDATES (BATCH 2 INGESTION)")
    print("Ingesting Batch 2 customer updates containing tier upgrades & location changes...")
    con.execute(f"COPY stg_customers FROM '{cust_b2_csv}' (HEADER, DELIMITER ',');")
    
    # Re-trigger transformation script to process SCD Type 2 merges
    con.execute(transform_sql)
    print("[OK] SCD Type 2 Merge executed successfully.")

    # Inspect SCD Type 2 Customer Records
    scd2_results = con.execute("""
        SELECT 
            customer_id, 
            customer_tier, 
            city, 
            effective_start_date, 
            effective_end_date, 
            is_current 
        FROM dim_customer 
        WHERE customer_id IN (
            SELECT customer_id 
            FROM dim_customer 
            GROUP BY customer_id 
            HAVING COUNT(*) > 1
        )
        ORDER BY customer_id, effective_start_date
    """)
    print_table(scd2_results, "SCD Type 2 Customer History Audit (Multiple Versions)")

    print_header("8. EXECUTING BUSINESS-CRITICAL ANALYTICAL SQL QUERIES")
    analytics_sql_path = os.path.join(SQL_DIR, "analytics_queries.sql")
    with open(analytics_sql_path, "r", encoding="utf-8") as f:
        analytics_sql = f.read()

    queries = analytics_sql.split(";")
    query_names = [
        "Query 1: Month-over-Month Revenue & Profit Margin Growth Trend",
        "Query 2: Customer Cohort Retention & Lifetime Value (LTV)",
        "Query 3: RFM Customer Segmentation & Profitability"
    ]
    
    q_idx = 0
    for query in queries:
        cleaned_q = query.strip()
        if cleaned_q and ("SELECT" in cleaned_q.upper() or "WITH" in cleaned_q.upper()):
            title = query_names[q_idx] if q_idx < len(query_names) else f"Analytical Query {q_idx+1}"
            res = con.execute(cleaned_q)
            print_table(res, title)
            q_idx += 1

    print_header("ENTERPRISE DATA WAREHOUSE PIPELINE EXECUTED SUCCESSFULLY")

if __name__ == "__main__":
    run_pipeline()
