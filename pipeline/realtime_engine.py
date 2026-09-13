"""
Data Warehouse - Real-Time Automated Event-Driven ELT Engine
Handles real-time ingestion, data validation gates, SCD Type 2 dimension versioning,
transactional fact loading, and instant database storage.
"""

import sys
import os
import time
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.supabase_client import execute_sql, get_duckdb_connection, get_mode, init_schema
from pipeline.loaders import create_pipeline_run, update_pipeline_run, log_data_quality_issue


class RealTimeELTEngine:
    """
    Real-Time ELT Processing Engine for automated data warehouse updates.
    Processes micro-batches of records or raw CSV/JSON drops instantly into
    Kimball Star Schema dimensional tables and transactional fact tables.
    """

    def __init__(self):
        self.mode = get_mode()
        init_schema()

    def process_raw_dataframe(self, df: pd.DataFrame, source_type: str = "csv_upload") -> Dict[str, Any]:
        """
        Process a raw pandas DataFrame containing order, customer, or product records.
        Automatically categorizes columns, ingests into staging, runs quality gates,
        updates SCD2 customer/product dimensions, and loads fact_sales.
        """
        start_time = time.time()
        run_id = create_pipeline_run(f"realtime_{source_type}", metadata={"rows_submitted": len(df)})

        summary = {
            "run_id": run_id,
            "status": "running",
            "source_type": source_type,
            "submitted_rows": len(df),
            "ingested": {"customers": 0, "products": 0, "orders": 0},
            "transformed": {"customers_scd2": 0, "products": 0, "facts_loaded": 0},
            "rejected_rows": 0,
            "quality_issues": [],
            "execution_time_ms": 0,
        }

        try:
            # Standardize column names (lowercase & stripped)
            df.columns = [str(c).strip().lower() for c in df.columns]

            # 1. Classify and Route Columns
            cust_df = pd.DataFrame()
            prod_df = pd.DataFrame()
            order_df = pd.DataFrame()

            # Customer columns check
            if "customer_id" in df.columns or "raw_customer_id" in df.columns:
                cust_cols = [c for c in df.columns if c in [
                    "customer_id", "raw_customer_id", "first_name", "last_name",
                    "email", "customer_tier", "city", "state", "country", "updated_at"
                ]]
                cust_df = df[cust_cols].drop_duplicates().copy()

            # Product columns check
            if "product_id" in df.columns or "raw_product_id" in df.columns:
                prod_cols = [c for c in df.columns if c in [
                    "product_id", "raw_product_id", "product_name", "category",
                    "subcategory", "unit_price", "cost_price", "updated_at"
                ]]
                prod_df = df[prod_cols].drop_duplicates().copy()

            # Order columns check
            if "order_id" in df.columns and "quantity" in df.columns:
                order_cols = [c for c in df.columns if c in [
                    "order_id", "order_line_number", "customer_id", "product_id",
                    "order_timestamp", "quantity", "unit_price", "discount_amount", "tax_amount"
                ]]
                order_df = df[order_cols].copy()

            # 2. Ingest into Staging
            if not cust_df.empty:
                summary["ingested"]["customers"] = self._stage_customers(cust_df)
            if not prod_df.empty:
                summary["ingested"]["products"] = self._stage_products(prod_df)
            if not order_df.empty:
                summary["ingested"]["orders"] = self._stage_orders(order_df, run_id, summary)

            # 3. Execute Dimensional Transformations & Fact Load
            transformed_counts = self._execute_realtime_elt(run_id)
            summary["transformed"] = transformed_counts

            # 4. Trigger Real-Time ML Predictive Inference
            try:
                from ml.predictor import run_realtime_ml_inference
                summary["ml_insights"] = run_realtime_ml_inference(run_id)
            except Exception as ml_err:
                summary["ml_insights"] = {"status": "error", "error": str(ml_err)}

            # 5. Trigger Supabase Cloud Replication & Sync
            try:
                from pipeline.cloud_sync import sync_duckdb_to_supabase
                summary["cloud_sync"] = sync_duckdb_to_supabase()
            except Exception as sync_err:
                summary["cloud_sync"] = {"status": "error", "error": str(sync_err)}

            # 6. Finalize Pipeline Run Status
            summary["status"] = "completed"
            summary["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)

            update_pipeline_run(
                run_id=run_id,
                status="completed",
                records_extracted=len(df),
                records_loaded=summary["ingested"]["orders"] + summary["ingested"]["customers"] + summary["ingested"]["products"],
                records_transformed=transformed_counts.get("facts_loaded", 0),
                records_rejected=summary["rejected_rows"],
            )

        except Exception as e:
            summary["status"] = "failed"
            summary["error"] = str(e)
            summary["execution_time_ms"] = round((time.time() - start_time) * 1000, 2)
            update_pipeline_run(run_id=run_id, status="failed", error_message=str(e))

        return summary

    def _stage_customers(self, df: pd.DataFrame) -> int:
        """Stage raw customer records into stg_customers."""
        cust_id_col = "raw_customer_id" if "raw_customer_id" in df.columns else "customer_id"
        count = 0
        for _, row in df.iterrows():
            cid = str(row.get(cust_id_col, "")).strip()
            if not cid or cid.lower() in ("none", "nan", "null"):
                continue

            updated_at = row.get("updated_at")
            if pd.isna(updated_at) or not updated_at:
                updated_at = datetime.now().isoformat()
            else:
                updated_at = str(updated_at)

            execute_sql("""
                INSERT INTO stg_customers
                    (raw_customer_id, first_name, last_name, email, customer_tier, city, state, country, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS TIMESTAMP))
            """, (
                cid,
                str(row.get("first_name", "")).strip(),
                str(row.get("last_name", "")).strip(),
                str(row.get("email", "")).strip().lower(),
                str(row.get("customer_tier", "Standard")).strip(),
                str(row.get("city", "")).strip(),
                str(row.get("state", "")).strip(),
                str(row.get("country", "")).strip(),
                updated_at
            ))
            count += 1
        return count

    def _stage_products(self, df: pd.DataFrame) -> int:
        """Stage raw product records into stg_products."""
        prod_id_col = "raw_product_id" if "raw_product_id" in df.columns else "product_id"
        count = 0
        for _, row in df.iterrows():
            pid = str(row.get(prod_id_col, "")).strip()
            if not pid or pid.lower() in ("none", "nan", "null"):
                continue

            updated_at = row.get("updated_at")
            if pd.isna(updated_at) or not updated_at:
                updated_at = datetime.now().isoformat()
            else:
                updated_at = str(updated_at)

            unit_price = float(row.get("unit_price", 0.0))
            cost_price = float(row.get("cost_price", 0.0))

            execute_sql("""
                INSERT INTO stg_products
                    (raw_product_id, product_name, category, subcategory, unit_price, cost_price, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, CAST(%s AS TIMESTAMP))
            """, (
                pid,
                str(row.get("product_name", pid)).strip(),
                str(row.get("category", "General")).strip(),
                str(row.get("subcategory", "General")).strip(),
                unit_price,
                cost_price,
                updated_at
            ))
            count += 1
        return count

    def _stage_orders(self, df: pd.DataFrame, run_id: str, summary: dict) -> int:
        """Stage raw orders and execute Data Quality Gate checks."""
        count = 0
        rejected = 0

        for idx, row in df.iterrows():
            oid = str(row.get("order_id", "")).strip()
            cid = str(row.get("customer_id", "")).strip()
            pid = str(row.get("product_id", "")).strip()
            qty = int(row.get("quantity", 0))

            # Quality Check 1: Corrupt Value Filter Gate
            if not oid or not cid or not pid or qty <= 0:
                rejected += 1
                log_data_quality_issue(
                    pipeline_run_id=run_id,
                    table_name="stg_orders",
                    check_name="Corrupt Value Filter Gate",
                    check_type="Value Range Constraint",
                    records_checked=1,
                    records_failed=1,
                    severity="critical",
                    details=f"Row {idx}: Invalid order_id='{oid}', cid='{cid}', pid='{pid}', qty={qty}"
                )
                continue

            order_timestamp = row.get("order_timestamp")
            if pd.isna(order_timestamp) or not order_timestamp:
                order_timestamp = datetime.now().isoformat()
            else:
                order_timestamp = str(order_timestamp)

            line_no = int(row.get("order_line_number", 1))
            unit_price = float(row.get("unit_price", 0.0))
            discount = float(row.get("discount_amount", 0.0))
            tax = float(row.get("tax_amount", 0.0))

            execute_sql("""
                INSERT INTO stg_orders
                    (order_id, order_line_number, customer_id, product_id, order_timestamp, quantity, unit_price, discount_amount, tax_amount)
                VALUES (%s, %s, %s, %s, CAST(%s AS TIMESTAMP), %s, %s, %s, %s)
            """, (oid, line_no, cid, pid, order_timestamp, qty, unit_price, discount, tax))
            count += 1

        summary["rejected_rows"] += rejected
        return count

    def _execute_realtime_elt(self, run_id: str) -> Dict[str, int]:
        """
        Executes immediate Kimball Star Schema transformation queries:
        1. Date Dimension Populate
        2. Product Dimension Update (SCD Type 1)
        3. Customer Dimension Update (SCD Type 2 Merge & Expire)
        4. Fact Sales Insertion
        """
        # 1. Date Dimension Check
        mode = get_mode()
        if mode == "supabase":
            execute_sql("""
                INSERT INTO dim_date (
                    date_key, full_date, day_name, day_of_week, day_of_month,
                    day_of_year, week_of_year, month, month_name, quarter, year,
                    is_weekend, is_month_start, is_month_end, fiscal_quarter, fiscal_year
                )
                SELECT
                    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER) AS date_key,
                    CAST(d AS DATE) AS full_date,
                    TRIM(TO_CHAR(d, 'Day')) AS day_name,
                    EXTRACT(DOW FROM d) + 1 AS day_of_week,
                    EXTRACT(DAY FROM d) AS day_of_month,
                    EXTRACT(DOY FROM d) AS day_of_year,
                    EXTRACT(WEEK FROM d) AS week_of_year,
                    EXTRACT(MONTH FROM d) AS month,
                    TRIM(TO_CHAR(d, 'Month')) AS month_name,
                    EXTRACT(QUARTER FROM d) AS quarter,
                    EXTRACT(YEAR FROM d) AS year,
                    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
                    CASE WHEN EXTRACT(DAY FROM d) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
                    CASE WHEN (d + INTERVAL '1 day')::DATE = DATE_TRUNC('month', d + INTERVAL '1 month')::DATE THEN TRUE ELSE FALSE END AS is_month_end,
                    EXTRACT(QUARTER FROM d) AS fiscal_quarter,
                    EXTRACT(YEAR FROM d) AS fiscal_year
                FROM GENERATE_SERIES(DATE '2024-01-01', DATE '2026-12-31', INTERVAL '1 day') AS d
                ON CONFLICT (date_key) DO NOTHING;
            """)
        else:
            execute_sql("""
                INSERT INTO dim_date (
                    date_key, full_date, day_name, day_of_week, day_of_month,
                    day_of_year, week_of_year, month, month_name, quarter, year,
                    is_weekend, is_month_start, is_month_end, fiscal_quarter, fiscal_year
                )
                SELECT
                    CAST(STRFTIME(d, '%Y%m%d') AS INTEGER) AS date_key,
                    CAST(d AS DATE) AS full_date,
                    STRFTIME(d, '%A') AS day_name,
                    EXTRACT(DOW FROM d) + 1 AS day_of_week,
                    EXTRACT(DAY FROM d) AS day_of_month,
                    EXTRACT(DOY FROM d) AS day_of_year,
                    EXTRACT(WEEK FROM d) AS week_of_year,
                    EXTRACT(MONTH FROM d) AS month,
                    STRFTIME(d, '%B') AS month_name,
                    EXTRACT(QUARTER FROM d) AS quarter,
                    EXTRACT(YEAR FROM d) AS year,
                    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
                    CASE WHEN EXTRACT(DAY FROM d) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
                    CASE WHEN d = LAST_DAY(d) THEN TRUE ELSE FALSE END AS is_month_end,
                    EXTRACT(QUARTER FROM d) AS fiscal_quarter,
                    EXTRACT(YEAR FROM d) AS fiscal_year
                FROM GENERATE_SERIES(DATE '2024-01-01', DATE '2026-12-31', INTERVAL '1 day') AS s(d)
                ON CONFLICT (date_key) DO NOTHING;
            """)

        # 2. Product Dimension (SCD Type 1)
        execute_sql("""
            INSERT INTO dim_product (product_key, product_id, product_name, category, subcategory, unit_price, cost_price, is_active, updated_at)
            SELECT
                COALESCE((SELECT MAX(product_key) FROM dim_product), 0) + ROW_NUMBER() OVER () AS product_key,
                stg.raw_product_id AS product_id,
                stg.product_name,
                stg.category,
                stg.subcategory,
                stg.unit_price,
                stg.cost_price,
                TRUE AS is_active,
                stg.updated_at
            FROM (
                SELECT raw_product_id, product_name, category, subcategory, unit_price, cost_price, updated_at,
                       ROW_NUMBER() OVER (PARTITION BY raw_product_id ORDER BY updated_at DESC) AS rn
                FROM stg_products
            ) stg
            WHERE stg.rn = 1
              AND stg.raw_product_id NOT IN (SELECT product_id FROM dim_product);
        """)

        # Update existing product prices (SCD1)
        execute_sql("""
            UPDATE dim_product
            SET product_name = stg.product_name,
                category = stg.category,
                subcategory = stg.subcategory,
                unit_price = stg.unit_price,
                cost_price = stg.cost_price,
                updated_at = stg.updated_at
            FROM (
                SELECT raw_product_id, product_name, category, subcategory, unit_price, cost_price, updated_at,
                       ROW_NUMBER() OVER (PARTITION BY raw_product_id ORDER BY updated_at DESC) AS rn
                FROM stg_products
            ) stg
            WHERE dim_product.product_id = stg.raw_product_id
              AND stg.rn = 1
              AND (dim_product.unit_price <> stg.unit_price OR dim_product.cost_price <> stg.cost_price OR dim_product.product_name <> stg.product_name);
        """)

        # 3. Customer Dimension (SCD Type 2)
        # Step A: Identify changed customers and expire active records
        execute_sql("""
            UPDATE dim_customer
            SET effective_end_date = stg.updated_at,
                is_current = FALSE
            FROM (
                SELECT raw_customer_id, first_name, last_name, email, customer_tier, city, state, country, updated_at,
                       ROW_NUMBER() OVER (PARTITION BY raw_customer_id ORDER BY updated_at DESC) AS rn
                FROM stg_customers
            ) stg
            WHERE dim_customer.customer_id = stg.raw_customer_id
              AND stg.rn = 1
              AND dim_customer.is_current = TRUE
              AND (
                  dim_customer.first_name <> stg.first_name OR
                  dim_customer.last_name <> stg.last_name OR
                  dim_customer.email <> stg.email OR
                  dim_customer.customer_tier <> stg.customer_tier OR
                  COALESCE(dim_customer.city, '') <> COALESCE(stg.city, '') OR
                  COALESCE(dim_customer.state, '') <> COALESCE(stg.state, '') OR
                  COALESCE(dim_customer.country, '') <> COALESCE(stg.country, '')
              );
        """)

        # Step B: Insert new versions for expired records AND brand-new customers
        execute_sql("""
            INSERT INTO dim_customer (customer_key, customer_id, first_name, last_name, email, customer_tier, city, state, country, effective_start_date, effective_end_date, is_current)
            SELECT
                COALESCE((SELECT MAX(customer_key) FROM dim_customer), 0) + ROW_NUMBER() OVER () AS customer_key,
                stg.raw_customer_id AS customer_id,
                stg.first_name,
                stg.last_name,
                stg.email,
                stg.customer_tier,
                stg.city,
                stg.state,
                stg.country,
                stg.updated_at AS effective_start_date,
                CAST(NULL AS TIMESTAMP) AS effective_end_date,
                TRUE AS is_current
            FROM (
                SELECT raw_customer_id, first_name, last_name, email, customer_tier, city, state, country, updated_at,
                       ROW_NUMBER() OVER (PARTITION BY raw_customer_id ORDER BY updated_at DESC) AS rn
                FROM stg_customers
            ) stg
            LEFT JOIN dim_customer dc ON stg.raw_customer_id = dc.customer_id AND dc.is_current = TRUE
            WHERE stg.rn = 1 AND dc.customer_key IS NULL;
        """)

        # 4. Insert Fact Sales with SCD2 Point-in-Time lookup
        order_date_key_sql = "CAST(TO_CHAR(o.order_timestamp, 'YYYYMMDD') AS INTEGER)" if mode == "supabase" else "CAST(STRFTIME(o.order_timestamp, '%Y%m%d') AS INTEGER)"
        execute_sql(f"""
            INSERT INTO fact_sales (
                sales_fact_key, order_id, order_line_number, customer_key, product_key,
                order_date_key, quantity, unit_price, gross_amount, discount_amount,
                tax_amount, net_amount, cost_amount, profit_amount, profit_margin_pct
            )
            WITH customer_matches AS (
                SELECT
                    o.order_id,
                    o.order_line_number,
                    c.customer_key,
                    ROW_NUMBER() OVER (
                        PARTITION BY o.order_id, o.order_line_number
                        ORDER BY
                            CASE
                                WHEN o.order_timestamp >= c.effective_start_date AND (c.effective_end_date IS NULL OR o.order_timestamp < c.effective_end_date) THEN 1
                                WHEN c.is_current = TRUE THEN 2
                                ELSE 3
                            END,
                            c.effective_start_date DESC
                    ) AS rn
                FROM stg_orders o
                JOIN dim_customer c ON o.customer_id = c.customer_id
            )
            SELECT
                COALESCE((SELECT MAX(sales_fact_key) FROM fact_sales), 0) + ROW_NUMBER() OVER () AS sales_fact_key,
                o.order_id,
                o.order_line_number,
                cm.customer_key,
                p.product_key,
                {order_date_key_sql} AS order_date_key,
                o.quantity,
                o.unit_price,
                (o.quantity * o.unit_price) AS gross_amount,
                o.discount_amount,
                o.tax_amount,
                ((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount) AS net_amount,
                (o.quantity * p.cost_price) AS cost_amount,
                (((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount) - (o.quantity * p.cost_price)) AS profit_amount,
                CASE
                    WHEN ((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount) > 0
                    THEN ROUND(((((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount) - (o.quantity * p.cost_price)) / ((o.quantity * o.unit_price) - o.discount_amount + o.tax_amount)) * 100, 2)
                    ELSE 0.00
                END AS profit_margin_pct
            FROM stg_orders o
            JOIN dim_product p ON o.product_id = p.product_id
            JOIN customer_matches cm ON o.order_id = cm.order_id AND o.order_line_number = cm.order_line_number AND cm.rn = 1
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_sales fs
                WHERE fs.order_id = o.order_id AND fs.order_line_number = o.order_line_number
            );
        """)

        # 5. High-Value VIP Transaction Alert Check (>= $1,000)
        try:
            vip_orders = execute_sql("""
                SELECT
                    f.order_id,
                    c.first_name || ' ' || c.last_name AS customer_name,
                    c.customer_tier,
                    f.net_amount
                FROM fact_sales f
                JOIN dim_customer c ON f.customer_key = c.customer_key
                WHERE f.net_amount >= 1000.00
                ORDER BY f.created_at DESC
                LIMIT 3
            """, fetch=True)

            if vip_orders:
                from pipeline.notifications import send_high_value_transaction_alert
                for vo in vip_orders:
                    send_high_value_transaction_alert(
                        order_id=vo["order_id"],
                        customer_name=vo["customer_name"],
                        net_amount=float(vo["net_amount"]),
                        customer_tier=vo["customer_tier"]
                    )
        except Exception as notify_err:
            print(f"[NOTIFICATION WARNING] VIP transaction alert trigger failed: {notify_err}")

        # Fetch transformed counts
        cust_cnt = execute_sql("SELECT COUNT(*) AS cnt FROM dim_customer", fetch=True)[0]["cnt"]
        prod_cnt = execute_sql("SELECT COUNT(*) AS cnt FROM dim_product", fetch=True)[0]["cnt"]
        fact_cnt = execute_sql("SELECT COUNT(*) AS cnt FROM fact_sales", fetch=True)[0]["cnt"]

        return {
            "total_customers_in_dw": cust_cnt,
            "total_products_in_dw": prod_cnt,
            "facts_loaded": fact_cnt,
        }


# Singleton engine instance
realtime_engine = RealTimeELTEngine()
