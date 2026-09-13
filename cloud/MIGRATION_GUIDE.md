# Cloud Data Warehouse Migration & Deployment Playbook

This playbook details the step-by-step enterprise deployment strategy for migrating the local DuckDB Data Warehouse to **Snowflake Data Cloud** or **Google BigQuery**.

---

## ❄️ 1. Snowflake Migration Deployment Steps

### Step 1: AWS S3 Storage Integration & IAM Configuration
Create an IAM Policy & Role in AWS allowing Snowflake to access raw data buckets:
```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_dw_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeS3WarehouseRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-company-data-warehouse-bucket/raw/');
```

### Step 2: Execute DDL & Setup Staging Pipeline
Execute [cloud/snowflake/01_snowflake_ddl.sql](file:///c:/Users/niraa/OneDrive/Desktop/Data%20Warehouse/cloud/snowflake/01_snowflake_ddl.sql) to create virtual warehouses, schemas, file formats, external stages, clustering keys, and COPY INTO triggers.

### Step 3: dbt Cloud / CLI Execution against Snowflake
Set environment variables:
```bash
export DBT_ENV_SECRET_SNOWFLAKE_PASSWORD="<YourSecurePassword>"
```
Run dbt models and data assertions against Snowflake:
```bash
dbt run --profiles-dir cloud --profile ecommerce_dw_cloud --target snowflake_prod
dbt test --profiles-dir cloud --profile ecommerce_dw_cloud --target snowflake_prod
```

---

## 🟡 2. Google BigQuery Migration Deployment Steps

### Step 1: Google Cloud Storage (GCS) Bucket Setup
Upload raw files to GCS:
```bash
gsutil cp data/raw/*.csv gs://my-company-data-warehouse/raw/
```

### Step 2: Service Account Permissions
Grant the GCP Service Account the following IAM roles:
- `BigQuery Admin` (`roles/bigquery.admin`)
- `Storage Object Viewer` (`roles/storage.objectViewer`)

### Step 3: Execute BigQuery DDL
Execute [cloud/bigquery/01_bigquery_ddl.sql](file:///c:/Users/niraa/OneDrive/Desktop/Data%20Warehouse/cloud/bigquery/01_bigquery_ddl.sql) to provision external GCS tables, dataset schemas, partitioning on `order_date_key`, and clustering on `customer_key` & `product_key`.

### Step 4: Run dbt against BigQuery Target
```bash
dbt run --profiles-dir cloud --profile ecommerce_dw_cloud --target bigquery_prod
dbt test --profiles-dir cloud --profile ecommerce_dw_cloud --target bigquery_prod
```

---

## ⚡ Production Best Practices & Cost Optimization

1. **Snowflake Auto-Suspend**: Warehouse configured with `AUTO_SUSPEND = 60` to pause compute costs after 1 minute of inactivity.
2. **Micro-partition Clustering**: `fact_sales` clustered by `ORDER_DATE_KEY` in Snowflake and `PARTITION BY RANGE_BUCKET` in BigQuery to prevent expensive full table scans.
3. **SCD Type 2 Point-in-Time Joins**: Ensure dbt models maintain non-overlapping `effective_start_date` and `effective_end_date` timestamps during incremental cloud loads.
