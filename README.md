# Automated Real-Time Data Warehouse & Analytics Engine

An enterprise-grade, real-time event-driven **Data Warehouse System** built using **Kimball Dimensional Modeling Methodology**. Built with **DuckDB** for ultra-fast local OLAP query execution, **Supabase PostgreSQL** for cloud replication, **FastAPI** for real-time ingestion REST endpoints, **dbt** for model assertions & lineage documentation, **Machine Learning** for churn risk & revenue forecasting, **Slack/Discord Webhooks** for instant alerts, and **Docker Compose** for one-click microservice deployment.

---

## 🏛️ End-to-End System Architecture

```
                  ┌─────────────────────────────────────────────────────────────┐
                  │                 REAL-TIME INGESTION LAYER                   │
                  ├──────────────────────┬──────────────────────┬───────────────┤
                  │ File Watcher Daemon  │ REST API Endpoints   │ Dashboard UI  │
                  │ (data/incoming/)     │ (/api/v1/ingest/*)   │ Drag & Drop   │
                  └──────────┬───────────┴──────────┬───────────┴───────┬───────┘
                             │                      │                   │
                             ▼                      ▼                   ▼
                  ┌─────────────────────────────────────────────────────────────┐
                  │              REAL-TIME EVENT-DRIVEN ELT ENGINE              │
                  │  - Data Quality Filter Gates (Corrupt Value Filters)        │
                  │  - Kimball Star Schema SCD Type 2 (dim_customer versioning) │
                  │  - Point-in-Time Fact Loading (fact_sales)                  │
                  └──────────┬──────────────────────┬───────────────────┬───────┘
                             │                      │                   │
                             ▼                      ▼                   ▼
     ┌──────────────────────────────┐ ┌───────────────────────────┐ ┌───────────────────────────┐
     │ ML PREDICTIVE ANALYTICS      │ │ WEBHOOK NOTIFICATIONS     │ │ CLOUD REPLICATION         │
     │ - Customer Churn Risk (%)    │ │ - Slack / Discord Webhooks│ │ - Local DuckDB ->         │
     │ - 3-Month AI Sales Forecast  │ │ - Quality Failure Alerts  │ │   Supabase PostgreSQL     │
     │ - Order Anomaly Detector     │ │ - High-Value VIP Orders   │ │   Background Replicator   │
     └──────────────────────────────┘ └───────────────────────────┘ └───────────────────────────┘
                             │                      │                   │
                             └──────────────────────┼───────────────────┘
                                                    ▼
                  ┌─────────────────────────────────────────────────────────────┐
                  │              dbt TEST ASSERTER & DOCS SERVER                │
                  │  - 9/9 Model Test Assertions Passed (PK, FK, Non-Null)      │
                  │  - Interactive Visual Lineage DAG (http://localhost:8000/dbt-docs)
                  └─────────────────────────────────────────────────────────────┘
```

---

## ✨ Core System Capabilities

### 1. Real-Time Automated Data Ingestion
- **Automated Directory File Watcher ([pipeline/watcher.py](file:///c:/Users/niraa/OneDrive/Desktop/Data%20Warehouse/pipeline/watcher.py))**: Continuously monitors `data/incoming/` for `.csv` or `.json` files. As soon as a file is dropped, it is ingested, transformed, and archived to `data/archive/`.
- **FastAPI REST API ([api/main.py](file:///c:/Users/niraa/OneDrive/Desktop/Data%20Warehouse/api/main.py))**:
  - `POST /api/v1/ingest/csv`: Multipart CSV file upload endpoint.
  - `POST /api/v1/ingest/json`: Streaming JSON micro-batch event ingestion.
  - `GET /api/v1/realtime/stream-status`: Real-time warehouse metrics & row counts.

### 2. Real-Time Kimball Star Schema ELT Engine ([pipeline/realtime_engine.py](file:///c:/Users/niraa/OneDrive/Desktop/Data%20Warehouse/pipeline/realtime_engine.py))
- **Data Quality Gates**: Rejects corrupt rows (negative quantity, null keys) and logs failures to `data_quality_log`.
- **Kimball SCD Type 2 (`dim_customer`)**: Detects profile changes, expires historical versions (`is_current = False`), and creates net-new active versions (`is_current = True`).
- **Point-in-Time Fact Loading (`fact_sales`)**: Resolves customer surrogate keys against SCD2 date ranges and calculates gross revenue, net revenue, cost, profit, and margin %.

### 3. Machine Learning Predictive Intelligence ([ml/predictor.py](file:///c:/Users/niraa/OneDrive/Desktop/Data%20Warehouse/ml/predictor.py))
- **Customer Churn Risk Model**: Predicts churn risk probability % (`High Risk`, `Medium Risk`, `Low Risk`) based on order recency, frequency, and spend.
- **Sales Revenue & Demand Forecast**: 3-month AI revenue and unit demand forecast using Holt-Winters Exponential Smoothing.
- **Order Anomaly Detector**: Statistical Z-score outlier detection identifying high-value surge transactions.

### 4. Real-Time Webhook & Alert Notifications ([pipeline/notifications.py](file:///c:/Users/niraa/OneDrive/Desktop/Data%20Warehouse/pipeline/notifications.py))
- Dispatches color-coded **Slack** & **Discord** Webhooks (or console logs) when:
  - Data Quality check failures occur (`CRITICAL` Red).
  - High-value VIP transactions $\ge \$1,000$ occur (`VIP` Gold).

### 5. Supabase Cloud Replication Engine ([pipeline/cloud_sync.py](file:///c:/Users/niraa/OneDrive/Desktop/Data%20Warehouse/pipeline/cloud_sync.py))
- Background daemon thread replicating local DuckDB tables into **Supabase PostgreSQL Cloud** to keep remote BI tools updated live.

### 6. dbt Incremental Tests & Live Documentation Server ([pipeline/dbt_runner.py](file:///c:/Users/niraa/OneDrive/Desktop/Data%20Warehouse/pipeline/dbt_runner.py))
- Programmatic dbt model test suite verifying PK uniqueness, non-null constraints, and FK referential integrity (**9/9 tests PASSED**).
- Interactive visual dbt documentation site served directly at `http://localhost:8000/dbt-docs`.

---

## 🐳 Docker Compose Deployment

The complete microservices stack is fully containerized:

```bash
# One-Click Production Start (Docker Compose)
docker compose up --build -d
```

### Windows & Linux One-Click Launchers
- **Windows**: `scripts\docker_run.bat`
- **Linux / macOS**: `./scripts/docker_run.sh`

---

## 💻 Local Quick Start Guide

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Real-Time Ingestion Daemon & REST API
```bash
python run.py --realtime
```
*Monitors `data/incoming/` and accepts HTTP POST requests on `http://localhost:8000`.*

### 3. Run Executive Analytics Dashboard
```bash
python scripts/run_dashboard.py
```
*Open `http://localhost:8501` to view live analytics charts and drag-and-drop CSV upload sidebar.*

---

## 🔗 Endpoint Quick Reference

| Service / Interface | URL | Description |
|---|---|---|
| **FastAPI Swagger Docs** | `http://localhost:8000/docs` | Interactive REST API Documentation |
| **dbt Documentation & Lineage DAG** | `http://localhost:8000/dbt-docs` | Interactive dbt Star Schema Models & DAG |
| **Streamlit Analytics Dashboard** | `http://localhost:8501` | Executive BI Dashboard |
| **Stream Status API** | `http://localhost:8000/api/v1/realtime/stream-status` | Live Data Warehouse Row Counts |
| **ML Churn Risk API** | `http://localhost:8000/api/v1/ml/churn-risk` | Customer Churn Risk Scores |
| **ML Revenue Forecast API** | `http://localhost:8000/api/v1/ml/revenue-forecast` | 3-Month AI Revenue Projections |
| **Cloud Sync Status API** | `http://localhost:8000/api/v1/cloud/sync-status` | Supabase Cloud Sync Audit |
