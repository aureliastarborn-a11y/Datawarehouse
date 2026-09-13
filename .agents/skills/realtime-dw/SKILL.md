---
name: realtime-dw
description: Real-Time Event-Driven Data Warehouse system with Kimball SCD2 modeling, ML predictors, Webhook alerts, Supabase cloud sync, dbt docs server, and Docker Compose deployment.
---

# Real-Time Data Warehouse Skill & Operational Procedures

This skill provides operational workflows, CLI entry points, and architectural rules for the **Real-Time Data Warehouse System**.

## 1. System Execution Entry Points

### Run Real-Time Ingestion Daemon & REST API
```bash
python run.py --realtime
# Runs directory watcher monitoring data/incoming/ and starts FastAPI on port 8000
```

### Run Executive Analytics Dashboard
```bash
python scripts/run_dashboard.py
# Runs Streamlit UI on http://localhost:8501
```

### Run One-Click Docker Compose Stack
```bash
docker compose up --build -d
```

---

## 2. API & Interface Reference

- **Swagger REST API**: `http://localhost:8000/docs`
- **Interactive dbt Docs & Lineage DAG**: `http://localhost:8000/dbt-docs`
- **Stream Status Metrics API**: `http://localhost:8000/api/v1/realtime/stream-status`
- **Customer Churn Risk ML API**: `http://localhost:8000/api/v1/ml/churn-risk`
- **3-Month Sales Forecast ML API**: `http://localhost:8000/api/v1/ml/revenue-forecast`
- **Cloud Sync Status API**: `http://localhost:8000/api/v1/cloud/sync-status`

---

## 3. Data Ingestion Procedures

- **Directory Drop**: Drop `.csv` or `.json` files directly into `data/incoming/`. Successfully processed files move to `data/archive/`.
- **REST Event Streaming**: POST JSON micro-batches to `/api/v1/ingest/json`.
- **High-Frequency Stream Generator**: Run `python scripts/stream_generator.py`.

---

## 4. Cloud & DB Management

- **Supabase Cloud Migration Setup**: Run `python scripts/setup_supabase.py`.
- **Programmatic dbt Test Suite**: Run `python -c "from pipeline.dbt_runner import run_dbt_tests; run_dbt_tests()"`
