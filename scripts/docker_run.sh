#!/usr/bin/env bash
# ============================================================================
# One-Click Docker Container Launcher — Real-Time Data Warehouse
# ============================================================================
set -e

echo "======================================================================"
echo "  STARTING DOCKER CONTAINER DEPLOYMENT STACK"
echo "======================================================================"
echo ""

docker compose up --build -d

echo ""
echo "======================================================================"
echo "  DEPLOYMENT SUCCESSFUL!"
echo "======================================================================"
echo "  - FastAPI REST Ingestion API:  http://localhost:8000/docs"
echo "  - Interactive dbt Docs & DAG:  http://localhost:8000/dbt-docs"
echo "  - Executive Analytics Dashboard: http://localhost:8501"
echo "======================================================================"
