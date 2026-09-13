# ============================================================================
# Production Dockerfile — Real-Time Automated Data Warehouse
# ============================================================================
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

# Set working directory inside container
WORKDIR /app

# Install system compilation dependencies for DuckDB and C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . /app/

# Create data directories
RUN mkdir -p /app/data/incoming /app/data/archive /app/data/failed /app/target

# Expose ports: 8000 (FastAPI & dbt Docs), 8501 (Streamlit Dashboard)
EXPOSE 8000 8501

# Default command runs the Real-Time Ingestion Daemon & API Server
CMD ["python", "run.py", "--realtime"]
