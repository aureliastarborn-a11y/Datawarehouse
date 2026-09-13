import os
import uuid
from datetime import date
from typing import Optional, Dict, Any
from contextlib import contextmanager

import duckdb
from fastapi import FastAPI, Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from personal_dw.elt_pipeline import (
    DB_PATH,
    get_duckdb_connection,
    init_olap_schema,
    run_elt_transformations
)

# Load environment variables
load_dotenv()

API_KEY = os.getenv("API_KEY", "secret-dw-key-123")

# Initialize DuckDB Schema on Module Import
init_olap_schema()
run_elt_transformations()

# Initialize FastAPI App
app = FastAPI(
    title="Personal Data Warehouse OLAP API Layer",
    description="FastAPI ingestion & analytics backend connected to DuckDB OLAP Engine (Finance, Fitness, Media domains).",
    version="2.0.0"
)

# API Key Security Scheme
api_key_header_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(x_api_key: Optional[str] = Security(api_key_header_scheme)):
    """Security dependency enforcing X-API-Key header authentication."""
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key authentication header."
        )
    return x_api_key


@contextmanager
def get_db():
    """Database connection context manager targeting DuckDB OLAP storage."""
    conn = get_duckdb_connection()
    try:
        yield conn
    finally:
        conn.close()

# ============================================================================
# PYDANTIC INGESTION SCHEMAS
# ============================================================================

class FinanceIngestPayload(BaseModel):
    amount: float = Field(..., description="Transaction amount in currency units", example=145.50)
    category_name: str = Field(..., description="Category name (e.g. Groceries, Tech)", example="Software/Tech")
    category_type: Optional[str] = Field("Expense", description="Expense or Income", example="Expense")
    merchant: str = Field(..., description="Merchant / Vendor name", example="AWS Services")
    transaction_date: Optional[date] = Field(default_factory=date.today, description="Date of transaction")
    notes: Optional[str] = Field(None, description="Optional notes", example="Monthly Cloud Hosting Bill")


class FitnessIngestPayload(BaseModel):
    workout_name: str = Field(..., description="Workout name (e.g. Running, Weightlifting)", example="Running")
    workout_category: Optional[str] = Field("Cardio", description="Category (Cardio, Strength, etc.)", example="Cardio")
    duration_minutes: int = Field(..., gt=0, description="Duration in minutes", example=45)
    calories_burned: Optional[int] = Field(0, ge=0, description="Estimated calories burned", example=420)
    workout_date: Optional[date] = Field(default_factory=date.today, description="Date of workout")
    intensity: Optional[str] = Field("Moderate", description="Intensity level (Low, Moderate, High)", example="High")


class MediaIngestPayload(BaseModel):
    title: str = Field(..., description="Media title", example="Designing Data-Intensive Applications")
    media_type: str = Field(..., description="Type (Book, Movie, Podcast, TV Show)", example="Book")
    genre: Optional[str] = Field(None, description="Genre tag", example="Technology")
    creator: Optional[str] = Field(None, description="Author / Director / Host", example="Martin Kleppmann")
    consumed_date: Optional[date] = Field(default_factory=date.today, description="Consumption date")
    rating: Optional[float] = Field(None, ge=0.0, le=10.0, description="Rating from 0.0 to 10.0", example=9.8)
    notes: Optional[str] = Field(None, description="Personal review notes", example="Essential read for system design")


# ============================================================================
# HEALTH & ROUTE DISCOVERY
# ============================================================================

@app.get("/")
def root():
    return {
        "status": "online",
        "service": "Personal Data Warehouse API Layer",
        "engine": "DuckDB Embedded OLAP Engine",
        "storage": DB_PATH,
        "docs": "/docs"
    }

# ============================================================================
# INGESTION ENDPOINTS (POST /api/v1/ingest/{domain})
# ============================================================================

@app.post("/api/v1/ingest/finance", dependencies=[Depends(verify_api_key)], status_code=status.HTTP_201_CREATED)
def ingest_finance(payload: FinanceIngestPayload):
    raw_id = f"FIN-{uuid.uuid4().hex[:8].upper()}"
    with get_db() as conn:
        # Load raw staging table
        conn.execute("""
            INSERT INTO stg_finance_transactions 
            (raw_id, amount, category_name, category_type, merchant, transaction_date, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (raw_id, payload.amount, payload.category_name, payload.category_type, payload.merchant, payload.transaction_date, payload.notes))

        # Execute automated ELT pipeline transformation
        run_elt_transformations(conn)

    return {
        "message": "Financial transaction ingested and transformed into Star Schema fact table",
        "raw_id": raw_id,
        "payload": payload
    }


@app.post("/api/v1/ingest/fitness", dependencies=[Depends(verify_api_key)], status_code=status.HTTP_201_CREATED)
def ingest_fitness(payload: FitnessIngestPayload):
    raw_id = f"FIT-{uuid.uuid4().hex[:8].upper()}"
    with get_db() as conn:
        # Load raw staging table
        conn.execute("""
            INSERT INTO stg_fitness_workouts 
            (raw_id, workout_name, workout_category, duration_minutes, calories_burned, workout_date, intensity)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (raw_id, payload.workout_name, payload.workout_category, payload.duration_minutes, payload.calories_burned, payload.workout_date, payload.intensity))

        # Execute automated ELT pipeline transformation
        run_elt_transformations(conn)

    return {
        "message": "Fitness workout ingested and transformed into Star Schema fact table",
        "raw_id": raw_id,
        "payload": payload
    }


@app.post("/api/v1/ingest/media", dependencies=[Depends(verify_api_key)], status_code=status.HTTP_201_CREATED)
def ingest_media(payload: MediaIngestPayload):
    raw_id = f"MED-{uuid.uuid4().hex[:8].upper()}"
    with get_db() as conn:
        # Load raw staging table
        conn.execute("""
            INSERT INTO stg_media_consumption 
            (raw_id, title, media_type, genre, creator, consumed_date, rating, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (raw_id, payload.title, payload.media_type, payload.genre, payload.creator, payload.consumed_date, payload.rating, payload.notes))

        # Execute automated ELT pipeline transformation
        run_elt_transformations(conn)

    return {
        "message": "Media log ingested and transformed into Star Schema fact table",
        "raw_id": raw_id,
        "payload": payload
    }


@app.post("/api/v1/ingest/{domain}", dependencies=[Depends(verify_api_key)], status_code=status.HTTP_201_CREATED)
def ingest_generic(domain: str, payload: Dict[str, Any]):
    domain_lower = domain.lower()
    if domain_lower == "finance":
        p = FinanceIngestPayload(**payload)
        return ingest_finance(p)
    elif domain_lower == "fitness":
        p = FitnessIngestPayload(**payload)
        return ingest_fitness(p)
    elif domain_lower == "media":
        p = MediaIngestPayload(**payload)
        return ingest_media(p)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported domain '{domain}'. Valid domains: finance, fitness, media")


# ============================================================================
# ANALYTICS ENDPOINTS (GET /api/v1/analytics/{domain})
# ============================================================================

@app.get("/api/v1/analytics/finance", dependencies=[Depends(verify_api_key)])
def get_finance_analytics():
    with get_db() as conn:
        summary = conn.execute("""
            SELECT 
                COUNT(f.transaction_key) AS total_transactions,
                COALESCE(ROUND(SUM(f.amount), 2), 0.00) AS total_spend,
                COALESCE(ROUND(AVG(f.amount), 2), 0.00) AS avg_transaction_amount,
                COALESCE(ROUND(MAX(f.amount), 2), 0.00) AS max_single_transaction
            FROM fact_transactions f;
        """).df().to_dict(orient="records")[0]

        categories = conn.execute("""
            SELECT 
                c.category_name,
                c.category_type,
                COUNT(f.transaction_key) AS transaction_count,
                COALESCE(ROUND(SUM(f.amount), 2), 0.00) AS category_total_spend
            FROM dim_category c
            LEFT JOIN fact_transactions f ON c.category_key = f.category_key
            GROUP BY c.category_name, c.category_type
            ORDER BY category_total_spend DESC;
        """).df().to_dict(orient="records")

    return {
        "domain": "finance",
        "engine": "DuckDB OLAP Star Schema",
        "summary": summary,
        "category_breakdown": categories
    }


@app.get("/api/v1/analytics/fitness", dependencies=[Depends(verify_api_key)])
def get_fitness_analytics():
    with get_db() as conn:
        summary = conn.execute("""
            SELECT 
                COUNT(f.workout_key) AS total_workouts,
                COALESCE(SUM(f.duration_minutes), 0) AS total_duration_minutes,
                COALESCE(SUM(f.calories_burned), 0) AS total_calories_burned,
                COALESCE(ROUND(AVG(f.duration_minutes), 1), 0.0) AS avg_workout_duration_min
            FROM fact_workouts f;
        """).df().to_dict(orient="records")[0]

        breakdown = conn.execute("""
            SELECT 
                t.workout_name,
                t.workout_category,
                COUNT(f.workout_key) AS total_sessions,
                COALESCE(SUM(f.duration_minutes), 0) AS total_minutes,
                COALESCE(SUM(f.calories_burned), 0) AS total_calories
            FROM dim_workout_type t
            LEFT JOIN fact_workouts f ON t.workout_type_key = f.workout_type_key
            GROUP BY t.workout_name, t.workout_category
            ORDER BY total_sessions DESC;
        """).df().to_dict(orient="records")

    return {
        "domain": "fitness",
        "engine": "DuckDB OLAP Star Schema",
        "summary": summary,
        "workout_breakdown": breakdown
    }


@app.get("/api/v1/analytics/media", dependencies=[Depends(verify_api_key)])
def get_media_analytics():
    with get_db() as conn:
        summary = conn.execute("""
            SELECT 
                COUNT(f.log_key) AS total_items_consumed,
                COALESCE(ROUND(AVG(f.rating), 2), 0.0) AS avg_rating
            FROM fact_media_consumption f;
        """).df().to_dict(orient="records")[0]

        type_breakdown = conn.execute("""
            SELECT 
                m.media_type,
                COUNT(f.log_key) AS items_logged,
                COALESCE(ROUND(AVG(f.rating), 2), 0.0) AS avg_type_rating
            FROM dim_media_item m
            JOIN fact_media_consumption f ON m.media_key = f.media_key
            GROUP BY m.media_type
            ORDER BY items_logged DESC;
        """).df().to_dict(orient="records")

    return {
        "domain": "media",
        "engine": "DuckDB OLAP Star Schema",
        "summary": summary,
        "type_breakdown": type_breakdown
    }


@app.get("/api/v1/analytics/{domain}", dependencies=[Depends(verify_api_key)])
def get_generic_analytics(domain: str):
    domain_lower = domain.lower()
    if domain_lower == "finance":
        return get_finance_analytics()
    elif domain_lower == "fitness":
        return get_fitness_analytics()
    elif domain_lower == "media":
        return get_media_analytics()
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported analytics domain '{domain}'. Valid domains: finance, fitness, media")
