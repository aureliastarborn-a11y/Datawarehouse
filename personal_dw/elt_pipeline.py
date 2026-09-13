import os
import duckdb

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "warehouse.duckdb")

def get_duckdb_connection():
    """Returns a connection to the local DuckDB OLAP Data Warehouse engine."""
    return duckdb.connect(DB_PATH)

def init_olap_schema(con=None):
    """Initializes Staging, Kimball Star Schema Dimensions/Facts, and Analytical Views in DuckDB."""
    should_close = False
    if con is None:
        con = get_duckdb_connection()
        should_close = True

    # 1. STAGING TABLES (Raw Ingestion Layer)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stg_finance_transactions (
            raw_id VARCHAR,
            amount DOUBLE,
            category_name VARCHAR,
            category_type VARCHAR,
            merchant VARCHAR,
            transaction_date DATE,
            notes VARCHAR,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS stg_fitness_workouts (
            raw_id VARCHAR,
            workout_name VARCHAR,
            workout_category VARCHAR,
            duration_minutes INTEGER,
            calories_burned INTEGER,
            workout_date DATE,
            intensity VARCHAR,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS stg_media_consumption (
            raw_id VARCHAR,
            title VARCHAR,
            media_type VARCHAR,
            genre VARCHAR,
            creator VARCHAR,
            consumed_date DATE,
            rating DOUBLE,
            notes VARCHAR,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # 2. KIMBALL STAR SCHEMA DIMENSIONS & FACTS
    con.execute("""
        -- Finance Domain
        CREATE TABLE IF NOT EXISTS dim_category (
            category_key BIGINT PRIMARY KEY,
            category_name VARCHAR UNIQUE,
            category_type VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS fact_transactions (
            transaction_key BIGINT PRIMARY KEY,
            raw_id VARCHAR UNIQUE,
            category_key BIGINT,
            amount DOUBLE,
            merchant VARCHAR,
            transaction_date DATE,
            notes VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Fitness Domain
        CREATE TABLE IF NOT EXISTS dim_workout_type (
            workout_type_key BIGINT PRIMARY KEY,
            workout_name VARCHAR UNIQUE,
            workout_category VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS fact_workouts (
            workout_key BIGINT PRIMARY KEY,
            raw_id VARCHAR UNIQUE,
            workout_type_key BIGINT,
            duration_minutes INTEGER,
            calories_burned INTEGER,
            workout_date DATE,
            intensity VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Media Domain
        CREATE TABLE IF NOT EXISTS dim_media_item (
            media_key BIGINT PRIMARY KEY,
            title VARCHAR,
            media_type VARCHAR,
            genre VARCHAR,
            creator VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS fact_media_consumption (
            log_key BIGINT PRIMARY KEY,
            raw_id VARCHAR UNIQUE,
            media_key BIGINT,
            consumed_date DATE,
            rating DOUBLE,
            notes VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    if should_close:
        con.close()

def run_elt_transformations(con=None):
    """Executes automated ELT SQL transformations to populate Kimball Star Schema tables from Staging."""
    should_close = False
    if con is None:
        con = get_duckdb_connection()
        should_close = True

    # -------------------------------------------------------------------------
    # ELT STEP 1: FINANCE TRANSFORMATIONS (Staging -> Dim_Category -> Fact_Transactions)
    # -------------------------------------------------------------------------
    # Populate Category Dimension
    con.execute("""
        INSERT INTO dim_category (category_key, category_name, category_type)
        SELECT 
            ROW_NUMBER() OVER () + COALESCE((SELECT MAX(category_key) FROM dim_category), 0) AS category_key,
            s.category_name,
            COALESCE(s.category_type, 'Expense') AS category_type
        FROM stg_finance_transactions s
        WHERE s.category_name IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM dim_category dc WHERE dc.category_name = s.category_name
          )
        GROUP BY s.category_name, s.category_type;
    """)

    # Populate Transaction Fact Table
    con.execute("""
        INSERT INTO fact_transactions (transaction_key, raw_id, category_key, amount, merchant, transaction_date, notes)
        SELECT
            ROW_NUMBER() OVER () + COALESCE((SELECT MAX(transaction_key) FROM fact_transactions), 0) AS transaction_key,
            s.raw_id,
            dc.category_key,
            s.amount,
            s.merchant,
            s.transaction_date,
            s.notes
        FROM stg_finance_transactions s
        JOIN dim_category dc ON s.category_name = dc.category_name
        WHERE s.raw_id NOT IN (SELECT raw_id FROM fact_transactions);
    """)

    # -------------------------------------------------------------------------
    # ELT STEP 2: FITNESS TRANSFORMATIONS (Staging -> Dim_Workout_Type -> Fact_Workouts)
    # -------------------------------------------------------------------------
    # Populate Workout Type Dimension
    con.execute("""
        INSERT INTO dim_workout_type (workout_type_key, workout_name, workout_category)
        SELECT
            ROW_NUMBER() OVER () + COALESCE((SELECT MAX(workout_type_key) FROM dim_workout_type), 0) AS workout_type_key,
            s.workout_name,
            COALESCE(s.workout_category, 'Cardio') AS workout_category
        FROM stg_fitness_workouts s
        WHERE s.workout_name IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM dim_workout_type dw WHERE dw.workout_name = s.workout_name
          )
        GROUP BY s.workout_name, s.workout_category;
    """)

    # Populate Workout Fact Table
    con.execute("""
        INSERT INTO fact_workouts (workout_key, raw_id, workout_type_key, duration_minutes, calories_burned, workout_date, intensity)
        SELECT
            ROW_NUMBER() OVER () + COALESCE((SELECT MAX(workout_key) FROM fact_workouts), 0) AS workout_key,
            s.raw_id,
            dw.workout_type_key,
            s.duration_minutes,
            s.calories_burned,
            s.workout_date,
            s.intensity
        FROM stg_fitness_workouts s
        JOIN dim_workout_type dw ON s.workout_name = dw.workout_name
        WHERE s.raw_id NOT IN (SELECT raw_id FROM fact_workouts);
    """)

    # -------------------------------------------------------------------------
    # ELT STEP 3: MEDIA TRANSFORMATIONS (Staging -> Dim_Media_Item -> Fact_Media)
    # -------------------------------------------------------------------------
    # Populate Media Item Dimension
    con.execute("""
        INSERT INTO dim_media_item (media_key, title, media_type, genre, creator)
        SELECT
            ROW_NUMBER() OVER () + COALESCE((SELECT MAX(media_key) FROM dim_media_item), 0) AS media_key,
            s.title,
            s.media_type,
            s.genre,
            s.creator
        FROM stg_media_consumption s
        WHERE s.title IS NOT NULL AND s.media_type IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM dim_media_item dm WHERE dm.title = s.title AND dm.media_type = s.media_type
          )
        GROUP BY s.title, s.media_type, s.genre, s.creator;
    """)

    # Populate Media Consumption Fact Table
    con.execute("""
        INSERT INTO fact_media_consumption (log_key, raw_id, media_key, consumed_date, rating, notes)
        SELECT
            ROW_NUMBER() OVER () + COALESCE((SELECT MAX(log_key) FROM fact_media_consumption), 0) AS log_key,
            s.raw_id,
            dm.media_key,
            s.consumed_date,
            s.rating,
            s.notes
        FROM stg_media_consumption s
        JOIN dim_media_item dm ON s.title = dm.title AND s.media_type = dm.media_type
        WHERE s.raw_id NOT IN (SELECT raw_id FROM fact_media_consumption);
    """)

    if should_close:
        con.close()

if __name__ == "__main__":
    print("Initializing DuckDB OLAP Schemas & Tables...")
    init_olap_schema()
    print("Running initial ELT Pipeline Transformations...")
    run_elt_transformations()
    print("[SUCCESS] DuckDB Personal Data Warehouse initialized successfully!")
