-- ============================================================================
-- Personal Data Warehouse Initialization Script
-- Creates 3 Domain Schemas: finance, fitness, media
-- Executed automatically on PostgreSQL container start
-- ============================================================================

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS finance;
CREATE SCHEMA IF NOT EXISTS fitness;
CREATE SCHEMA IF NOT EXISTS media;

-- ============================================================================
-- 1. FINANCE SCHEMA (Star Schema)
-- ============================================================================

CREATE TABLE IF NOT EXISTS finance.dim_category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    category_type VARCHAR(50) DEFAULT 'Expense',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS finance.fact_transactions (
    transaction_id SERIAL PRIMARY KEY,
    amount NUMERIC(12, 2) NOT NULL,
    category_id INT REFERENCES finance.dim_category(category_id),
    merchant VARCHAR(150) NOT NULL,
    transaction_date DATE NOT NULL DEFAULT CURRENT_DATE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 2. FITNESS SCHEMA (Star Schema)
-- ============================================================================

CREATE TABLE IF NOT EXISTS fitness.dim_workout_type (
    workout_type_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50) DEFAULT 'Cardio',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fitness.fact_workouts (
    workout_id SERIAL PRIMARY KEY,
    workout_type_id INT REFERENCES fitness.dim_workout_type(workout_type_id),
    duration_minutes INT NOT NULL CHECK (duration_minutes > 0),
    calories_burned INT DEFAULT 0,
    workout_date DATE NOT NULL DEFAULT CURRENT_DATE,
    intensity VARCHAR(30) DEFAULT 'Moderate',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 3. MEDIA SCHEMA (Star Schema)
-- ============================================================================

CREATE TABLE IF NOT EXISTS media.dim_media_item (
    media_id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    media_type VARCHAR(50) NOT NULL, -- 'Movie', 'Book', 'Podcast', 'TV Show', 'Album'
    genre VARCHAR(100),
    creator VARCHAR(150),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_media_title_type UNIQUE (title, media_type)
);

CREATE TABLE IF NOT EXISTS media.fact_media_consumption (
    log_id SERIAL PRIMARY KEY,
    media_id INT REFERENCES media.dim_media_item(media_id),
    consumed_date DATE NOT NULL DEFAULT CURRENT_DATE,
    rating NUMERIC(3, 1) CHECK (rating >= 0.0 AND rating <= 10.0),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INITIAL SEED DATA FOR DIMENSIONS
-- ============================================================================

INSERT INTO finance.dim_category (category_name, category_type) VALUES
    ('Groceries', 'Expense'),
    ('Dining Out', 'Expense'),
    ('Utilities', 'Expense'),
    ('Software/Tech', 'Expense'),
    ('Salary', 'Income')
ON CONFLICT (category_name) DO NOTHING;

INSERT INTO fitness.dim_workout_type (name, category) VALUES
    ('Running', 'Cardio'),
    ('Weightlifting', 'Strength'),
    ('Cycling', 'Cardio'),
    ('Yoga', 'Flexibility'),
    ('Swimming', 'Cardio')
ON CONFLICT (name) DO NOTHING;

INSERT INTO media.dim_media_item (title, media_type, genre, creator) VALUES
    ('Designing Data-Intensive Applications', 'Book', 'Technology', 'Martin Kleppmann'),
    ('Inception', 'Movie', 'Sci-Fi', 'Christopher Nolan'),
    ('Lex Fridman Podcast', 'Podcast', 'Technology', 'Lex Fridman')
ON CONFLICT (title, media_type) DO NOTHING;
