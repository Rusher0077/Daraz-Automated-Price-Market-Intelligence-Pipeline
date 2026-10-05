-- schema.sql
-- Daraz Market Intelligence Pipeline, Neon PostgreSQL schema

-- raw_staging: untouched, as-received data per simulated daily batch.
-- Append-only, mirrors the raw columns from raw_combined.csv exactly.
CREATE TABLE raw_staging (
    staging_id SERIAL PRIMARY KEY,
    row_id INTEGER NOT NULL,
    id INTEGER,
    name TEXT,
    description TEXT,
    availability TEXT,
    brand TEXT,
    category TEXT,
    sku TEXT,
    mpn TEXT,
    price TEXT,              
    original_price TEXT,
    discount_perc TEXT,
    sold_by TEXT,
    positive_seller_ratings TEXT,
    ship_on_time TEXT,
    seller_rating TEXT,
    rating TEXT,
    source_sheet TEXT,
    workbook TEXT,
    scrape_batch_id INTEGER NOT NULL,   
    ingested_at TIMESTAMP NOT NULL DEFAULT now()
);

-- clean_products: output of clean_batch(), one row per validated,
-- deduplicated product. Matches the 23 columns confirmed from cleaning.py.
CREATE TABLE clean_products (
    product_id SERIAL PRIMARY KEY,
    row_id INTEGER NOT NULL,
    id INTEGER,
    name TEXT,
    availability TEXT,
    brand TEXT,
    category TEXT,                      
    sku TEXT,
    mpn TEXT,
    price NUMERIC,
    original_price NUMERIC,
    discount_perc NUMERIC,
    sold_by TEXT,
    positive_seller_ratings NUMERIC,
    ship_on_time NUMERIC,
    seller_rating NUMERIC,
    rating NUMERIC,
    source_sheet TEXT,
    workbook TEXT,
    category_normalized TEXT,
    category_leaf TEXT,
    is_discounted BOOLEAN,
    category_match_score NUMERIC,
    is_contamination_flagged BOOLEAN,
    scrape_batch_id INTEGER NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT now()
);

-- pipeline_run_log: one row per pipeline execution, per requirements.md Section 5
CREATE TABLE pipeline_run_log (
    run_id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP NOT NULL DEFAULT now(),
    scrape_batch_id INTEGER,
    rows_in INTEGER,
    rows_cleaned INTEGER,
    rows_flagged_contamination INTEGER,
    rows_dropped INTEGER,
    duration_seconds NUMERIC,
    errors TEXT
);