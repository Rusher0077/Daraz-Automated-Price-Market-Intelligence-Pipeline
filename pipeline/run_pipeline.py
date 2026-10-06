# %%
# Database connection setup
# Reads the Neon connection string from .env, never hardcoded here.

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

NEON_CONNECTION_STRING = os.getenv("NEON_CONNECTION_STRING")

if NEON_CONNECTION_STRING is None:
    raise ValueError("NEON_CONNECTION_STRING not found, check your .env file")

engine = create_engine(NEON_CONNECTION_STRING)


# %%
# Test: confirming the connection actually works

from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"))
    tables = [row[0] for row in result]

print("Connected. Tables found:", tables)
# %%
# run_pipeline.py
# Orchestrates the pipeline: chunks raw data to simulate daily arrival,
# loads each chunk into Neon, runs it through clean_batch(), writes the
# cleaned result, and logs the run. Imports cleaning.py rather than
# duplicating any of its logic.

import pandas as pd
import numpy as np

from cleaning import load_raw, clean_batch

N_CHUNKS = 7


# %%
# Step 1: stratified chunking
# Splits raw_combined into N_CHUNKS pieces, stratified by category_normalized.
# Only the category mapping is applied before chunking, not the full
# clean_batch(), so this stays genuinely raw data, no price/text cleaning,
# no TF-IDF, that all still happens per chunk after loading.

from cleaning import CATEGORY_MAP

def chunk_raw_data(df, n_chunks = N_CHUNKS, random_state = 42):
    df= df.copy()
    df["scrape_batch_id"] = -1 # placeholder, will get overwritten later

    strat_col = df["source_sheet"].map(CATEGORY_MAP)

    for category in strat_col.unique():
        cat_idx = df[strat_col == category].index
        shuffled = pd.Series(cat_idx).sample(frac = 1, random_state=random_state).values
        batch_assignments = np.array_split(shuffled, n_chunks)

        for batch_num, idx_group in enumerate(batch_assignments, start=1):
            df.loc[idx_group, "scrape_batch_id"] = batch_num

    return df


# %%
# Test: confirm every sheet is represented in every chunk, and chunk sizes
# are reasonably balanced

df_raw = load_raw()
df_chunked = chunk_raw_data(df_raw)

print("rows per chunk: ")
print(df_chunked["scrape_batch_id"].value_counts().sort_index())

print()

print("sheets represented per chunk (should be 16 in each chunk): ")
print(df_chunked.groupby("scrape_batch_id")["source_sheet"].nunique())
print()

# spot check: HDD, smallest sheet of 200 rows, across all 7 chunks
print("HDD rows per chunk:")
print(df_chunked[df_chunked["source_sheet"] == "HDD"]["scrape_batch_id"].value_counts().sort_index())

# %%
# Fit the contamination reference ONCE, on the full raw dataset, before
# any chunk processing begins. Every batch_id run reuses this same
# vectorizer and these same centroids.

from cleaning import fit_contamination_reference

df_raw_full = load_raw() # full dataset, needed to build a representative reference
vectorizer, centroids = fit_contamination_reference(df_raw_full)

print("Reference fit on", len(df_raw_full), "rows,", len(centroids), "sheet centroid")

# %%
# Step 2: process one chunk end to end
# Simulates one pipeline run: load a chunk into raw_staging, clean
# it, write the result to clean_products, log the run. This is the function
# that gets manually triggered per simulated batch.

import time
from datetime import datetime

# Column name mapping, dataframe names to Neon's lowercase snake_case schema.
# Only columns whose names actually differ need an entry here.

RAW_COLUMN_MAP = {
    "ID": "id",
    "Name": "name",
    "Description": "description",
    "Availability": "availability",
    "Brand": "brand",
    "Category": "category",
    "SKU": "sku",
    "MPN": "mpn",
    "Price": "price",
    "Original_Price": "original_price",
    "Discount_Perc": "discount_perc",
    "sold_by": "sold_by",
    "Positive Seller Ratings": "positive_seller_ratings",
    "ship_on_time": "ship_on_time",
    "seller_rating": "seller_rating",
    "rating": "rating",
    "source_sheet": "source_sheet",
    "workbook": "workbook",
}

CLEAN_COLUMN_MAP = {
    "ID": "id",
    "Name": "name",
    "Availability": "availability",
    "Brand": "brand",
    "Category": "category",
    "SKU": "sku",
    "MPN": "mpn",
    "Price": "price",
    "Original_Price": "original_price",
    "Discount_Perc": "discount_perc",
    "sold_by": "sold_by",
    "Positive Seller Ratings": "positive_seller_ratings",
    "ship_on_time": "ship_on_time",
    "seller_rating": "seller_rating",
    "rating": "rating",
    "source_sheet": "source_sheet",
    "workbook": "workbook",
    "category_normalized": "category_normalized",
    "category_leaf": "category_leaf",
    "is_discounted": "is_discounted",
    "category_match_score": "category_match_score",
    "is_contamination_flagged": "is_contamination_flagged",
}

def process_chunk(df_chunked, batch_id, engine):
    start_time = time.time()
    errors = None

    chunk = df_chunked[df_chunked["scrape_batch_id"] == batch_id].copy()
    rows_in = len(chunk)
    # cross-chunk dedup: filter out SKUs already written to clean_products by
    # an earlier batch. Catches the 28 known cross-listed products that could
    # otherwise land in two different chunks.

    with engine.connect() as conn:
        existing_skus = pd.read_sql(text("SELECT sku FROM clean_products"), conn)["sku"].tolist()

    before_cross_dedup = len(chunk)
    chunk = chunk[~chunk["SKU"].isin(existing_skus)] # SKU, capital, matches raw_combined's column name
    cross_chunk_dropped = before_cross_dedup - len(chunk)
    if cross_chunk_dropped > 0:
        print(f"batch {batch_id}: {cross_chunk_dropped} rows dropped, SKU already in clean_products from an earlier batch")

    try:
        # write the raw chunk to raw_staging, untouched
        raw_to_write = chunk.rename(columns = RAW_COLUMN_MAP)
        raw_cols = list(RAW_COLUMN_MAP.values()) + ["row_id", "scrape_batch_id"]
        raw_to_write[raw_cols].to_sql("raw_staging", engine, if_exists = "append", index=False)

        # run the chunk through the cleaning pipeline
        cleaned = clean_batch(chunk, vectorizer, centroids)
        rows_cleaned = len(cleaned)
        rows_flagged = int(cleaned["is_contamination_flagged"].sum())
        rows_dropped = rows_in - rows_cleaned

        # write the cleaned chunk to clean_products
        clean_to_write = cleaned.rename(columns = CLEAN_COLUMN_MAP)
        clean_cols = list(CLEAN_COLUMN_MAP.values()) + ["row_id", "scrape_batch_id"]
        clean_to_write["scrape_batch_id"] = batch_id
        clean_to_write[clean_cols].to_sql("clean_products", engine, if_exists="append", index=False)

    except Exception as e:
        errors = str(e)
        rows_cleaned = 0
        rows_flagged = 0
        rows_dropped = rows_in

    duration_seconds = round(time.time() - start_time, 2)

    # log the run regardless of success or failure
    log_row = pd.DataFrame([{
        "scrape_batch_id": batch_id,
        "rows_in": rows_in,
        "rows_cleaned": rows_cleaned,
        "rows_flagged_contamination": rows_flagged,
        "rows_dropped": rows_dropped,
        "duration_seconds": duration_seconds,
        "errors": errors,
    }])
    log_row.to_sql("pipeline_run_log", engine, if_exists="append", index = False)

    status = "Failed" if errors else "Ok"
    print(f"batch {batch_id}: {status} | in={rows_in} cleaned={rows_cleaned} "
          f"flagged={rows_flagged} dropped={rows_dropped} duration={duration_seconds}s")

    if errors:
        print(f" error: {errors}")

    return cleaned if not errors else None


# %%
# Test: run chunk 1 only, as your first real manual trigger

result = process_chunk(df_chunked, batch_id=1, engine=engine)
# %%
