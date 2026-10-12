# run_batch.py
# The automated entry point. Each run loads the next batch that has not been
# loaded yet (found from pipeline_run_log), so the same command works for the
# manual trigger now and the GitHub Actions trigger later.
#
#   python run_batch.py          load the next pending batch
#   python run_batch.py --all    load every pending batch in order (used for a full reload)
#
# run_pipeline.py stays as my scratchpad, this file has no test cells.

import argparse
import os
import sys
import time

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from cleaning_core import CATEGORY_MAP, clean_batch, fit_contamination_reference, load_raw

N_CHUNKS = 7

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


def get_engine():
    load_dotenv()  # local runs read pipeline/.env, GitHub Actions passes the secret as an env var
    conn_string = os.getenv("NEON_CONNECTION_STRING")
    if conn_string is None:
        raise ValueError("NEON_CONNECTION_STRING not set (.env locally, repo secret on GitHub)")
    # Neon can take a few seconds to wake up, so allow a longer connect timeout
    return create_engine(conn_string, pool_pre_ping=True, connect_args={"connect_timeout": 30})


def chunk_raw_data(df, n_chunks=N_CHUNKS, random_state=42):
    # Stratified by category so every batch has every sheet. Only the sheet to
    # category mapping is used here, the data itself stays raw.
    df = df.copy()
    df["scrape_batch_id"] = -1

    strat_col = df["source_sheet"].map(CATEGORY_MAP)
    for category in strat_col.unique():
        cat_idx = df[strat_col == category].index
        shuffled = pd.Series(cat_idx).sample(frac=1, random_state=random_state).values
        for batch_num, idx_group in enumerate(np.array_split(shuffled, n_chunks), start=1):
            df.loc[idx_group, "scrape_batch_id"] = batch_num
    return df


def get_pending_batches(engine):
    # a batch counts as loaded only if it has a log row with no error
    with engine.connect() as conn:
        done = pd.read_sql(
            text("SELECT DISTINCT scrape_batch_id FROM pipeline_run_log WHERE errors IS NULL"),
            conn,
        )["scrape_batch_id"].tolist()
    return [b for b in range(1, N_CHUNKS + 1) if b not in done]


def process_chunk(df_chunked, batch_id, engine, vectorizer, centroids):
    start_time = time.time()
    errors = None
    cleaned = None

    chunk = df_chunked[df_chunked["scrape_batch_id"] == batch_id].copy()
    rows_in = len(chunk)

    # cross-batch dedup: skip SKUs an earlier batch already put in clean_products
    with engine.connect() as conn:
        existing_skus = pd.read_sql(text("SELECT sku FROM clean_products"), conn)["sku"].tolist()
    before = len(chunk)
    chunk = chunk[~chunk["SKU"].isin(existing_skus)]
    if before - len(chunk) > 0:
        print(f"batch {batch_id}: {before - len(chunk)} rows dropped, SKU already loaded by an earlier batch")

    try:
        # one transaction for both writes, so a failure leaves neither table half-loaded
        with engine.begin() as conn:
            raw_to_write = chunk.rename(columns=RAW_COLUMN_MAP)
            raw_cols = list(RAW_COLUMN_MAP.values()) + ["row_id", "scrape_batch_id"]
            raw_to_write[raw_cols].to_sql("raw_staging", conn, if_exists="append", index=False)

            cleaned = clean_batch(chunk, vectorizer, centroids)
            rows_cleaned = len(cleaned)
            rows_flagged = int(cleaned["is_contamination_flagged"].sum())
            rows_dropped = rows_in - rows_cleaned

            clean_to_write = cleaned.rename(columns=CLEAN_COLUMN_MAP)
            clean_cols = list(CLEAN_COLUMN_MAP.values()) + ["row_id", "scrape_batch_id"]
            clean_to_write["scrape_batch_id"] = batch_id
            clean_to_write[clean_cols].to_sql("clean_products", conn, if_exists="append", index=False)
    except Exception as e:
        errors = str(e)
        rows_cleaned = 0
        rows_flagged = 0
        rows_dropped = rows_in

    duration_seconds = round(time.time() - start_time, 2)

    # the log row is written whether the batch worked or not
    log_row = pd.DataFrame([{
        "scrape_batch_id": batch_id,
        "rows_in": rows_in,
        "rows_cleaned": rows_cleaned,
        "rows_flagged_contamination": rows_flagged,
        "rows_dropped": rows_dropped,
        "duration_seconds": duration_seconds,
        "errors": errors,
    }])
    log_row.to_sql("pipeline_run_log", engine, if_exists="append", index=False)

    status = "Failed" if errors else "Ok"
    print(f"batch {batch_id}: {status} | in={rows_in} cleaned={rows_cleaned} "
          f"flagged={rows_flagged} dropped={rows_dropped} duration={duration_seconds}s")
    if errors:
        print(f"  error: {errors}")
    return errors is None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="load every pending batch, not just the next one")
    args = parser.parse_args()

    engine = get_engine()
    pending = get_pending_batches(engine)
    if not pending:
        print(f"All {N_CHUNKS} batches already loaded, nothing to do.")
        return 0

    to_run = pending if args.all else pending[:1]
    print(f"Pending batches: {pending}. Running: {to_run}")

    df_raw = load_raw()
    # fit the contamination reference once on the full raw data, never per batch
    vectorizer, centroids = fit_contamination_reference(df_raw)
    df_chunked = chunk_raw_data(df_raw)

    for batch_id in to_run:
        ok = process_chunk(df_chunked, batch_id, engine, vectorizer, centroids)
        if not ok:
            return 1  # non-zero exit makes the GitHub Actions run show as failed
    return 0


if __name__ == "__main__":
    sys.exit(main())
