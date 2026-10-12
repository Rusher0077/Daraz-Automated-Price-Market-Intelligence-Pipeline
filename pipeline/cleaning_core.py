# cleaning_core.py
# The importable version of my cleaning functions. No test cells here.
# cleaning.py is my scratchpad, any function fix goes in this file only.

from pathlib import Path

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

RAW_PATH = Path(__file__).resolve().parent.parent / "data" / "processed" / "raw_combined.csv"

# Daraz's own Category field is sometimes wrong, so category_normalized comes
# from source_sheet, the page each product was actually scraped from.
CATEGORY_MAP = {
    "Keyboard": "Keyboards",
    "Feature Phone": "Feature Phones",
    "Toys": "Toys",
    "HDD": "Hard Drives",
    "Softwares": "Software",
    "Laptops": "Laptops",
    "AC": "Air Conditioners",
    "Kitchen Appliance": "Kitchen Appliances",
    "Fridge": "Refrigerators",
    "TV": "Televisions",
    "Sunglass": "Sunglasses",
    "Cloth": "Clothing",
    "Shirt": "Clothing",
    "Shoes": "Shoes",
    "Handbag": "Handbags",
    "Books": "Books",
}

COLUMNS_TO_DROP = [
    "PAGE URL",
    "Offer URL",
    "URL",
    "Seller",
    "Brand · Url",
    "Offers · Has Merchant Return Policy · Applicable Country",
    "Offers · Has Merchant Return Policy · Return Policy Category",
    "Type",
    "Description",
]

# One threshold does not fit every sheet, see the consolidation notebook.
DEFAULT_THRESHOLD = 0.15
SHEET_THRESHOLDS = {
    "TV": 0.20,
    "Laptops": 0.18,
    "Softwares": 0.08,
    "Books": 0.08,
}

# AC contamination overlaps real AC vocabulary, so these row_ids were
# confirmed by hand and flagged on top of the score check.
KNOWN_CONTAMINATION_IDS = {
    "AC": [3021, 3010, 2879, 3006, 2994, 3033, 3038, 3078, 3076, 3071, 3058, 2877,
           3049, 3041, 2931, 2925, 2924, 2916, 2908, 2903, 2978, 2975, 2961, 2952, 2972],
}


def load_raw(path=RAW_PATH):
    # "—" is the raw missing-value placeholder, utf-8-sig keeps the Taka sign intact
    return pd.read_csv(path, encoding="utf-8-sig", na_values=["—"])


def assign_categories(df, mapping=CATEGORY_MAP):
    df = df.copy()
    df["category_normalized"] = df["source_sheet"].map(mapping)

    # a sheet missing from the mapping should stop the run, not slip through as NaN
    unmapped = df.loc[df["category_normalized"].isna(), "source_sheet"].unique()
    if len(unmapped) > 0:
        raise ValueError(f"source_sheet values not in the mapping: {list(unmapped)}")

    # last breadcrumb segment, rows with no Category stay NaN on purpose
    leaf = df["Category"].str.split(">").str[-1].str.strip()
    df["category_leaf"] = leaf.replace("", pd.NA)
    return df


def clean_prices(df):
    df = df.copy()

    def to_number(series):
        return (
            series.astype(str)
            .str.replace("৳", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
            .replace("nan", pd.NA)
            .astype(float)
        )

    df["Price"] = to_number(df["Price"])
    df["Original_Price"] = to_number(df["Original_Price"])

    # -0.28 becomes 28
    df["Discount_Perc"] = df["Discount_Perc"].abs() * 100

    # set before filling, so "no discount" stays distinct from "not filled in"
    df["is_discounted"] = df["Discount_Perc"].notna() & (df["Discount_Perc"] > 0)

    missing_or_zero = (
        df["Original_Price"].isna() | df["Discount_Perc"].isna() | (df["Discount_Perc"] == 0)
    )
    df.loc[missing_or_zero, "Original_Price"] = df.loc[missing_or_zero, "Original_Price"].fillna(
        df.loc[missing_or_zero, "Price"]
    )
    df.loc[df["Original_Price"].isna(), "Original_Price"] = df.loc[
        df["Original_Price"].isna(), "Price"
    ]
    df["Discount_Perc"] = df["Discount_Perc"].fillna(0)
    return df


def clean_text_fields(df):
    df = df.copy()

    df["sold_by"] = (
        df["sold_by"].astype(str)
        .str.replace("Sold by", "", regex=False)
        .str.strip()
        .replace("nan", pd.NA)
    )
    df["Positive Seller Ratings"] = pd.to_numeric(
        df["Positive Seller Ratings"].astype(str)
        .str.replace("Positive Seller Ratings", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip(),
        errors="coerce",
    )
    df["ship_on_time"] = pd.to_numeric(
        df["ship_on_time"].astype(str)
        .str.replace("Ship on Time", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip(),
        errors="coerce",
    )
    df["seller_rating"] = pd.to_numeric(
        df["seller_rating"].astype(str).str.replace("/5", "", regex=False).str.strip(),
        errors="coerce",
    )
    # rating holds the review count
    df["rating"] = pd.to_numeric(
        df["rating"].astype(str).str.replace("Ratings", "", regex=False).str.strip(),
        errors="coerce",
    )
    return df


def drop_broken_rows(df):
    # scrape failures: no SKU, no Price, no Category
    df = df.copy()
    before = len(df)
    broken = df["SKU"].isna() & df["Price"].isna() & df["Category"].isna()
    df = df[~broken]
    print(f"drop_broken_rows: {before - len(df)} broken rows dropped, {len(df)} remain")
    return df


def dedup_by_sku(df):
    # keep="first" picks whichever sheet was combined first, not necessarily
    # the best-fit category. Accepted imprecision, same call as AC/Fridge.
    df = df.copy()
    before = len(df)
    df = df.drop_duplicates(subset="SKU", keep="first")
    print(f"dedup_by_sku: {before - len(df)} duplicate rows dropped, {len(df)} remain")
    return df


def drop_unused_columns(df, columns=COLUMNS_TO_DROP):
    df = df.copy()
    existing = [c for c in columns if c in df.columns]
    missing = [c for c in columns if c not in df.columns]
    if missing:
        print(f"drop_unused_columns: already missing, skipped: {missing}")
    return df.drop(columns=existing)


def fit_contamination_reference(df):
    # run once on the full raw data (with Description), never refit per chunk
    text = df["Name"].fillna("") + " " + df["Description"].fillna("")
    vectorizer = TfidfVectorizer(stop_words="english", max_features=5000)
    tfidf_matrix = vectorizer.fit_transform(text)

    centroids = {}
    for sheet in df["source_sheet"].unique():
        sheet_mask = (df["source_sheet"] == sheet).values
        centroid = tfidf_matrix[sheet_mask].mean(axis=0)
        centroids[sheet] = pd.Series(centroid.tolist()[0]).values
    return vectorizer, centroids


def flag_category_mismatch(df, vectorizer, centroids,
                           default_threshold=DEFAULT_THRESHOLD,
                           sheet_thresholds=SHEET_THRESHOLDS,
                           known_contamination_ids=KNOWN_CONTAMINATION_IDS):
    df = df.copy()

    # transform only, so every chunk uses the same vocabulary the thresholds were set on
    text = df["Name"].fillna("") + " " + df["Description"].fillna("")
    tfidf_matrix = vectorizer.transform(text)

    scores = pd.Series(index=df.index, dtype=float)
    for sheet in df["source_sheet"].unique():
        if sheet not in centroids:
            continue
        sheet_mask = (df["source_sheet"] == sheet).values
        sims = cosine_similarity(tfidf_matrix[sheet_mask], [centroids[sheet]])
        scores.loc[df.index[sheet_mask]] = sims.ravel()
    df["category_match_score"] = scores

    threshold_per_row = df["source_sheet"].map(sheet_thresholds).fillna(default_threshold)
    score_flag = df["category_match_score"] < threshold_per_row

    override_flag = pd.Series(False, index=df.index)
    for sheet, ids in known_contamination_ids.items():
        override_flag |= df["row_id"].isin(ids) & (df["source_sheet"] == sheet)

    df["is_contamination_flagged"] = score_flag | override_flag
    return df


def clean_batch(df, vectorizer, centroids):
    # flag_category_mismatch needs Description, so it runs before drop_unused_columns
    df = assign_categories(df)
    df = clean_prices(df)
    df = clean_text_fields(df)
    df = drop_broken_rows(df)
    df = dedup_by_sku(df)
    df = flag_category_mismatch(df, vectorizer, centroids)
    df = drop_unused_columns(df)
    return df