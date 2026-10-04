# %%
# cleaning.py
# Reusable cleaning functions for the Daraz pipeline.
# I'm building this one function at a time with # %% cells so I can test each
# step on the real data before it becomes part of the pipeline.

import pandas as pd
RAW_PATH = ("../data/processed/raw_combined.csv")

# %%
# Step 0: load the raw data
# The raw file uses the literal string "—" for missing values, so I tell pandas
# to treat it as NaN right at read time. utf-8-sig keeps the Taka sign and
# other special characters intact.

def load_raw(path=RAW_PATH):
    return pd.read_csv(path, encoding="utf-8-sig", na_values=["—"])

df_raw = load_raw()
print(df_raw.shape) #(8287, 27)

# %%
# Step 1: category assignment
# Daraz's own Category field is sometimes wrong (an AC cover filed under
# Laundry & Cleaning), so category_normalized comes from source_sheet, which
# tells me the page each product was actually scraped from.
# category_leaf is kept as a separate column from the raw Category breadcrumb,
# because it sometimes adds real detail (accessory vs core product).
# Shirt and Cloth both map to Clothing. source_sheet stays in the data, so I
# can split them again later if I need to.

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

def assign_categories(df, mapping = CATEGORY_MAP):
    df = df.copy()
    df["category_normalized"] = df["source_sheet"].map(mapping)

    # a sheet missing from the mapping should stop the run, not slip through as NaN
    unmapped = df.loc[df["category_normalized"].isna(), "source_sheet"].unique()
    if len(unmapped) > 0:
        raise ValueError(f"source_sheet values not in the mapping : {list(unmapped)}")

    # last breadcrumb segment. Rows with no Category stay NaN on purpose.
    leaf = df["Category"].str.split(">").str[-1].str.strip()
    df["category_leaf"] = leaf.replace("", pd.NA)

    return df


# %%
# Test: assign_categories on the full raw data

df_test = assign_categories(df_raw)

print(df_test["category_normalized"].value_counts())
print("total mapped: ", df_test["category_normalized"].notna().sum())
print("null category leaf: ", df_test["category_leaf"].isna().sum())

print(df_test[["source_sheet", "Category", "category_normalized", "category_leaf"]].sample(10, random_state=1))
# %%
# Step 2: price and discount cleaning
# Price and Original_Price come in like "৳ 51,990", so I'll strip the currency
# symbol and commas before converting to float.
# Discount_Perc is a negative decimal (-0.28) so I'll flip it to a positive
# percentage (28).
# is_discounted is set BEFORE filling missing values, so I don't lose the
# difference between "no discount" and "discount data wasn't filled in".
# A literal 0% discount counts as not discounted too, same as a NaN.

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

    # flip sign, -0.28 becomes 0.28, then to a percentage scale, 28
    df["Discount_Perc"] = df["Discount_Perc"].abs() * 100

    # is_discounted: True only where a real, nonzero discount exists
    df["is_discounted"] = df["Discount_Perc"].notna() & (df["Discount_Perc"] > 0)

    # now fill the non-discounted rows
    missing_or_zero = df["Original_Price"].isna() | df["Discount_Perc"].isna() | (df["Discount_Perc"] == 0)
    df.loc[missing_or_zero, "Original_Price"] = df.loc[missing_or_zero, "Original_Price"].fillna(df.loc[missing_or_zero, "Price"])
    df.loc[df["Original_Price"].isna(), "Original_Price"] = df.loc[df["Original_Price"].isna(), "Price"]
    df["Discount_Perc"] = df["Discount_Perc"].fillna(0)

    return df
# %%
# Test: clean_prices, chained off the category step's output

df_priced = clean_prices(df_test)

print(df_priced[["Price", "Original_Price", "Discount_Perc", "is_discounted"]].describe())
print("is_discounted value counts:")
print(df_priced["is_discounted"].value_counts())
print("null Price after cleaning:", df_priced["Price"].isna().sum())
print("null Original_Price after cleaning:", df_priced["Original_Price"].isna().sum())

df_priced[["Price", "Original_Price", "Discount_Perc", "is_discounted"]].sample(10, random_state=1)

# %%
# Step 3: string-parsing cleanup
# A few trust-signal fields come in with literal prefix/suffix text baked into
# the string, found during real data inspection, not in the original plan.
#   sold_by               "Sold byHaier"            -> "Haier"
#   Positive Seller Ratings "Positive Seller Ratings90%" -> 90.0
#   ship_on_time          "Ship on Time100%"        -> 100.0
#   seller_rating         "4.9/5"                   -> 4.9
#   rating                "270 Ratings"              -> 270  (this is review count)
# Some sellers show "Not enough data" instead of a number, no history yet.
# pd.to_numeric with errors="coerce" turns that into NaN instead of crashing.

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
        df["seller_rating"].astype(str)
        .str.replace("/5", "", regex=False)
        .str.strip(),
        errors="coerce",
    )

    df["rating"] = pd.to_numeric(
        df["rating"].astype(str)
        .str.replace("Ratings", "", regex=False)
        .str.strip(),
        errors="coerce",
    )

    return df



# %%
df_clean_text = clean_text_fields(df_priced)

cols = ["sold_by", "Positive Seller Ratings", "ship_on_time", "seller_rating", "rating"]
print(df_clean_text[cols].describe(include="all"))
print()
for c in cols:
    print(c, "nulls:", df_clean_text[c].isna().sum())

df_clean_text[cols].sample(10, random_state=1)


# %%
# Step 4a: drop known broken rows
# These are scrape failures confirmed from memory during scraping itself,
# not duplicates. No SKU, no Price, no Category, nothing to work with.
# Dropped explicitly here rather than relying on dedup's NaN behavior to
# remove them as a side effect.

def drop_broken_rows(df):
    df = df.copy()
    before = len(df)

    broken = df["SKU"].isna() & df["Price"].isna() & df["Category"].isna()
    df = df[~broken]

    print(f"drop_broken_rows: {before - len(df)} broken rows dropped, {len(df)} remain")
    return df

# %%
# Step 4b: deduplicate by SKU
# Dedup runs across the full combined dataset, not per sheet or per chunk,
# since the same product could in theory show up more than once anywhere
# in the scrape. I keep the first occurrence and log how many got dropped.

def dedup_by_sku(df):
    # Found 28 cross-listed products (same SKU, same listing, scraped from two
    # different category pages, e.g. a laptop charger appearing under both
    # Keyboard and Laptops). keep="first" picks whichever sheet got combined
    # first, not necessarily the better-fit category. Leaving this as is,
    # same acceptable-imprecision call as the AC/Fridge contamination scope.
    df = df.copy()

    before = len(df)
    df = df.drop_duplicates(subset="SKU", keep="first")
    after = len(df)

    print(f"dedup_by_sku: {before - after} duplicate rows dropped, {after} remain")
    return df


# %%
# Test: dedup_by_sku, chained off the text-cleaning step's output

df_no_broken = drop_broken_rows(df_clean_text)
df_deduped = dedup_by_sku(df_no_broken)

print("final row count:", len(df_deduped))

#%%
# Step 5: drop columns not needed in the analysis dataset
# These are kept in raw_staging (the untouched raw_combined feeds that), but
# dropped here since they add no analytical value, per requirements.md
# Section 7. Description is dropped from the analysis dataset only.

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

def drop_unused_columns(df, columns = COLUMNS_TO_DROP):
    df = df.copy()
    existing = [c for c in columns if c in df.columns]
    missing = [c for c in columns if c not in df.columns]

    if missing:
        print(f"drop_unsued_columns: These were already missing, skipped:{missing}")

    return df.drop(columns=existing)


# %%
# Test: drop_unused_columns, chained off the dedup step's output

df_final = drop_unused_columns(df_deduped)

print("Columns before: ", df_deduped.shape[1]) 
print("Columns after: ", df_final.shape[1])
print(df_final.columns.tolist())
# %%
# Step 6: TF-IDF category mismatch flagging
# Vectorize Name + Description across the whole dataset so every sheet shares
# one vocabulary space, build a centroid per source_sheet from its own rows,
# then score every row by cosine similarity to its own sheet's centroid.
# Low score means the product's text doesn't look like the rest of its sheet,
# a likely contamination or cross-domain accessory.
#
# A single global threshold doesn't work, confirmed in the notebook:
#   - TV and Laptops separate cleanly at their own higher thresholds (0.20, 0.18)
#   - Software and Books sit on a naturally lower baseline (unrelated legit
#     titles share little vocabulary), so they get lower thresholds (0.08)
#   - AC's contamination overlaps real AC vocabulary (car AC parts), so no
#     score cutoff separates it, handled instead with a manual override list
#     of row_ids confirmed as contamination by direct product inspection
#   - everything else uses the 0.15 default
#
# Note: this function is meant to run on raw_staging data per chunk, not on
# already-cleaned clean_products, per requirements.md Section 8. Description
# was already dropped from df_final earlier in this pipeline, so this
# function needs to be called on data that still has it, before
# drop_unused_columns, or Description needs to be carried separately into
# raw_staging for this step specifically.

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

DEFAULT_THRESHOLD = 0.15

SHEET_THRESHOLDS = {
    "TV": 0.20,
    "Laptops": 0.18,
    "Softwares": 0.08,
    "Books": 0.08,
}

KNOWN_CONTAMINATION_IDS = {
    "AC": [3021, 3010, 2879, 3006, 2994, 3033, 3038, 3078, 3076, 3071, 3058, 2877,
           3049, 3041, 2931, 2925, 2924, 2916, 2908, 2903, 2978, 2975, 2961, 2952, 2972],
}


def flag_category_mismatch(df, default_threshold=DEFAULT_THRESHOLD,
                            sheet_thresholds=SHEET_THRESHOLDS,
                            known_contamination_ids=KNOWN_CONTAMINATION_IDS):
    df = df.copy()

    text = (df["Name"].fillna("") + " " + df["Description"].fillna(""))

    vectorizer = TfidfVectorizer(stop_words="english")
    tfidf_matrix = vectorizer.fit_transform(text)

    scores = pd.Series(index=df.index, dtype=float)

    for sheet in df["source_sheet"].unique():
        sheet_mask = df["source_sheet"] == sheet
        sheet_vectors = tfidf_matrix[sheet_mask.values]

        centroid = sheet_vectors.mean(axis=0)
        centroid = pd.Series(centroid.tolist()[0])  # convert matrix row to array-like

        sims = cosine_similarity(sheet_vectors, [centroid.values])
        scores.loc[sheet_mask] = sims.ravel()

    df["category_match_score"] = scores

    # score-based flag, per-sheet threshold where set, default otherwise
    threshold_per_row = df["source_sheet"].map(sheet_thresholds).fillna(default_threshold)
    score_flag = df["category_match_score"] < threshold_per_row

    # manual override, on top of the score flag, not instead of it
    override_flag = pd.Series(False, index=df.index)
    for sheet, ids in known_contamination_ids.items():
        override_flag |= df["row_id"].isin(ids) & (df["source_sheet"] == sheet)

    df["is_contamination_flagged"] = score_flag | override_flag

    return df


# %%
# Test: flag_category_mismatch, chained off the dedup step's output, 
# then drop_unused_columns, chained off the flagging step's output

df_flagged = flag_category_mismatch(df_deduped)
df_final = drop_unused_columns(df_flagged)

print("Columns before:", df_flagged.shape[1])
print("Columns after:", df_final.shape[1])
print(df_final.columns.tolist())
print()
print("total flagged:", df_final["is_contamination_flagged"].sum())
print("flag rate:", round(df_final["is_contamination_flagged"].mean() * 100, 1), "%")


# %%
print(df_final.groupby("source_sheet")["is_contamination_flagged"].mean().sort_values(ascending=False) * 100)
# %%
# Step 7: the pipeline entry point
# Chains every cleaning step in the correct order. This is what actually runs
# per chunk once chunking/staging is wired up, not just a test harness like
# the cells above it.
# Order matters: flag_category_mismatch needs Description, so it has to run
# before drop_unused_columns, which removes it.

def clean_batch(df):
    df = assign_categories(df)
    df = clean_prices(df)
    df = clean_text_fields(df)
    df = drop_broken_rows(df)
    df = dedup_by_sku(df)
    df = flag_category_mismatch(df)
    df = drop_unused_columns(df)
    return df
# %%
# Test: clean_batch end to end, starting from the raw load, not from any
# intermediate df_ variable, to confirm the whole chain works standalone

df_raw_test = load_raw()
df_pipeline_result = clean_batch(df_raw_test)

print("input rows:", len(df_raw_test))
print("output rows:", len(df_pipeline_result))
print("output columns:", df_pipeline_result.shape[1])
print(df_pipeline_result.columns.tolist())
print()
print("total flagged:", df_pipeline_result["is_contamination_flagged"].sum())
print("flag rate:", round(df_pipeline_result["is_contamination_flagged"].mean() * 100, 1), "%")
# %%
