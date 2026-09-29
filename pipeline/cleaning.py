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
