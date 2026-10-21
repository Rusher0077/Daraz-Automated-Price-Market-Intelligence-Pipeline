# Automated Price & Market Intelligence Pipeline (Daraz BD)

## Project Overview

An end-to-end data pipeline built on **8,287** real product listings scraped from Daraz Bangladesh. The data arrives in 7 simulated daily batches, gets cleaned and validated by a reusable Python pipeline (including a TF-IDF check for products filed under the wrong category), lands in a cloud PostgreSQL database on Neon, and feeds a two-page Power BI dashboard. A GitHub Actions workflow triggers the pipeline on a schedule, so batches load without anyone running a script.

On top of the pipeline sits a market intelligence analysis of pricing, discounting, ratings and brand positioning across 15 product categories.

**Stack:** Python (pandas, scikit-learn, SQLAlchemy) · PostgreSQL (Neon) · GitHub Actions · Power BI · PandaExtract

**Live dashboard:** [View the published Power BI report](https://app.powerbi.com/view?r=eyJrIjoiNjE3NTBhNGYtZjg0ZS00ZGQyLWIxZTUtOGM2ZTVlODY5OWRjIiwidCI6IjlhMjdkMTllLTQ0MjEtNDRmYi1hY2Q4LWZkNTZjOWZiYTJkOCIsImMiOjEwfQ%3D%3D&pageName=8ab4b6239fe095483ff2)

---

## Results at a Glance

| Metric | Value |
|---|---|
| Rows ingested | **8,287** across 4 workbooks and 16 scraped sheets |
| Rows cleaned and stored | **8,252** |
| Rows dropped | **35** (7 failed scrapes, 28 duplicate SKUs) |
| Rows flagged as category contamination | **1,650 (20.0%)** |
| Batches loaded | **7**, stratified by category |
| Categories analysed | **15** |
| Pipeline run time | about **40 seconds** per batch on GitHub Actions |

Flagged rows are kept in `clean_products` with a boolean flag rather than deleted, so the analysis can include or exclude them on purpose.

---

## Dataset

Product listings from [Daraz Bangladesh](https://www.daraz.com.bd), scraped with the PandaExtract browser extension. Four workbooks, 16 sheets:

| Workbook | Sheets |
|---|---|
| Electronics | Keyboard, Feature Phone, Toys, HDD, Softwares, Laptops |
| Home Appliances | AC, Kitchen Appliance, Fridge, TV |
| Fashion | Sunglass, Cloth, Shirt, Shoes, Handbag |
| Books | Books |

Each row carries 24 raw columns: name, description, brand, category breadcrumb, SKU, price, original price, discount, seller stats, and rating fields. The HDD sheet was stopped at 200 rows on purpose, because past that point the category page started surfacing unrelated products.

This is a one-time scrape, not a live feed. The "daily" arrival is simulated by chunking the data (see Pipeline Execution).

---

## Architecture

```
Scraped workbooks (4 files, 16 sheets, 8,287 rows)
        |
        | CONSOLIDATION (notebooks/data_consolidation.ipynb)
        v
raw_combined.csv  (row_id added, source_sheet and workbook tagged)
        |
        | CHUNKING, stratified by category, 7 batches
        v
Neon PostgreSQL
  raw_staging   (append-only, exactly as received)
        |
        | PYTHON PIPELINE (pipeline/)
        |   parse prices and discounts, clean text fields
        |   drop broken rows, dedup by SKU (across batches)
        |   assign categories, flag contamination (TF-IDF)
        v
  clean_products        pipeline_run_log
  (one row per product) (one row per batch run)
        |
        | TRIGGER
        v
GitHub Actions (scheduled + manual) runs pipeline/run_batch.py
        |
        v
Power BI (Import mode, manual refresh)
  Page 1: Pipeline health
  Page 2: Market intelligence
```

---

## Data Model

Three tables on Neon, following a raw, clean, observability pattern. Full definitions are in [`sql/schema.sql`](sql/schema.sql).

| Table | Purpose |
|---|---|
| `raw_staging` | Untouched data as received from each batch. Price and discount fields stay as text on purpose. |
| `clean_products` | Pipeline output. Numeric prices, `category_normalized`, `category_leaf`, `is_discounted`, `category_match_score`, `is_contamination_flagged`, `scrape_batch_id`. |
| `pipeline_run_log` | One row per batch run: rows in, rows cleaned, rows flagged, rows dropped, duration, errors. |

Nulls in `clean_products` are real absences, not unfinished cleaning. Most listings have no ratings yet, and new sellers have no seller stats, so these fields are left null instead of imputed.

---

## Cleaning Rules

| Field | Raw example | What the pipeline does |
|---|---|---|
| `Price`, `Original_Price` | `৳ 51,990` | Strip the currency symbol and commas, convert to numeric |
| `Discount_Perc` | `-0.28` | Convert to a positive percentage (28) |
| Missing discount | empty | Set `is_discounted = false` first, then `Original_Price = Price` and `Discount_Perc = 0` |
| `sold_by` | `Sold byHaier` | Strip the prefix text |
| `Positive Seller Ratings`, `ship_on_time` | `Ship on Time100%` | Strip prefix and `%`, convert with `errors="coerce"` so "Not enough data" and "New Seller" become null |
| `seller_rating` | `4.9/5` | Strip `/5`, convert to float |
| `rating` | `270 Ratings` | Strip the suffix, convert to int (this column is the review count) |
| Missing values | `—` | Read the em dash placeholder as null |

Rows with no SKU, no price and no category are genuine scrape failures and are dropped explicitly.

---

## Category Handling

Daraz's own `Category` field turned out to be unreliable. An air conditioner cleaning cover, for example, was filed under `Laundry & Cleaning > Cleaning`. Mapping the raw values cleanly could not fix a field that is sometimes just wrong.

So the pipeline uses two separate columns:

- **`category_normalized`** comes from `source_sheet`, the page the product was actually scraped from (Shirt and Cloth are merged into one Clothing label, giving 15 categories).
- **`category_leaf`** is the last segment of Daraz's own breadcrumb, kept as an independent column. It is sometimes more specific (an AC accessory shows as "Cleaning") and sometimes just repeats the category. Both are fine.

They are not concatenated, because strings like "Feature Phones > Mobiles & Tablets" read backwards.

---

## Contamination Detection (TF-IDF)

Scraped category pages sometimes surface unrelated products, such as laser pointers and AirPods cases in the TV sheet. The pipeline scores every row by TF-IDF cosine similarity between its name and description and the centroid of its own sheet, then flags low scores.

One global threshold did not work, because sheets behave differently:

| Pattern | Sheets | What happens | How it is handled |
|---|---|---|---|
| Vocabulary-distinct contamination | TV, Laptops | Contamination shares almost no words with real listings, so scores split into two clear clusters | Threshold set at the histogram valley (0.20 and 0.18) |
| Vocabulary-overlapping contamination | AC (and probably Fridge) | Car AC accessories reuse words like "air conditioner" and "cooling", so scores overlap with real products. Known AC contamination scored between 0.07 and 0.38, nearly the full range | Manual override list of 25 reviewed `row_id` values on top of the score |
| Naturally low baseline similarity | Software, Books | Legitimate titles differ a lot from each other, so scores sit low without any contamination | Lower threshold (0.08) |

Everything else uses a default threshold of 0.15. These values were calibrated by reading histograms and sampling products by hand, which is a deliberate scope choice for a portfolio project.

The check also separates contamination from legitimate accessories. A cover made specifically for air conditioners is kept. A universal remote that lists AC as one of five supported devices is treated as contamination.

---

## Pipeline Execution

The combined dataset is split into **7 chunks, stratified by category**, so every category appears from the first batch onward instead of clumping. Each chunk stands in for one day of arrival.

For each batch the pipeline:

1. Writes the raw chunk to `raw_staging`
2. Filters out SKUs already present in `clean_products`, so duplicates across batches are caught
3. Runs the cleaning steps and the contamination flag
4. Writes the result to `clean_products`
5. Logs the run to `pipeline_run_log`, whether it succeeded or failed

### Pipeline files

| File | Role |
|---|---|
| `pipeline/cleaning_core.py` | The reusable cleaning functions, with no database calls |
| `pipeline/run_pipeline.py` | Orchestration for one batch: chunk handling, Neon writes, run logging |
| `pipeline/run_batch.py` | Entry point that loads one batch into `raw_staging` and runs it |
| `.github/workflows/run_pipeline.yml` | Triggers `run_batch.py` on a schedule or by hand |

### GitHub Actions

![GitHub Actions run history](media/screenshots/github_actions_runs.png)

The workflow runs **four times a day** (03:17, 09:17, 15:17 and 21:17, Asia/Dhaka time) and can also be started manually. It has a `load_all` option that loads every pending batch in one go. A concurrency group prevents two runs from overlapping, and the database connection string is stored as a GitHub secret.

The run history shows 8 runs. Run #1 was a test to confirm the trigger worked: all batches were already loaded from earlier manual runs, so it correctly loaded nothing. Runs #2 to #8 loaded the 7 batches, with #2 triggered manually and #3 to #8 running on the schedule. Each run finished in under 3 minutes, most in about 40 seconds.

---

## Market Intelligence Analysis

The full analysis is in [`notebooks/eda.ipynb`](notebooks/eda.ipynb), grouped by `category_normalized`.

**Method choices**

- Medians instead of means for prices, because price distributions here are heavily skewed
- Key statistics checked with and without flagged rows before deciding what to exclude
- Headline price charts use main products only, with flagged rows and accessories excluded, and the definition is stated on the chart
- A rating counts as real only when reviews are at least 1 and stars are above 0
- Discount depth is averaged among discounted products only
- Every rating-based number shows its sample size next to it, with a minimum rated count per category

<!-- TODO: add 4 to 5 findings from eda.ipynb with bold key numbers. Topics: price positioning, discount behavior, rating vs review count, discount depth vs rating, value ranking. -->

---

## Power BI Dashboard

Two pages, connected to Neon in Import mode (manual refresh). [Open the published report](https://app.powerbi.com/view?r=eyJrIjoiNjE3NTBhNGYtZjg0ZS00ZGQyLWIxZTUtOGM2ZTVlODY5OWRjIiwidCI6IjlhMjdkMTllLTQ0MjEtNDRmYi1hY2Q4LWZkNTZjOWZiYTJkOCIsImMiOjEwfQ%3D%3D&pageName=8ab4b6239fe095483ff2) or open [`dashboard/Dashboard.pbix`](dashboard/Dashboard.pbix) locally.

**Page 1: Pipeline health.** Ingestion volume, cleaning pass rate and contamination rate per batch, cumulative rows loaded, and flag rate by category.

![Pipeline health page](media/screenshots/pbi_pipeline_health.png)

**Page 2: Market intelligence.** The findings from the EDA notebook.

![Market intelligence page](media/screenshots/pbi_market_intelligence.png)

---

## Key Design Decisions

- **`source_sheet` is the category source of truth.** Daraz's category field is wrong often enough that no mapping could repair it, so the page a product was scraped from decides its category.
- **The TF-IDF vectorizer is fitted once, not per batch.** An early version refitted it on every chunk, which quietly changed the IDF weights and made the calibrated thresholds meaningless. Batch 1 flagged a different share than the full dataset because of it. The fix was to fit the vectorizer and sheet centroids once on the full raw data and only call `transform` per batch. After the fix, batch 1 flagged 19.4% against 20.0% overall.
- **Deduplication looks across batches.** Dedup inside a single chunk cannot catch a product whose two copies land in different batches, so each incoming chunk is filtered against SKUs already in `clean_products`.
- **Flagging is not deleting.** Flagged rows stay in `clean_products` with `is_contamination_flagged`. What to exclude is decided in the analysis, not hidden inside the pipeline.
- **Raw data is never cleaned before chunking.** If contamination were removed up front, every pipeline run would find none, and the `rows_flagged_contamination` metric would mean nothing.
- **A tested simplification was rolled back.** Adding `max_features=5000` to the vectorizer dropped the flagged count from 1,650 to 1,363, because the thresholds assume the full vocabulary. The vectorizer stays without it.

---

## Known Limitations

- Thresholds were calibrated on the full combined dataset by reading histograms and sampling by hand, not tuned rigorously for each of the 16 sheets.
- Fridge shows the same overlapping-vocabulary pattern as AC but was not fully investigated and has no override list.
- 28 products were listed under two category pages. The pipeline keeps the copy from whichever sheet was combined first, which is not always the better-fitting category.
- Some `ship_on_time` values are actually "Chat Response Rate", scraped into the same column position by the extension. 60 of the 161 nulls in that column come from this.
- Only about 12% of products have ratings, so rating-based findings rest on a smaller subset than price findings.
- The data is a single scrape. There is no real time dimension, so this project cannot show price changes over time.
- Power BI refresh is manual. Scheduled refresh would need Power BI Pro and a gateway.

---

## Repository Structure

```
Daraz-Automated-Price-Market-Intelligence-Pipeline/
├── .github/
│   └── workflows/
│       └── run_pipeline.yml     # GitHub Actions workflow that runs the pipeline on a schedule
├── dashboard/
│   └── Dashboard.pbix           # Power BI dashboard (2 pages)
├── data/
│   ├── raw/                     # original scraped workbooks (not tracked in git)
│   │   ├── Books.xlsx
│   │   ├── Electronics.xlsx
│   │   ├── Fashion.xlsx
│   │   └── Home Appliances.xlsx
│   └── processed/               # combined and mapped files (not tracked in git)
│       ├── category_value_counts.csv
│       ├── map.csv
│       └── raw_combined.csv
├── media/
│   └── screenshots/             # dashboard pages and GitHub Actions run history
├── notebooks/
│   ├── data_consolidation.ipynb # combining the workbooks and designing the contamination check
│   └── eda.ipynb                # market intelligence analysis
├── pipeline/
│   ├── cleaning_core.py         # reusable cleaning functions
│   ├── cleaning.py              # scratchpad where the cleaning functions were first built and tested
│   ├── run_batch.py             # loads one batch into raw_staging
│   └── run_pipeline.py          # runs the full pipeline on a batch
├── sql/
│   └── schema.sql               # table definitions for Neon
├── .gitattributes
├── .gitignore
├── requirements.txt             # Python dependencies
└── README.md
```

---

## Running This Project

Credentials and the raw data are not included in this repository. To run it yourself:

1. Create a free [Neon](https://neon.tech) PostgreSQL project and run [`sql/schema.sql`](sql/schema.sql) in it.
2. Create a `.env` file with `NEON_CONNECTION_STRING` set to your connection string. Use the `postgresql+psycopg2://` prefix, because plain `postgresql://` fails with SQLAlchemy 2.x.
3. Install dependencies with `pip install -r requirements.txt`.
4. Add your own scraped Daraz workbooks under `data/raw/` and run the consolidation notebook to produce `raw_combined.csv`.
5. Load one batch with `python pipeline/run_batch.py`, or every pending batch with `python pipeline/run_batch.py --all`.
6. For automation, add `NEON_CONNECTION_STRING` as a repository secret and enable the workflow in `.github/workflows/`.
7. Open `dashboard/Dashboard.pbix` in Power BI Desktop and point it at your Neon database.

<!-- TODO: confirm how run_batch.py gets the batch data on the GitHub runner, since data/ is not tracked in git. Adjust step 4 and the Architecture section if needed. -->

---

*Built by Pallab Dey*

- **Email**: pallabdey21@gmail.com
- **LinkedIn**: [linkedin.com/in/pallabdey007](https://www.linkedin.com/in/pallabdey007)
- **GitHub**: [github.com/Rusher0077](https://github.com/Rusher0077)
