# Standalone

This folder contains the data exploration and analysis scripts for the NosCités short-term rental dataset. It covers Phase 1 (standalone MongoDB) and the collection merge step before moving to the distributed architecture.

## Files

| File | Purpose |
| :--- | :--- |
| `merge_collections.mongodb.js` | Merges `paris_listings` + `lyon_listings` into `listings` — adds `city` field to both, merges, verifies counts |
| `analysis_queries.mongodb.js` | KPI analysis — all MongoDB queries run against the `paris_listings` collection (Phase 1) |
| `notebooks/analyse.ipynb` | Advanced statistics with Polars — medians, booking rates, geographic analysis |

## merge_collections.mongodb.js — Collection Merge

Run this script **after importing both CSV files** — Paris into `paris_listings` and Lyon into `lyon_listings` via Compass. The script:

1. Adds `city: "Paris"` to all documents in `paris_listings`
2. Adds `city: "Lyon"` to all documents in `lyon_listings`
3. Merges both into the unified `listings` collection
4. Verifies document counts
5. Keeps source collections intact for integration testing (optional drop at the bottom)

```bash
mongosh --file merge_collections.mongodb.js
```

## analysis_queries.mongodb.js — KPI Queries

Runs the full set of analysis queries against the `paris_listings` collection (Phase 1 & 2). Covers:

- Total document count and availability rate
- Listing breakdown by room type and top 5 property types
- Top 5 listings by number of reviews
- Total unique hosts
- Instant bookable listings and rate
- Professional hosts (more than 100 listings) and their share
- Superhost count and rate

```bash
mongosh --file analysis_queries.mongodb.js
```

## notebooks/analyse.ipynb — Advanced Statistics (Polars)

For queries requiring statistical operations that go beyond MongoDB's aggregation pipeline capabilities, data is extracted via **PyMongo** and processed using **Polars** DataFrames.

Covers:

- Median number of reviews per listing (global and by host category)
- Monthly booking rate by room type
- Listing density by Paris neighbourhood
- Neighbourhoods ranked by booking rate

```bash
# Install dependencies first
poetry install

# Launch the notebook
jupyter notebook notebooks/analyse.ipynb
```
