# E-Commerce Data Pipeline — GCP / BigQuery

A personal project I built to practice real-world data engineering on Google Cloud Platform.

The idea was simple: take data from two different sales channels — an online store (Shopify-style) and physical in-store POS transactions — and build a proper data warehouse that unifies everything into one place, ready for analytics and reporting.

---

## What it does

The pipeline ingests raw sales data from two sources, loads it into BigQuery, transforms it into a clean star schema, and exposes analytics views that power a live Looker Studio dashboard.

End result: a unified view of **5,500 transactions**, **€198,000+ in revenue**, across **12 months** of data — queryable in seconds.

---

## Architecture

```
[Online Store API]      [POS System]
        │                    │
        ▼                    ▼
   Python ingestion script (step2_upload_to_bigquery.py)
        │                    │
        ▼                    ▼
┌──────────────────────────────────┐
│  BigQuery: ecommerce_raw         │  Raw layer — data loaded as-is
│  • shopify_orders                │
│  • pos_transactions              │
│  • order_items                   │
│  • customers                     │
│  • products                      │
└──────────────────────────────────┘
               │
               ▼  SQL transformation (step3_transform.sql)
┌──────────────────────────────────┐
│  BigQuery: ecommerce_dwh         │  Star schema — clean, analytics-ready
│                                  │
│  Fact table:                     │
│  • fact_sales                    │  ← unified Shopify + POS in one table
│                                  │
│  Dimension tables:               │
│  • dim_customers                 │
│  • dim_products                  │
│  • dim_date                      │
│  • dim_channels                  │
│                                  │
│  Analytics views:                │
│  • v_revenue_by_channel          │
│  • v_top_products                │
│  • v_sales_by_weekday            │
│  • v_customer_segments           │
└──────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  Looker Studio Dashboard         │  Live — connected directly to BigQuery
└──────────────────────────────────┘
```

---

## Stack

- **Python 3** — data generation and BigQuery ingestion
- **Google BigQuery** — data warehouse (project: `ecommerce-data-pipeline-488523`)
- **SQL** — ELT transformations, star schema modelling
- **Looker Studio** — dashboards connected to BigQuery views
- **Google Cloud Platform** — infrastructure

---

## Project Structure

```
ecommerce-data-pipeline/
├── step1_generate_data.py        Generates realistic sample CSV data
├── step2_upload_to_bigquery.py   Loads CSVs into BigQuery raw layer
├── step3_transform.sql           Star schema + analytics views
└── README.md
```

---

## How to Run

### Prerequisites

```bash
pip install google-cloud-bigquery pandas pyarrow
gcloud auth application-default login
gcloud auth application-default set-quota-project ecommerce-data-pipeline-488523
```

### Step 1 — Generate sample data

```bash
python step1_generate_data.py
```

Generates 5 CSV files:
- 2,000 Shopify online orders
- 3,500 POS in-store transactions
- 10,658 order line items
- 500 customers across 10 French cities
- 20 product SKUs across 5 categories

Date range: March 2024 → February 2025 (12 months)

### Step 2 — Load into BigQuery

```bash
python step2_upload_to_bigquery.py
```

Loads all 5 CSVs into the `ecommerce_raw` dataset. The load uses `WRITE_TRUNCATE` so it's safe to re-run without creating duplicates.

### Step 3 — Run SQL transformations

Open `step3_transform.sql` in the BigQuery console and run each block in order:

1. Create the `ecommerce_dwh` dataset
2. Build dimension tables (dim_customers, dim_products, dim_date, dim_channels)
3. Build the unified `fact_sales` table
4. Create the four analytics views

### Step 4 — Looker Studio

Connect Looker Studio to the views in `ecommerce_dwh` for live dashboards.

---

## Key Design Decisions

**Two-layer architecture (raw + dwh)**
The raw layer keeps data exactly as it arrives — no modifications. All transformations happen in the dwh layer. This means if something breaks or requirements change, I can always re-run the transformations from the original data without losing anything.

**ELT not ETL**
I load raw data first, then transform inside BigQuery using SQL. BigQuery is built for this — it handles large-scale SQL transformations much more efficiently than doing it outside before loading.

**UNION ALL to unify two channels**
Shopify and the POS system use completely different schemas and ID formats. Rather than trying to force them into the same shape at ingestion, I load them separately into raw tables and unify them in the `fact_sales` table using a UNION ALL. This keeps the raw data clean and makes the transformation logic explicit and easy to maintain.

**Idempotent pipeline**
Every load step uses `WRITE_TRUNCATE` and every SQL step uses `CREATE OR REPLACE`. This means the entire pipeline can be re-run from scratch at any time and will produce the same result — no duplicates, no conflicts.

---

## Live Dashboard

[View the Looker Studio Dashboard](https://lookerstudio.google.com/reporting/667b11ca-3d6d-428c-af96-02787f5a2953)
