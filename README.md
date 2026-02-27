# E-Commerce Data Pipeline — GCP / BigQuery

A personal data engineering project simulating a real-world retail data infrastructure on Google Cloud Platform.

The pipeline centralises data from two sales channels — an online store (Shopify-style) and in-store POS transactions — into a unified BigQuery data warehouse, with analytics views ready for Looker Studio dashboards.

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
│  BigQuery: ecommerce_raw         │  Raw layer — untransformed source data
│  • shopify_orders                │
│  • pos_transactions              │
│  • order_items                   │
│  • customers                     │
│  • products                      │
└──────────────────────────────────┘
               │
               ▼  SQL transformation (step3_transform.sql)
┌──────────────────────────────────┐
│  BigQuery: ecommerce_dwh         │  Star schema — analytics-ready
│                                  │
│  Fact table:                     │
│  • fact_sales (partitioned/date) │  ← unified Shopify + POS
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
│  Looker Studio Dashboard         │  Connected directly to BigQuery views
│  • Revenue by channel            │
│  • Top products                  │
│  • Sales trends over time        │
│  • Customer segments             │
└──────────────────────────────────┘
```

---

## Stack

- **Python 3** — data generation and BigQuery ingestion
- **Google BigQuery** — data warehouse (raw + transformed layers)
- **SQL** — ELT transformations, star schema modelling
- **Looker Studio** — dashboards connected to BigQuery views
- **Google Cloud Platform** — infrastructure

---

## Project Structure

```
ecommerce-data-pipeline/
├── step1_generate_data.py        Generate sample CSV data
├── step2_upload_to_bigquery.py   Load CSVs into BigQuery raw layer
├── step3_transform.sql           SQL: star schema + analytics views
└── README.md
```

---

## How to Run

### 1. Prerequisites

```bash
pip install google-cloud-bigquery pandas pyarrow
gcloud auth application-default login
```

Set your GCP project ID in `step2_upload_to_bigquery.py`:
```python
PROJECT_ID = "your-project-id"
```

### 2. Generate sample data

```bash
python step1_generate_data.py
```

Creates 5 CSV files simulating 200 online orders and 300 in-store transactions.

### 3. Load into BigQuery

```bash
python step2_upload_to_bigquery.py
```

Loads all CSVs into `ecommerce_raw` dataset. Load is idempotent (`WRITE_TRUNCATE`) — safe to re-run.

### 4. Run SQL transformations

Open `step3_transform.sql` in the BigQuery console and run each block in order to build:
- The `ecommerce_dwh` dataset
- Dimension tables (dim_customers, dim_products, dim_date, dim_channels)
- The unified `fact_sales` table partitioned by date
- Four analytics views

### 5. Connect to Looker Studio

Connect Looker Studio to the `ecommerce_dwh` views for live dashboards.

---

## Key Design Decisions

**Two-layer architecture (raw + dwh)**
Raw data is loaded untouched. Transformations happen in a separate dataset. If requirements change, raw data can always be reprocessed.

**ELT over ETL**
Data is loaded first, then transformed inside BigQuery using SQL. BigQuery handles transformation at scale more efficiently than preprocessing outside.

**fact_sales partitioned by date**
Partitioning limits the data scanned per query. For a query on "last week's sales," only 7 partitions are read instead of the full table — reduces cost and improves speed.

**UNION ALL to unify channels**
Shopify and POS use different schemas and ID formats. A UNION ALL in fact_sales normalises both into a single consistent table, so all downstream analytics work across both channels without needing to know the source.
