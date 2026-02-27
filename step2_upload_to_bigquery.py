# ============================================================
# E-Commerce Data Pipeline — Upload raw CSVs to BigQuery
# ============================================================
# SETUP BEFORE RUNNING:
#   1. pip install google-cloud-bigquery pandas pyarrow
#   2. gcloud auth application-default login
#   3. Set PROJECT_ID below to your GCP project ID
# ============================================================

from google.cloud import bigquery
import pandas as pd
import os

PROJECT_ID = "ecommerce-data-pipeline-488523"   # ← change this
DATASET_ID = "ecommerce_raw"

client = bigquery.Client(project="ecommerce-data-pipeline-488523")


def create_dataset():
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)
    print(f"  ✅  Dataset '{DATASET_ID}' ready")


def upload_csv(csv_file, table_name):
    if not os.path.exists(csv_file):
        print(f"  ⚠️   {csv_file} not found — run step1 first")
        return

    df = pd.read_csv(csv_file)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # idempotent — safe to re-run
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"  ✅  {table_name:<30} {table.num_rows} rows")


if __name__ == "__main__":
    print(f"\n🚀  Loading data into BigQuery")
    print(f"    Project : {PROJECT_ID}")
    print(f"    Dataset : {DATASET_ID}\n")

    create_dataset()
    print()

    files = [
        ("raw_shopify_orders.csv",    "shopify_orders"),
        ("raw_pos_transactions.csv",  "pos_transactions"),
        ("raw_order_items.csv",       "order_items"),
        ("raw_customers.csv",         "customers"),
        ("raw_products.csv",          "products"),
    ]
    for csv_file, table in files:
        upload_csv(csv_file, table)

    print(f"\n✅  All tables loaded.")
    print(f"🔗  https://console.cloud.google.com/bigquery?project={PROJECT_ID}")
    print(f"\n👉  Next: open step3_transform.sql in BigQuery console")
