# ============================================================
# E-Commerce Data Pipeline — GCP / BigQuery
# Personal project: data engineering practice
# ============================================================
# Simulates two data sources:
#   - Shopify  : online orders
#   - POS      : in-store point-of-sale transactions
# Generates CSV files that are then loaded into BigQuery
# ============================================================
# Run: python step1_generate_data.py
# ============================================================

import csv
import random
from datetime import datetime, timedelta

random.seed(42)

PRODUCTS = [
    {"product_id": "P001", "name": "Organic Green Tea 100g",    "category": "Drinks",    "price": 9.50,  "channel": "both"},
    {"product_id": "P002", "name": "Matcha Latte Mix",          "category": "Drinks",    "price": 14.00, "channel": "both"},
    {"product_id": "P003", "name": "Artisan Granola 500g",      "category": "Food",      "price": 8.50,  "channel": "online"},
    {"product_id": "P004", "name": "Dark Chocolate Bar 85%",    "category": "Snacks",    "price": 5.00,  "channel": "both"},
    {"product_id": "P005", "name": "Cold Brew Coffee Kit",      "category": "Drinks",    "price": 12.00, "channel": "online"},
    {"product_id": "P006", "name": "Fresh Sandwich",            "category": "Food",      "price": 7.50,  "channel": "instore"},
    {"product_id": "P007", "name": "Reusable Water Bottle",     "category": "Lifestyle", "price": 22.00, "channel": "online"},
    {"product_id": "P008", "name": "Protein Bar x6",            "category": "Snacks",    "price": 11.00, "channel": "both"},
    {"product_id": "P009", "name": "Seasonal Fruit Bowl",       "category": "Food",      "price": 6.50,  "channel": "instore"},
    {"product_id": "P010", "name": "Herbal Tea Sampler",        "category": "Drinks",    "price": 16.00, "channel": "both"},
]

CUSTOMERS = [
    {
        "customer_id": f"C{str(i).zfill(3)}",
        "name": name,
        "email": f"{name.lower().replace(' ', '.')}@email.com",
        "city": random.choice(["Paris", "Lyon", "Marseille", "Bordeaux", "Lille", "Nantes"]),
        "loyalty_member": random.choice([True, False])
    }
    for i, name in enumerate([
        "Sophie Martin", "Lucas Dubois", "Emma Bernard", "Noah Thomas",
        "Jade Petit", "Hugo Robert", "Lea Richard", "Gabriel Simon",
        "Chloe Michel", "Ethan Lefebvre", "Laura Moreau", "Pierre Lambert",
        "Marie Dupont", "Antoine Girard", "Camille Roux", "Julien Blanc",
        "Alice Fontaine", "Maxime Chevalier", "Inès Morel", "Tom Garnier",
    ], 1)
]

def generate_shopify_orders(n=200):
    orders = []
    start_date = datetime(2025, 6, 1)
    for i in range(1, n + 1):
        customer = random.choice(CUSTOMERS)
        order_date = start_date + timedelta(
            days=random.randint(0, 180),
            hours=random.randint(8, 22),
            minutes=random.randint(0, 59)
        )
        available = [p for p in PRODUCTS if p["channel"] in ("online", "both")]
        num_items = random.randint(1, 4)
        items = random.sample(available, min(num_items, len(available)))
        total = round(sum(p["price"] * random.randint(1, 3) for p in items), 2)

        orders.append({
            "order_id":       f"SHP-{str(i).zfill(4)}",
            "source":         "shopify",
            "customer_id":    customer["customer_id"],
            "order_date":     order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "status":         random.choices(
                                  ["completed", "refunded", "cancelled"],
                                  weights=[90, 6, 4])[0],
            "total_amount":   total,
            "num_items":      len(items),
            "shipping_city":  customer["city"],
        })
    return orders

def generate_pos_transactions(n=300):
    transactions = []
    start_date = datetime(2025, 6, 1)
    for i in range(1, n + 1):
        customer = random.choice(CUSTOMERS + [None] * 8)
        trans_date = start_date + timedelta(
            days=random.randint(0, 180),
            hours=random.randint(9, 21),
            minutes=random.randint(0, 59)
        )
        available = [p for p in PRODUCTS if p["channel"] in ("instore", "both")]
        num_items = random.randint(1, 3)
        items = random.sample(available, min(num_items, len(available)))
        total = round(sum(p["price"] * random.randint(1, 2) for p in items), 2)

        transactions.append({
            "transaction_id":   f"POS-{str(i).zfill(4)}",
            "source":           "pos",
            "customer_id":      customer["customer_id"] if customer else "ANONYMOUS",
            "transaction_date": trans_date.strftime("%Y-%m-%d %H:%M:%S"),
            "payment_method":   random.choice(["card", "card", "cash", "contactless"]),
            "total_amount":     total,
            "num_items":        len(items),
            "store_location":   random.choice(["Paris - Centre", "Paris - Nord", "Lyon"]),
        })
    return transactions

def generate_order_items(orders, transactions):
    items = []
    item_id = 1
    all_txns = [(o["order_id"], "shopify") for o in orders] + \
               [(t["transaction_id"], "pos") for t in transactions]

    for txn_id, source in all_txns:
        if source == "shopify":
            available = [p for p in PRODUCTS if p["channel"] in ("online", "both")]
        else:
            available = [p for p in PRODUCTS if p["channel"] in ("instore", "both")]

        num_items = random.randint(1, 3)
        selected = random.sample(available, min(num_items, len(available)))
        for product in selected:
            qty = random.randint(1, 3)
            items.append({
                "item_id":      f"ITEM-{str(item_id).zfill(5)}",
                "order_id":     txn_id,
                "product_id":   product["product_id"],
                "product_name": product["name"],
                "category":     product["category"],
                "quantity":     qty,
                "unit_price":   product["price"],
                "line_total":   round(product["price"] * qty, 2),
            })
            item_id += 1
    return items

def write_csv(filename, data):
    if not data:
        return
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"  ✅  {filename}  ({len(data)} rows)")

if __name__ == "__main__":
    print("\n📦 Generating e-commerce sample data...\n")
    shopify_orders   = generate_shopify_orders(200)
    pos_transactions = generate_pos_transactions(300)
    order_items      = generate_order_items(shopify_orders, pos_transactions)

    write_csv("raw_shopify_orders.csv",      shopify_orders)
    write_csv("raw_pos_transactions.csv",    pos_transactions)
    write_csv("raw_order_items.csv",         order_items)
    write_csv("raw_customers.csv",           CUSTOMERS)
    write_csv("raw_products.csv",            PRODUCTS)

    print("\n✅  Done — 5 CSV files ready.")
    print("👉  Next: python step2_upload_to_bigquery.py")
