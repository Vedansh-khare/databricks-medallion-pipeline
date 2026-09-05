"""
Generates synthetic raw data files simulating a source system dump. Mimics real-world messiness: nulls, dupes, inconsistent types, late-arriving records. Run once to populate data/raw/ before running bronze ingestion.
"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

RAW_DIR = Path(__file__).parent / "raw"
RAW_DIR.mkdir(exist_ok=True)

CUSTOMERS = [f"CUST{str(i).zfill(4)}" for i in range(1, 201)]
PRODUCTS = [f"PROD{str(i).zfill(3)}" for i in range(1, 51)]
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", None]  # None simulates missing region
CATEGORIES = ["ELECTRONICS", "APPAREL", "HOME", "GROCERY", "TOYS"]


def random_date(start_days_ago=180, end_days_ago=0):
    delta = random.randint(end_days_ago, start_days_ago)
    return (datetime.now() - timedelta(days=delta)).strftime("%Y-%m-%d %H:%M:%S")


def generate_customers(n=200):
    rows = []
    for i, cust_id in enumerate(CUSTOMERS[:n]):
        # inject dirty data: ~5% missing email, ~3% duplicate rows
        email = f"user{i}@example.com" if random.random() > 0.05 else None
        rows.append({
            "customer_id": cust_id,
            "customer_name": f"Customer {i}",
            "email": email,
            "region": random.choice(REGIONS),
            "signup_date": random_date(365, 30),
        })
    # inject duplicates
    dupes = random.sample(rows, k=int(n * 0.03))
    rows.extend(dupes)
    random.shuffle(rows)
    return rows


def generate_products(n=50):
    rows = []
    for i, prod_id in enumerate(PRODUCTS[:n]):
        rows.append({
            "product_id": prod_id,
            "product_name": f"Product {i}",
            "category": random.choice(CATEGORIES),
            "unit_price": round(random.uniform(5.0, 500.0), 2),
        })
    return rows


def generate_orders(n=5000):
    rows = []
    for i in range(n):
        # inject dirty data: ~2% null customer_id, ~4% negative/zero quantity (data entry errors)
        cust_id = random.choice(CUSTOMERS) if random.random() > 0.02 else None
        qty = random.randint(1, 10)
        if random.random() < 0.04:
            qty = random.choice([0, -1])
        rows.append({
            "order_id": f"ORD{str(i).zfill(6)}",
            "customer_id": cust_id,
            "product_id": random.choice(PRODUCTS),
            "quantity": qty,
            "order_date": random_date(180, 0),
            "order_status": random.choice(["COMPLETED", "CANCELLED", "PENDING", "RETURNED"]),
        })
    return rows


def write_csv(rows, filename, fieldnames):
    path = RAW_DIR / filename
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {path}")


def write_json(rows, filename):
    path = RAW_DIR / filename
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    print(f"Wrote {len(rows)} rows -> {path}")


if __name__ == "__main__":
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders()

    write_csv(customers, "customers.csv", ["customer_id", "customer_name", "email", "region", "signup_date"])
    write_csv(products, "products.csv", ["product_id", "product_name", "category", "unit_price"])
    # orders as JSONL to simulate event-stream-style source (common real pattern: orders come as JSON events, dims as CSV extracts)
    write_json(orders, "orders.jsonl")

    print("\nSample data generation complete.")
    print(f"Files in: {RAW_DIR}")
