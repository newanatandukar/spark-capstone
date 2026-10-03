#!/usr/bin/env python3
"""
make_data.py — generates the messy datasets for the Spark capstone.
Pure standard library. Run:  python make_data.py
Creates ./data/orders.csv, ./data/customers.csv, ./data/products.csv
Same seed for everyone -> everyone's numbers match.
"""
import csv, random, os
from datetime import datetime, timedelta

random.seed(42)
os.makedirs("data", exist_ok=True)

N_ORDERS = 1_000_000
N_CUSTOMERS = 10_000
N_PRODUCTS = 500

COUNTRIES = ["NP", "US", "IN", "JP", "AU", "DE", "GB", "BR"]
# 60% NP -> the skew for Part C
COUNTRY_WEIGHTS = [60, 8, 8, 6, 5, 5, 4, 4]

FIRST = ["Aarav", "Sita", "Kiran", "Maya", "Bibek", "Anju", "Rohan", "Priya",
         "Suman", "Nisha", "Hari", "Gita", "Dipesh", "Rita", "Sanjay", "Mina"]
LAST = ["Shrestha", "Gurung", "Thapa", "Rai", "Karki", "Sharma", "Adhikari",
        "Magar", "Tamang", "Basnet", "KC", "Joshi"]
CATEGORIES = ["electronics", "clothing", "grocery", "books", "sports",
              "beauty", "toys", "home"]

def rand_date(start_year=2023, end_year=2026):
    start = datetime(start_year, 1, 1)
    delta = datetime(end_year, 6, 30) - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

# ---------------- products.csv (tiny -> broadcast candidate #2) ----------------
with open("data/products.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["product_id", "category", "unit_price"])
    for pid in range(1, N_PRODUCTS + 1):
        w.writerow([pid, random.choice(CATEGORIES), round(random.uniform(1, 500), 2)])

# ---------------- customers.csv (small -> broadcast candidate #1) ----------------
# Mess: ~3% of customer_ids appear TWICE with different signup_date
#       (dedupe rule: keep the LATEST signup_date)
rows = []
for cid in range(1, N_CUSTOMERS + 1):
    name = f"{random.choice(FIRST)} {random.choice(LAST)}"
    country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
    rows.append([cid, name, country, rand_date(2020, 2025).strftime("%Y-%m-%d")])
dups = random.sample(rows, int(N_CUSTOMERS * 0.03))
for r in dups:
    rows.append([r[0], r[1], r[2], rand_date(2020, 2025).strftime("%Y-%m-%d")])
random.shuffle(rows)
with open("data/customers.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["customer_id", "name", "country", "signup_date"])
    w.writerows(rows)

# ---------------- orders.csv (big, messy, skewed) ----------------
# Mess: ~2% duplicate order_ids (exact duplicate rows)
#       ~1% negative or zero amount
#       ~0.5% empty amount
#       ~1% customer_id that does not exist in customers (orphans, id > 10000)
#       ~0.5% malformed timestamp ("not-a-date")
with open("data/orders.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["order_id", "customer_id", "product_id", "amount", "ts", "country"])
    oid = 0
    dup_pool = []
    for _ in range(N_ORDERS):
        oid += 1
        cid = random.randint(1, N_CUSTOMERS)
        r = random.random()
        if r < 0.01:                       # orphan customer
            cid = random.randint(N_CUSTOMERS + 1, N_CUSTOMERS + 500)
        pid = random.randint(1, N_PRODUCTS)
        r = random.random()
        if r < 0.01:
            amount = round(random.uniform(-200, 0), 2)   # negative/zero
        elif r < 0.015:
            amount = ""                                   # missing
        else:
            amount = round(random.expovariate(1 / 80) + 1, 2)
        r = random.random()
        ts = "not-a-date" if r < 0.005 else rand_date().strftime("%Y-%m-%d %H:%M:%S")
        country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
        row = [oid, cid, pid, amount, ts, country]
        w.writerow(row)
        if random.random() < 0.02:
            dup_pool.append(row)
    for row in dup_pool:                    # exact duplicates appended at the end
        w.writerow(row)

print("done ->",
      f"orders={N_ORDERS}+{len(dup_pool)} rows,",
      f"customers={len(rows)} rows, products={N_PRODUCTS} rows")
print("skew check: ~60% of orders should be country=NP")
