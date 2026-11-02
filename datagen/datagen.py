"""Lakeshore Outfitters — synthetic data generator.

Companion module for the course. It generates a small, realistic e-commerce
dataset with this shape:

    customers ──< addresses   (1-2 per customer: shipping / billing)
        │
        └──< orders           (each with a nested `items` array)
                └── payments  (exactly one per order)
                        └──< refunds  (a small share of payments)

Design rules (explained in lesson 2.5):
- Python standard library only. You can read every line — no magic.
- Deterministic: same seed => identical data. Your tables match the videos.
- Referential integrity: every foreign key points to a row that exists.
- Data-quality problems are OPT-IN via `dirty=True` — clean by default.
"""

import csv
import json
import random
from datetime import datetime, timedelta

# --------------------------------------------------------------- reference data

FIRST_NAMES = ["Ava", "Ben", "Carla", "Diego", "Elena", "Felix", "Grace", "Hugo",
               "Iris", "Jonas", "Karim", "Lena", "Marco", "Nadia", "Oscar", "Paula",
               "Quinn", "Rosa", "Sam", "Tara"]

LAST_NAMES = ["Alvarez", "Baker", "Costa", "Dietrich", "Evans", "Fischer", "Gomez",
              "Hansen", "Ito", "Jensen", "Keller", "Lindberg", "Moretti", "Novak",
              "Olsen", "Petrov", "Quintana", "Rossi", "Silva", "Tanaka"]

CITIES = [("Denver", "CO"), ("Seattle", "WA"), ("Portland", "OR"), ("Boulder", "CO"),
          ("Salt Lake City", "UT"), ("Bozeman", "MT"), ("Asheville", "NC"),
          ("Flagstaff", "AZ"), ("Burlington", "VT"), ("Bend", "OR"),
          ("Boise", "ID"), ("Missoula", "MT")]

STREETS = ["Alpine Way", "Basecamp Rd", "Cedar Ridge Ln", "Driftwood Ave",
           "Elk Meadow Dr", "Foothill Blvd", "Glacier St", "Highline Trail",
           "Juniper Ct", "Lakeview Dr", "Summit Pass", "Timberline Rd"]

# Fixed product catalog: (product_id, name, category, unit_price)
PRODUCTS = [
    ("P001", "Ridgeline 2P Tent",        "Camping",  249.00),
    ("P002", "Ridgeline 4P Tent",        "Camping",  399.00),
    ("P003", "Ember Down Sleeping Bag",  "Camping",  179.00),
    ("P004", "Basecamp Camping Stove",   "Camping",   89.00),
    ("P005", "Torrent 40L Backpack",     "Hiking",   139.00),
    ("P006", "Torrent 65L Backpack",     "Hiking",   189.00),
    ("P007", "Switchback Trekking Poles","Hiking",    69.00),
    ("P008", "Cascade Hiking Boots",     "Footwear", 159.00),
    ("P009", "Riverbed Trail Runners",   "Footwear", 129.00),
    ("P010", "Summit Insulated Jacket",  "Apparel",  199.00),
    ("P011", "Drizzle Rain Shell",       "Apparel",  119.00),
    ("P012", "Alpenglow Fleece",         "Apparel",   79.00),
    ("P013", "Merino Base Layer",        "Apparel",   59.00),
    ("P014", "Glacier Sunglasses",       "Gear",      89.00),
    ("P015", "Lumen 400 Headlamp",       "Gear",      45.00),
    ("P016", "Hydra 2L Water Bladder",   "Gear",      35.00),
    ("P017", "Trailside First Aid Kit",  "Gear",      29.00),
    ("P018", "Otter Dry Bag 20L",        "Water",     39.00),
    ("P019", "Lakeshore Touring Kayak",  "Water",    899.00),
    ("P020", "Driftline Paddle",         "Water",    129.00),
]

PAYMENT_METHODS = ["credit_card", "paypal", "gift_card", "bank_transfer"]

# Numeric status codes on purpose — decoding them into words is a Section 6 exercise.
PAYMENT_STATUS_CODES = {1: "completed", 2: "pending", 3: "failed"}

REFUND_REASONS = ["DAMAGED", "WRONG_ITEM", "TOO_SMALL", "TOO_LARGE", "CHANGED_MIND"]
REFUND_SOURCES = ["customer_support", "self_service"]

BASE_DATE = datetime(2026, 1, 5, 8, 0, 0)   # the shop "opened" here
DEFAULT_SEED = 2026

# ------------------------------------------------------------------- generators


def generate_customers(n=200, seed=DEFAULT_SEED, dirty=False):
    """Return `n` customers. With dirty=True: ~5% null emails, ~3% duplicates."""
    rng = random.Random(seed)
    rows = []
    for i in range(1, n + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        created = BASE_DATE + timedelta(minutes=rng.randint(0, 60 * 24 * 90))
        rows.append({
            "customer_id": f"C{i:05d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}.{i}@example.com".lower(),
            "created_at": created.isoformat(),
        })
    if dirty:
        for row in rng.sample(rows, k=max(1, n // 20)):      # ~5% missing email
            row["email"] = None
        for row in rng.sample(rows, k=max(1, n * 3 // 100)):  # ~3% duplicated later
            dup = dict(row)
            later = datetime.fromisoformat(row["created_at"]) + timedelta(days=1)
            dup["created_at"] = later.isoformat()
            rows.append(dup)
    return rows


def generate_addresses(customers, seed=DEFAULT_SEED):
    """1 shipping address per customer, plus a billing address for ~70% of them.

    One ROW per address (long format) — pivoting shipping/billing into one row
    per customer is a Section 6 exercise.
    """
    rng = random.Random(seed)
    rows = []
    for cust in customers:
        types = ["shipping"] + (["billing"] if rng.random() < 0.7 else [])
        for addr_type in types:
            city, state = rng.choice(CITIES)
            rows.append({
                "customer_id": cust["customer_id"],
                "address_type": addr_type,
                "street": f"{rng.randint(1, 999)} {rng.choice(STREETS)}",
                "city": city,
                "state": state,
                "postcode": f"{rng.randint(10000, 99999)}",
            })
    return rows


def generate_products():
    """The fixed Lakeshore catalog (no randomness — it's reference data)."""
    return [{"product_id": p, "name": n, "category": c, "unit_price": u}
            for p, n, c, u in PRODUCTS]


def generate_orders(customers, n=1000, seed=DEFAULT_SEED, dirty=False):
    """Return `n` orders with a nested `items` array (1-4 products, qty 1-3).

    Every order belongs to an existing customer and happens AFTER that
    customer's signup. With dirty=True: ~4% of orders contain a duplicated
    item entry (deduplicating the array is a Section 6 exercise).
    """
    rng = random.Random(seed)
    rows = []
    for i in range(1, n + 1):
        cust = rng.choice(customers)
        signup = datetime.fromisoformat(cust["created_at"])
        order_ts = signup + timedelta(minutes=rng.randint(60, 60 * 24 * 60))
        items = []
        for prod_id, name, category, price in rng.sample(PRODUCTS, k=rng.randint(1, 4)):
            items.append({"product_id": prod_id,
                          "quantity": rng.randint(1, 3),
                          "unit_price": price})
        if dirty and rng.random() < 0.04:
            items.append(dict(items[0]))          # duplicated item entry
        rows.append({
            "order_id": f"O{i:06d}",
            "customer_id": cust["customer_id"],
            "order_ts": order_ts.isoformat(),
            "items": items,
        })
    return rows


def generate_payments(orders, seed=DEFAULT_SEED):
    """Exactly one payment per order. `status_code` is numeric on purpose."""
    rng = random.Random(seed)
    rows = []
    for order in orders:
        amount = sum(it["quantity"] * it["unit_price"] for it in order["items"])
        pay_ts = datetime.fromisoformat(order["order_ts"]) + \
            timedelta(minutes=rng.randint(0, 30))
        rows.append({
            "payment_id": order["order_id"].replace("O", "PAY"),
            "order_id": order["order_id"],
            "payment_ts": pay_ts.isoformat(),
            "amount": round(amount, 2),
            "method": rng.choice(PAYMENT_METHODS),
            "status_code": rng.choices([1, 2, 3], weights=[85, 10, 5])[0],
        })
    return rows


def generate_refunds(payments, seed=DEFAULT_SEED, rate=0.08):
    """Refund ~`rate` of COMPLETED payments, never more than the paid amount.

    `reason` is packed as "REASON|source" (e.g. "DAMAGED|self_service") —
    splitting it apart is a Section 6 exercise.
    """
    rng = random.Random(seed)
    completed = [p for p in payments if p["status_code"] == 1]
    rows = []
    for i, pay in enumerate(rng.sample(completed, k=int(len(completed) * rate)), 1):
        refund_ts = datetime.fromisoformat(pay["payment_ts"]) + \
            timedelta(days=rng.randint(1, 30))
        rows.append({
            "refund_id": f"R{i:05d}",
            "payment_id": pay["payment_id"],
            "refund_ts": refund_ts.isoformat(),
            "amount": round(pay["amount"] * rng.choice([0.25, 0.5, 1.0]), 2),
            "reason": f"{rng.choice(REFUND_REASONS)}|{rng.choice(REFUND_SOURCES)}",
        })
    return rows


def generate_all(seed=DEFAULT_SEED, n_customers=200, n_orders=1000, dirty=False):
    """One-call entry point. Returns {entity_name: list_of_dicts}."""
    customers = generate_customers(n_customers, seed=seed, dirty=dirty)
    orders = generate_orders(customers, n_orders, seed=seed, dirty=dirty)
    payments = generate_payments(orders, seed=seed)
    return {
        "customers": customers,
        "addresses": generate_addresses(customers, seed=seed),
        "products": generate_products(),
        "orders": orders,
        "payments": payments,
        "refunds": generate_refunds(payments, seed=seed),
    }

# ---------------------------------------------------------------------- writers


def write_json(rows, path):
    """Write rows as JSON Lines (one JSON object per line) — Spark's favourite."""
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def write_csv(rows, path, delimiter=","):
    """Write rows as CSV with a header row. Use delimiter='\\t' for TSV."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)
