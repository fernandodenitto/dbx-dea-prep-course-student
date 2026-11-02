"""Lakeshore live-feed simulator — companion to datagen.py.

Drops small order files into the landing volume so streaming lessons process
GENUINELY new data, not pre-staged files. Run it from a second "feeder"
notebook while your stream runs in the first — and watch the full lifecycle:
file lands → stream picks it up → table grows.

Design rules:
- Reuses datagen.py generators — no logic duplicated.
- Deterministic per (seed, batch_num): re-emitting a batch rewrites an
  IDENTICAL file, so Auto Loader (which tracks files by path) ignores reruns.
- Event timestamps increase monotonically across batches.
- New orders reference the SAME 200-customer pool as batch_001 — referential
  integrity holds across the whole feed.
"""

import random
import time
from datetime import datetime, timedelta

import datagen

# All feed timestamps come after every batch_001 timestamp (which end ~early June
# 2026: signup window ~90 days + order window ~60 days from Jan 5)
FEED_BASE = datetime(2026, 7, 1, 9, 0, 0)
FIRST_FEED_ORDER = 1001          # batch_001 used order ids O000001–O001000


def _order_batch(batch_num, batch_size, seed, extra_fields=None):
    """Deterministic batch of orders: same (seed, batch_num) => same rows."""
    customers = datagen.generate_customers(200, seed=seed)   # same pool as batch_001
    rng = random.Random(f"{seed}-orders-feed-{batch_num}")
    base_ts = FEED_BASE + timedelta(minutes=30 * (batch_num - 1))
    start_id = FIRST_FEED_ORDER + (batch_num - 1) * batch_size

    rows = []
    for i in range(batch_size):
        cust = rng.choice(customers)
        items = []
        for prod_id, name, category, price in rng.sample(datagen.PRODUCTS,
                                                         k=rng.randint(1, 3)):
            items.append({"product_id": prod_id,
                          "quantity": rng.randint(1, 3),
                          "unit_price": price})
        row = {
            "order_id": f"O{start_id + i:06d}",
            "customer_id": cust["customer_id"],
            "order_ts": (base_ts + timedelta(seconds=i * 20)).isoformat(),
            "items": items,
        }
        if extra_fields:
            row.update(extra_fields)      # e.g. {"sales_channel": "mobile_app"}
        rows.append(row)
    return rows


def emit_batch(entity, volume_path, batch_num, batch_size=20,
               seed=datagen.DEFAULT_SEED, extra_fields=None):
    """Generate and write ONE feed file. Returns the file path.

    Only 'orders' is implemented — it's the entity our streams ingest.
    """
    if entity != "orders":
        raise ValueError(f"feed.py only implements 'orders' (got '{entity}')")
    rows = _order_batch(batch_num, batch_size, seed, extra_fields)
    path = f"{volume_path}/{entity}_feed_{batch_num:04d}.json"
    datagen.write_json(rows, path)
    return path


def run_feed(entity, volume_path, batches=5, batch_size=20,
             interval_seconds=10, seed=datagen.DEFAULT_SEED, start_batch=1):
    """Emit `batches` files, one every `interval_seconds`.

    Run this in a SECOND notebook while your streaming query runs in the
    first. Re-running with the same parameters rewrites identical files, so
    already-processed batches are not ingested twice.
    """
    last = start_batch + batches - 1
    for b in range(start_batch, last + 1):
        path = emit_batch(entity, volume_path, b, batch_size, seed)
        print(f"[feed] batch {b:>3} → {path}")
        if b < last:
            time.sleep(interval_seconds)
    print(f"[feed] done: {batches} batches of {batch_size} {entity}")
