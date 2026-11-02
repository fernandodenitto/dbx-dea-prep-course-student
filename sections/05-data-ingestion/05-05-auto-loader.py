# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader: incremental file ingestion at scale
# MAGIC
# MAGIC Auto Loader = Structured Streaming with a file source that **discovers and
# MAGIC tracks new files** for you — schema inference, schema tracking, and
# MAGIC exactly-once bookkeeping included. Source format: `cloudFiles`.
# MAGIC
# MAGIC Today it ingests our orders — the entity that "keeps arriving".

# COMMAND ----------

ORDERS_SRC  = "/Volumes/lakeshore/landing/files/orders"
AL_BASE     = "/Volumes/lakeshore/landing/files/_autoloader/orders"
CHECKPOINT  = f"{AL_BASE}/checkpoint"
SCHEMA_LOC  = f"{AL_BASE}/schema"

# COMMAND ----------

def ingest_orders():
    """One availableNow pass of the Auto Loader stream: land pending files."""
    (spark.readStream
     .format("cloudFiles")
     .option("cloudFiles.format", "json")
     .option("cloudFiles.schemaLocation", SCHEMA_LOC)
     .load(ORDERS_SRC)
     .selectExpr("*", "_metadata.file_path AS source_file")
     .writeStream
     .option("checkpointLocation", CHECKPOINT)
     .option("mergeSchema", "true")   # sink evolves with the source (lesson 5.6)
     .outputMode("append")
     .trigger(availableNow=True)
     .toTable("lakeshore.bronze.orders")
     ).awaitTermination()
    return spark.table("lakeshore.bronze.orders").count()

def rows_from(file_pattern):
    """How many bronze rows came from files whose name matches the pattern."""
    return (spark.table("lakeshore.bronze.orders")
            .where(f"source_file LIKE '%{file_pattern}%'").count())

print(f"orders in bronze after this pass: {ingest_orders()}")
assert rows_from("orders_batch_001") == 1000

# COMMAND ----------

# MAGIC %md
# MAGIC 1,000 orders from `orders_batch_001.json`, schema inferred and **saved to the
# MAGIC schemaLocation** so every restart agrees on it. The `source_file` column we
# MAGIC added via `_metadata` records provenance — a bronze-layer best practice. And
# MAGIC the automatic `_rescued_data` column is Auto Loader's safety net for data that
# MAGIC doesn't match the schema.
# MAGIC
# MAGIC **Look closely at the schema** — exam fact incoming: for JSON, Auto Loader
# MAGIC infers every column as **STRING**, nested fields included, so `items` arrives
# MAGIC as a JSON *string*, not an array. Typed inference exists,
# MAGIC `cloudFiles.inferColumnTypes = true`, but we keep bronze raw on purpose: bronze
# MAGIC preserves what arrived; *parsing* is silver's job, in Section 6 with `from_json`.

# COMMAND ----------

types = dict(spark.table("lakeshore.bronze.orders").dtypes)
print({c: types[c] for c in ["order_id", "order_ts", "items", "_rescued_data", "source_file"]})
assert types["items"] == "string"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, order_ts, items, source_file
# MAGIC FROM lakeshore.bronze.orders LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## New files arrive: the live demo, two tabs
# MAGIC
# MAGIC 1. Open `05-05-auto-loader-feeder` from this section's folder in a **second tab** and run
# MAGIC    it. It drops one new order file per interval, batches 1, 2 and 3.
# MAGIC 2. Come back and run the next cell: a loop of `availableNow` passes. Each pass
# MAGIC    catches up and stops, and the count climbs while you watch.
# MAGIC
# MAGIC Why a loop and not one long-running stream? This is serverless, and lesson 5.4
# MAGIC showed that `processingTime` is rejected here. A loop of `availableNow` passes
# MAGIC gives the same picture, using a trigger that really runs on your compute.
# MAGIC
# MAGIC No second tab? Skip the next cell: the cell after it gets you to the same place.

# COMMAND ----------

import time

LIVE_PASSES = 6
for p in range(1, LIVE_PASSES + 1):
    n = ingest_orders()
    print(f"{time.strftime('%H:%M:%S')}  pass {p}: {n:,} orders in bronze")
    if p < LIVE_PASSES:
        time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catch up, with or without the feeder
# MAGIC
# MAGIC This cell emits batches 1, 2 and 3 itself, then runs one pass. If the feeder
# MAGIC already dropped them, these are identical files at the same paths, and Auto
# MAGIC Loader ignores them: **0 new rows**. If you skipped the feeder, they are new:
# MAGIC **60 new rows**. Either way you land on the same number.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")           # make datagen.py and feed.py importable

import feed

before = spark.table("lakeshore.bronze.orders").count()
for b in (1, 2, 3):
    feed.emit_batch("orders", ORDERS_SRC, batch_num=b)   # 20 orders each
after = ingest_orders()
print(f"new rows this pass: {after - before}")

assert rows_from("orders_batch_001") == 1000
assert rows_from("orders_feed_000") == 60               # batches 1, 2, 3: exactly 20 each
print("batch_001 + feed batches 1-3 = 1,060 orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prove it: re-emit a batch
# MAGIC
# MAGIC Same file, same path, one more pass. `cloudFiles.allowOverwrites` defaults to
# MAGIC `false`, so a path already in the checkpoint is not read again.

# COMMAND ----------

before = spark.table("lakeshore.bronze.orders").count()
feed.emit_batch("orders", ORDERS_SRC, batch_num=2)
after = ingest_orders()
print(f"re-emitted batch 2 → {after - before} new rows")
assert after == before

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The new rows, with their provenance
# MAGIC SELECT order_id, order_ts, source_file
# MAGIC FROM lakeshore.bronze.orders
# MAGIC WHERE source_file LIKE '%feed%'
# MAGIC ORDER BY order_id LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## File discovery modes (exam theory)
# MAGIC
# MAGIC How does Auto Loader *discover* files? The exam guide names two modes; the docs
# MAGIC now split the second in two.
# MAGIC
# MAGIC | | Directory listing | File notification with file events | Classic file notification |
# MAGIC |---|---|---|---|
# MAGIC | How | Lists the input directory | Events from a Databricks-managed queue, one per external location | Events from a queue per stream, in your cloud account |
# MAGIC | Option | none, it is the default | `cloudFiles.useManagedFileEvents = true` | `cloudFiles.useNotifications = true` |
# MAGIC | Setup | Only access to the data | An admin enables file events on the external location; no extra permissions on the stream | Elevated cloud permissions for the queue and subscription |
# MAGIC | Docs' position | Default; quick starts | **Recommended for most workloads** | Older path |
# MAGIC
# MAGIC Same exactly-once guarantees everywhere. We use directory listing, the only
# MAGIC mode a volume on Free Edition needs. `cloudFiles.useIncrementalListing` is
# MAGIC deprecated.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC `cloudFiles` = streaming file source with automatic discovery + tracking ·
# MAGIC `schemaLocation` stores the inferred schema · JSON columns infer as STRING ·
# MAGIC `_rescued_data` catches misfits · `_metadata.file_path` = provenance ·
# MAGIC `availableNow` = incremental batch, the trigger serverless accepts · discovery:
# MAGIC directory listing (default), file events (recommended), classic notification.

# COMMAND ----------

dbutils.notebook.exit("auto-loader-complete")
