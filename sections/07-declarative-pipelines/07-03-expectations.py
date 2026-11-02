# Databricks notebook source
# MAGIC %md
# MAGIC # 7.3 — A broken batch, on purpose
# MAGIC
# MAGIC Run this notebook **twice**, with one pipeline run in between:
# MAGIC
# MAGIC 1. First run: lands 20 orders with no `order_id`.
# MAGIC 2. In the pipeline editor, click **Run file** on `01_st_orders_raw.sql`, then on
# MAGIC    `02_st_orders_clean.sql`.
# MAGIC 3. Second run: proves the rows reached bronze, never reached silver, and removes
# MAGIC    the file from the landing folder so later lessons see the same files as before.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")           # make datagen.py and feed.py importable

import feed

# COMMAND ----------

RAW = "lakeshore.bronze.sdp_orders_raw"
CLEAN = "lakeshore.silver.sdp_orders"
ORDERS = "/Volumes/lakeshore/landing/files/orders"
BROKEN_BATCH = 102                                  # no other lesson uses this number

if not spark.catalog.tableExists(RAW):
    dbutils.notebook.exit("Build the pipeline in lesson 7.2 first")

# COMMAND ----------

bad_in_bronze = spark.table(RAW).filter("order_id IS NULL").count()

if bad_in_bronze == 0:
    path = feed.emit_batch("orders", ORDERS, batch_num=BROKEN_BATCH,
                           extra_fields={"order_id": None})
    print(f"Landed {path}: 20 orders with order_id = NULL")
    print("Now click Run file on files 01 and 02, then run this notebook again.")
    dbutils.notebook.exit("broken-batch-landed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second run: what the expectation did

# COMMAND ----------

n_raw = spark.table(RAW).count()
n_clean = spark.table(CLEAN).count()
print(f"bronze.sdp_orders_raw: {n_raw} rows, {bad_in_bronze} of them with no order_id")
print(f"silver.sdp_orders:     {n_clean} rows, none with no order_id")

assert bad_in_bronze == 20
assert n_raw == 1100
assert n_clean == 1080
assert spark.table(CLEAN).filter("order_id IS NULL").count() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC The streaming table already holds these 20 rows, so the file has done its job.
# MAGIC Removing it keeps the landing folder the same as lessons 5.5 and 5.6 left it. The
# MAGIC 20 rows stay in bronze as the evidence, which is what bronze is for.

# COMMAND ----------

broken = f"orders_feed_{BROKEN_BATCH:04d}.json"
if broken in [f.name for f in dbutils.fs.ls(ORDERS)]:
    dbutils.fs.rm(f"{ORDERS}/{broken}")
    print(f"removed {broken}")

assert broken not in [f.name for f in dbutils.fs.ls(ORDERS)]

# COMMAND ----------

dbutils.notebook.exit("broken-batch-verified")
