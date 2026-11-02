# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 2.6 — Lakeshore Setup: namespace + first data
# MAGIC
# MAGIC In this notebook we:
# MAGIC 1. Create the **Lakeshore namespace**: one catalog, four schemas, one volume.
# MAGIC 2. Generate the first batch of data with `datagen.py` and land it as **files**
# MAGIC    (JSON, CSV, TSV) in the volume — exactly how raw data arrives in real life.
# MAGIC
# MAGIC > **Before running:** open this notebook from its place in your course Git folder
# MAGIC > (lesson 1.5). `datagen.py` lives in `datagen/`, and the generation cell
# MAGIC > walks up from here to find it.
# MAGIC
# MAGIC Safe to re-run: everything is `IF NOT EXISTS`, and file writes are idempotent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The namespace
# MAGIC
# MAGIC Unity Catalog organizes everything in a **three-level namespace**:
# MAGIC `catalog.schema.object`. We create one catalog for the whole course and one
# MAGIC schema per medallion layer — plus `landing` for raw files.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS lakeshore
# MAGIC COMMENT 'Lakeshore Outfitters — course data platform';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS lakeshore.landing COMMENT 'Raw incoming files';
# MAGIC CREATE SCHEMA IF NOT EXISTS lakeshore.bronze  COMMENT 'Raw data, as ingested';
# MAGIC CREATE SCHEMA IF NOT EXISTS lakeshore.silver  COMMENT 'Cleaned and validated';
# MAGIC CREATE SCHEMA IF NOT EXISTS lakeshore.gold    COMMENT 'Business-level aggregates';

# COMMAND ----------

# MAGIC %md
# MAGIC A **volume** is the Unity Catalog object for *files* (tables are for rows).
# MAGIC Files in a volume live under the path `/Volumes/<catalog>/<schema>/<volume>/`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS lakeshore.landing.files
# MAGIC COMMENT 'Landing zone for raw Lakeshore files';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate the data
# MAGIC
# MAGIC `datagen.py` is plain Python — standard library only, fully deterministic
# MAGIC (seed 2026), with referential integrity built in. Open it and read it: it IS
# MAGIC course material, not hidden plumbing. Find it in your Git folder under
# MAGIC `datagen/`.
# MAGIC
# MAGIC The first lines of the next cell make it importable: Python only imports from folders
# MAGIC on `sys.path`, and `datagen` isn't one of them until we add it.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")           # make datagen.py and feed.py importable

from datagen import generate_all, write_csv, write_json

data = generate_all(seed=2026, n_customers=200, n_orders=1000)
{entity: len(rows) for entity, rows in data.items()}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Land the files
# MAGIC
# MAGIC Real sources don't hand you tidy tables — they hand you **files in mixed
# MAGIC formats**. We reproduce that on purpose:
# MAGIC
# MAGIC | Entity | Format | Why |
# MAGIC |---|---|---|
# MAGIC | customers | JSON Lines | typical app-database export |
# MAGIC | orders | JSON Lines (nested `items` array) | semi-structured — the fun one |
# MAGIC | addresses | TSV | the classic awkward legacy feed |
# MAGIC | products | CSV | reference data |
# MAGIC | payments | CSV | payment-provider style export |
# MAGIC | refunds | CSV | support-tool export (with a packed `reason` column) |

# COMMAND ----------

LANDING = "/Volumes/lakeshore/landing/files"

targets = {
    "customers": (write_json, "customers_batch_001.json"),
    "orders":    (write_json, "orders_batch_001.json"),
    "addresses": (write_csv,  "addresses_batch_001.tsv"),
    "products":  (write_csv,  "products.csv"),
    "payments":  (write_csv,  "payments_batch_001.csv"),
    "refunds":   (write_csv,  "refunds_batch_001.csv"),
}

for entity, (writer, filename) in targets.items():
    folder = f"{LANDING}/{entity}"
    os.makedirs(folder, exist_ok=True)
    if writer is write_csv and filename.endswith(".tsv"):
        writer(data[entity], f"{folder}/{filename}", delimiter="\t")
    else:
        writer(data[entity], f"{folder}/{filename}")
    print(f"✓ {entity:<10} → {folder}/{filename} ({len(data[entity])} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Look at what landed
# MAGIC
# MAGIC You can browse these files in **Catalog Explorer** too:
# MAGIC `lakeshore → landing → files`.

# COMMAND ----------

for entity in targets:
    for f in dbutils.fs.ls(f"{LANDING}/{entity}"):
        print(f"{f.path}  ({f.size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC Peek at one raw file — three lines of the orders JSON. Note the nested
# MAGIC `items` array: we'll be working with it a lot.

# COMMAND ----------

with open(f"{LANDING}/orders/orders_batch_001.json") as f:
    for _ in range(3):
        print(f.readline().strip()[:160], "…")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done
# MAGIC
# MAGIC The Lakeshore namespace exists and raw files are sitting in the landing zone —
# MAGIC in three different formats, like real life. **No tables yet**: that's exactly
# MAGIC where the course goes next (bronze, in Section 4 and 5).

# COMMAND ----------

dbutils.notebook.exit("setup-complete")
