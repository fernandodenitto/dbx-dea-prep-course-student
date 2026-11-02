# Databricks notebook source
# MAGIC %md
# MAGIC # 🚚 Lakeshore feeder
# MAGIC
# MAGIC Run this notebook **in a second browser tab** while lesson 5.5's notebook runs
# MAGIC its loop of ingestion passes in the first. It drops one order file per interval
# MAGIC into the landing volume, slower than a pass takes, so every pass finds something
# MAGIC new and you can watch the table grow.
# MAGIC
# MAGIC Keep the defaults: batches 1, 2 and 3 are the ones the rest of the course counts
# MAGIC on. The feed is deterministic, so running it again rewrites identical files and
# MAGIC Auto Loader ignores them. Do not raise the batch count: a batch 4 would change
# MAGIC every order total in the course.

# COMMAND ----------

dbutils.widgets.text("batches", "3", "Number of batches")
dbutils.widgets.text("interval_seconds", "45", "Seconds between batches")
dbutils.widgets.text("start_batch", "1", "First batch number")

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")           # make datagen.py and feed.py importable

import feed

feed.run_feed(
    entity="orders",
    volume_path="/Volumes/lakeshore/landing/files/orders",
    batches=int(dbutils.widgets.get("batches")),
    batch_size=20,
    interval_seconds=int(dbutils.widgets.get("interval_seconds")),
    start_batch=int(dbutils.widgets.get("start_batch")),
)

# COMMAND ----------

dbutils.notebook.exit("feed-complete")
