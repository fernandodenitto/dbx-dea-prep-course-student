# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.5 — Deduplication, aggregations, and the pivot
# MAGIC
# MAGIC The counting lesson: the aggregation functions the exam names
# MAGIC (`count`, `approx_count_distinct`, `mean`, `summary`), the dedup verbs, and
# MAGIC the pivot that finally moves addresses to silver.

# COMMAND ----------

from pyspark.sql.functions import (col, count, countDistinct,
                                   approx_count_distinct, avg, sum as f_sum)

items = spark.table("lakeshore.silver.order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The counting family

# COMMAND ----------

items.agg(
    count("*").alias("item_rows"),
    countDistinct("order_id").alias("orders_exact"),
    approx_count_distinct("order_id").alias("orders_approx"),
    avg("line_total").alias("mean_line_total"),
    f_sum("line_total").alias("revenue"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC `approx_count_distinct` trades a small error (about 5% by default, tunable with
# MAGIC `relativeSD`) for a fraction of the cost of exact distinct counting on huge data —
# MAGIC the exam wants you to know *when* it's the right answer: dashboards and
# MAGIC monitoring yes, financial reconciliation no.
# MAGIC
# MAGIC Per-group is the same family after `groupBy`:

# COMMAND ----------

(items.groupBy("product_id")
 .agg(f_sum("quantity").alias("units_sold"),
      f_sum("line_total").alias("revenue"))
 .orderBy(col("revenue").desc())
 .show(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `summary()` — the one-line profiler

# COMMAND ----------

items.select("quantity", "unit_price", "line_total").summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dedup, the PySpark way
# MAGIC
# MAGIC Lesson 6.1 deduped with a window + QUALIFY (control over WHICH row wins).
# MAGIC PySpark's quick verbs, on a deliberately doubled sample:

# COMMAND ----------

sample = items.limit(10)
doubled = sample.unionByName(sample)
print(f"doubled: {doubled.count()} → distinct(): {doubled.distinct().count()}"
      f" → dropDuplicates(['order_id','product_id']): "
      f"{doubled.dropDuplicates(['order_id','product_id']).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC `distinct()` = whole-row duplicates; `dropDuplicates([keys])` = keyed dedup,
# MAGIC but you DON'T control which row survives — when the winner matters (latest
# MAGIC timestamp!), use the window pattern from 6.1.
# MAGIC
# MAGIC ## 4. The pivot: addresses to silver
# MAGIC
# MAGIC Bronze addresses are LONG: up to two rows per customer (shipping/billing).
# MAGIC Analytics wants WIDE: one row per customer. That's a pivot — here as
# MAGIC conditional aggregation, the form that shows the mechanics:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.addresses AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   max(CASE WHEN address_type = 'shipping' THEN street   END) AS shipping_street,
# MAGIC   max(CASE WHEN address_type = 'shipping' THEN city     END) AS shipping_city,
# MAGIC   max(CASE WHEN address_type = 'shipping' THEN state    END) AS shipping_state,
# MAGIC   max(CASE WHEN address_type = 'shipping' THEN postcode END) AS shipping_postcode,
# MAGIC   max(CASE WHEN address_type = 'billing'  THEN street   END) AS billing_street,
# MAGIC   max(CASE WHEN address_type = 'billing'  THEN city     END) AS billing_city,
# MAGIC   max(CASE WHEN address_type = 'billing'  THEN state    END) AS billing_state,
# MAGIC   max(CASE WHEN address_type = 'billing'  THEN postcode END) AS billing_postcode
# MAGIC FROM lakeshore.bronze.addresses
# MAGIC GROUP BY customer_id;
# MAGIC
# MAGIC SELECT count(*) AS customers_with_addresses,
# MAGIC        count_if(billing_street IS NULL) AS shipping_only
# MAGIC FROM lakeshore.silver.addresses;

# COMMAND ----------

# MAGIC %md
# MAGIC Each `max(CASE WHEN …)` picks the value from the matching row-type — long
# MAGIC becomes wide, one row per customer, billing columns NULL where the customer
# MAGIC has none. PySpark's `.groupBy("customer_id").pivot("address_type")` does
# MAGIC the same with less typing and less visibility — know both, prefer whichever
# MAGIC your team reads faster.

# COMMAND ----------

n = spark.table("lakeshore.silver.addresses").count()
assert n == 200, f"expected 200, got {n}"
print("✓ silver.addresses: one wide row per customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC count / countDistinct / approx_count_distinct (cheap, about 5% error by default,
# MAGIC tunable with relativeSD — know when it's acceptable) · mean/avg + summary() for profiling · distinct() vs
# MAGIC dropDuplicates(keys) vs window-QUALIFY (only the window controls the
# MAGIC winner) · pivot = conditional aggregation: long → wide.

# COMMAND ----------

dbutils.notebook.exit("dedup-aggregations-complete")
