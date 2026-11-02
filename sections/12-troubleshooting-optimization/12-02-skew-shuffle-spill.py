# Databricks notebook source
# MAGIC %md
# MAGIC # 12.2 — Salting a skewed aggregation
# MAGIC
# MAGIC The diagnosis in this lesson lives in the Spark UI, which serverless does not have. The
# MAGIC **remedy** is ordinary code, so that part we can run. We count rows per shuffle key,
# MAGIC because that is what decides how much work one task gets.

# COMMAND ----------

from pyspark.sql import functions as F

# Half of all rows belong to one wholesale account, customer 0.
orders = (spark.range(5_000_000)
          .withColumn("customer_id", F.when(F.col("id") % 2 == 0, F.lit(0)).otherwise(F.col("id") % 1000))
          .withColumn("amount", F.col("id") % 10))

def key_spread(df, *keys):
    """Rows per shuffle key: the biggest key, the median key, and how many keys there are."""
    return (df.groupBy(*keys).count()
            .agg(F.max("count").alias("max_rows"),
                 F.expr("percentile_approx(count, 0.5)").alias("median_rows"),
                 F.count("*").alias("keys"))
            .first())

before = key_spread(orders, "customer_id")
print(f"by customer_id:        max {before.max_rows:>9,}  median {before.median_rows:>6,}  keys {before.keys:>5,}")

# COMMAND ----------

# MAGIC %md
# MAGIC One key holds 500 times the median. One task would get all of it, and adaptive execution
# MAGIC would not help: its skew handling is for joins, not for `groupBy`.
# MAGIC
# MAGIC ## Add a salt, so the hot key spreads across 16 tasks

# COMMAND ----------

salted = orders.withColumn("salt", F.pmod(F.xxhash64("id"), F.lit(16)))

after = key_spread(salted, "customer_id", "salt")
print(f"by customer_id + salt: max {after.max_rows:>9,}  median {after.median_rows:>6,}  keys {after.keys:>5,}")

assert after.max_rows < before.max_rows / 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate twice, and get the same answer
# MAGIC
# MAGIC First by key and salt, where the work spreads. Then by key alone, over far fewer rows.

# COMMAND ----------

plain = orders.groupBy("customer_id").agg(F.sum("amount").alias("total"))

two_step = (salted.groupBy("customer_id", "salt").agg(F.sum("amount").alias("partial"))
            .groupBy("customer_id").agg(F.sum("partial").alias("total")))

differences = plain.exceptAll(two_step).count() + two_step.exceptAll(plain).count()
print("rows that differ between the two answers:", differences)
assert differences == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Skewed `groupBy`: salt the key and aggregate in two steps, or pre-aggregate · skewed join:
# MAGIC adaptive execution splits the partition first, thresholds next · a sum survives two steps
# MAGIC unchanged, and an average needs sum and count carried separately.

# COMMAND ----------

dbutils.notebook.exit("salting-complete")
