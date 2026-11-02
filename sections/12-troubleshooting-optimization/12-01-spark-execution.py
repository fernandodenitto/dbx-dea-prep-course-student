# Databricks notebook source
# MAGIC %md
# MAGIC # 12.1 — Spark execution: jobs, stages, tasks
# MAGIC
# MAGIC The hierarchy: an **action** (count, write, display) triggers a **job**; a job splits
# MAGIC into **stages** at every *shuffle boundary*; each stage runs parallel **tasks**, one per
# MAGIC partition.
# MAGIC
# MAGIC Honest constraint: **the Spark UI does not exist on serverless**, and Free Edition is
# MAGIC serverless only. So here we read the same structure in two places we do have: the query
# MAGIC plan, and the query profile behind each cell's **See performance** link.

# COMMAND ----------

import contextlib
import io
import re
import time

from pyspark.sql.functions import col

def plan_of(df):
    """The physical plan as text, so that we can read it and assert on it."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()
    return buffer.getvalue()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A narrow job: no shuffle
# MAGIC
# MAGIC Filters and column math touch each partition independently.

# COMMAND ----------

big = spark.range(20_000_000).withColumn("key", col("id") % 1000)

narrow = big.filter(col("id") % 7 == 0).withColumn("double", col("id") * 2)
print(f"narrow result: {narrow.count():,} rows")

narrow_plan = plan_of(narrow)
print(narrow_plan)
assert "Exchange" not in narrow_plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. A wide job: a shuffle, and therefore a stage boundary
# MAGIC
# MAGIC `groupBy` needs rows with the same key on the same machine. Look for the **exchange** in
# MAGIC the plan. On this Photon compute it is spelled `PhotonShuffleExchangeSink` and `Source`.

# COMMAND ----------

wide = big.groupBy("key").count()
print(f"wide result: {wide.count():,} groups")

wide_plan = plan_of(wide)
print(wide_plan)
assert "ShuffleExchange" in wide_plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A join: the small side gets broadcast
# MAGIC
# MAGIC The lookup is 1,000 generated rows. Instead of shuffling 20 million rows to meet it, Spark
# MAGIC ships the lookup to every task. That is the `BroadcastHashJoin` from lessons 6.3 and 6.9.

# COMMAND ----------

lookup = spark.range(1000).withColumnRenamed("id", "key") \
    .withColumn("label", col("key") % 10)

joined = big.join(lookup, on="key").groupBy("label").count()
print(f"joined result: {joined.count():,} labels")

join_plan = plan_of(joined)
print(join_plan)
assert "BroadcastHashJoin" in join_plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Now open the query profile
# MAGIC
# MAGIC Under the `groupBy` cell above click **See performance**, pick the statement, then **See
# MAGIC query profile**. Find the operators you just read (Scan or Range, Shuffle, the
# MAGIC aggregation), then time spent and rows on each, and in the details panel wall-clock
# MAGIC duration against aggregated task time.
# MAGIC
# MAGIC What you will **not** find here: stages and per-task min, median and max. Those live in
# MAGIC the Spark UI on classic compute. We describe that screen in the lesson, because the exam
# MAGIC asks about it.
# MAGIC
# MAGIC ## 5. Change one thing, re-measure
# MAGIC
# MAGIC The one tuning knob serverless lets us set is `spark.sql.shuffle.partitions`. Its default
# MAGIC here is `auto`. We change it, read the plan, time the query, and put it back.

# COMMAND ----------

print("default:", spark.conf.get("spark.sql.shuffle.partitions"))

def measure(partitions):
    """Run one aggregation at this setting: partitions in the initial plan, and seconds."""
    spark.conf.set("spark.sql.shuffle.partitions", partitions)
    df = big.groupBy((col("id") % 100000).alias("k")).count()
    planned = re.search(r"hashpartitioning\(.*?, (\d+)\)", plan_of(df)).group(1)
    start = time.time()
    df.count()
    return planned, round(time.time() - start, 2)

results = {p: measure(p) for p in ["auto", "2000", "4"]}
spark.conf.set("spark.sql.shuffle.partitions", "auto")

for setting, (planned, seconds) in results.items():
    print(f"{setting:>5}: {planned:>5} partitions in the initial plan, {seconds} s")

assert results["2000"][0] == "2000" and results["4"][0] == "4"

# COMMAND ----------

# MAGIC %md
# MAGIC The setting reaches the plan. The clock barely moves on a workload this small, and that is
# MAGIC the honest result: adaptive execution adjusts partitions at runtime. Measure before you
# MAGIC believe a knob.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC action → job → stages (cut at shuffles) → parallel tasks (one per partition) · narrow ops
# MAGIC share a stage, wide ops (groupBy, join, distinct) add a boundary · on serverless you read
# MAGIC this in the plan and the query profile · the Spark UI is classic compute only, and the
# MAGIC exam still asks you to interpret it.

# COMMAND ----------

dbutils.notebook.exit("spark-execution-complete")
