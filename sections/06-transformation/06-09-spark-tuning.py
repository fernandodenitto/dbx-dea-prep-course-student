# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.9 — Spark tuning: change one thing, re-measure
# MAGIC
# MAGIC The guide says "understand the tuning parameters **and re-measure the
# MAGIC performance**". Three of the four parameters can't be set on serverless. One can.
# MAGIC So we do the part we can do for real: change it, run the same query, compare.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Which knobs does serverless give you?

# COMMAND ----------

print(f"spark.sql.shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")

for knob, value in [("spark.sql.shuffle.partitions", "auto"),
                    ("spark.sql.autoBroadcastJoinThreshold", "10m"),
                    ("spark.default.parallelism", "8"),
                    ("spark.executor.memory", "4g"),
                    ("spark.driver.memory", "4g")]:
    try:
        spark.conf.set(knob, value)
        outcome = "settable"
    except Exception as e:
        outcome = "CONFIG_NOT_AVAILABLE" if "CONFIG_NOT_AVAILABLE" in str(e) else str(e)[:80]
    print(f"{knob:40} {outcome}")
    assert outcome == ("settable" if knob == "spark.sql.shuffle.partitions"
                       else "CONFIG_NOT_AVAILABLE"), f"{knob}: {outcome}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The experiment
# MAGIC
# MAGIC Lakeshore's 2,602 order lines can't move a stopwatch, so this one query is synthetic:
# MAGIC 30 million rows, 200,000 keys, one `groupBy`. Same query every time, one setting
# MAGIC changed. Three things recorded:
# MAGIC
# MAGIC - **planned**: the partition count in the plan, read from `EXPLAIN`
# MAGIC - **actual**: how many partitions really held output, from `spark_partition_id()`
# MAGIC - **seconds**: wall clock, and it will differ on your screen

# COMMAND ----------

import re
import time
from pyspark.sql.functions import countDistinct, spark_partition_id

SQL = """
SELECT k, sum(id) AS total
FROM (SELECT id, id % 200000 AS k FROM range(30000000))
GROUP BY k
"""

def measure(setting):
    """Set one knob, run one query, report what the plan and the run actually did."""
    spark.conf.set("spark.sql.shuffle.partitions", str(setting))
    plan = spark.sql(f"EXPLAIN {SQL}").first()[0]
    planned = int(re.search(r"hashpartitioning\([^,]+, (\d+)\)", plan).group(1))

    start = time.perf_counter()
    actual = (spark.sql(SQL).withColumn("p", spark_partition_id())
              .agg(countDistinct("p")).first()[0])
    return {"setting": setting, "planned": planned, "actual": actual,
            "seconds": round(time.perf_counter() - start, 2)}

results = [measure(s) for s in ("auto", 1, 8, 4000, "auto")]
spark.conf.set("spark.sql.shuffle.partitions", "auto")      # put it back

for r in results:
    print(f"{str(r['setting']):>6}  planned {r['planned']:>5}  actual {r['actual']:>5}  {r['seconds']:>6} s")

# COMMAND ----------

by_setting = {r["setting"]: r for r in results}
assert by_setting[1]["planned"] == 1 and by_setting[1]["actual"] == 1
assert by_setting[4000]["planned"] == 4000
assert by_setting[4000]["actual"] < 4000, "AQE should have coalesced 4000 partitions"
print(f"asked for 4000 → planned {by_setting[4000]['planned']} "
      f"→ ran with {by_setting[4000]['actual']}: AQE coalesced them")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reading the result
# MAGIC
# MAGIC Measured on 1 November 2026:
# MAGIC
# MAGIC | Setting | Planned | Actual | Seconds |
# MAGIC |---|---|---|---|
# MAGIC | `auto` | 228 | 9 | 0.6–1.0 |
# MAGIC | 1 | 1 | 1 | 1.2 |
# MAGIC | 8 | 8 | 8 | 0.7 |
# MAGIC | 4000 | 4000 | 8 | 0.7 |
# MAGIC
# MAGIC - The **setting reached the plan**: you asked for 4000, the plan says 4000.
# MAGIC - **AQE overruled it at run time**: 4000 planned became 8 actual. That's why "hand-tune
# MAGIC   the 200" is usually the wrong exam answer.
# MAGIC - **You can still make it worse**: 1 partition is the one setting AQE can't rescue,
# MAGIC   and it's the slowest. It can merge partitions; it won't split your one partition here.
# MAGIC
# MAGIC The other half of re-measuring is reading the plan and the **query profile**. There
# MAGIC is no Spark UI on serverless, which means none on Free Edition. Section 12 covers both.

# COMMAND ----------

dbutils.notebook.exit("spark-tuning-complete")
