# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.2 — Bronze → silver in PySpark: the DataFrame API
# MAGIC
# MAGIC Same job as last lesson, other language. Payments need cleaning: a string
# MAGIC timestamp and a cryptic numeric `status_code`. We fix both in PySpark —
# MAGIC because the exam shows code in SQL when possible and in Python otherwise, the
# MAGIC guide's skills talk about DataFrames, and real teams switch between the two.

# COMMAND ----------

from pyspark.sql.functions import col, when, to_timestamp, to_date

payments_bronze = spark.table("lakeshore.bronze.payments")
payments_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC The Rosetta stone — every SQL idea has a DataFrame twin:
# MAGIC
# MAGIC | SQL | PySpark |
# MAGIC |---|---|
# MAGIC | `SELECT cols` | `.select(...)` |
# MAGIC | `SELECT * EXCEPT (col)` | `.drop(col)` |
# MAGIC | `col AS new_name` | `.withColumnRenamed(old, new)` |
# MAGIC | `WHERE` | `.filter(...)` / `.where(...)` |
# MAGIC | new/changed column | `.withColumn(name, expr)` |
# MAGIC | `CASE WHEN` | `when(...).otherwise(...)` |
# MAGIC | `CAST` | `.cast(...)` / `to_timestamp(...)` |
# MAGIC | `GROUP BY` | `.groupBy().agg(...)` |
# MAGIC | write table | `.write.saveAsTable(...)` |

# COMMAND ----------

payments_silver = (
    payments_bronze
    .withColumn("payment_ts", to_timestamp(col("payment_ts")))
    .withColumn("payment_date", to_date(col("payment_ts")))
    .withColumn("status",
                when(col("status_code") == 1, "completed")
                .when(col("status_code") == 2, "pending")
                .when(col("status_code") == 3, "failed")
                .otherwise("unknown"))
    .select("payment_id", "order_id", "payment_ts", "payment_date",
            "amount", "method", "status")
)

payments_silver.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Three things to notice. **Nothing has executed yet** except the `show` —
# MAGIC DataFrame operations are *lazy*: they build a plan, and an action (show,
# MAGIC count, write) triggers it. **`when/otherwise`** is CASE WHEN with the same
# MAGIC catch-all discipline — the `otherwise("unknown")` means a future status
# MAGIC code 4 won't silently become NULL. And `status_code` is gone because the
# MAGIC `select` didn't name it: silver speaks business language, not source codes.
# MAGIC
# MAGIC `select` is one way to lose a column. The explicit verbs, on a throwaway
# MAGIC DataFrame (nothing is written here):

# COMMAND ----------

verbs = (payments_bronze
         .drop("status_code")                              # remove a column by name
         .withColumnRenamed("method", "payment_method")    # rename one column
         .filter(col("amount") > 100))                     # keep some rows

print(verbs.columns)
print(f"payments over 100: {verbs.count()} of {payments_bronze.count()}")
assert "status_code" not in verbs.columns and "payment_method" in verbs.columns
assert verbs.count() < payments_bronze.count()

# COMMAND ----------

(payments_silver.write
 .mode("overwrite")
 .saveAsTable("lakeshore.silver.payments"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, count(*) AS n, round(sum(amount), 2) AS total_amount
# MAGIC FROM lakeshore.silver.payments
# MAGIC GROUP BY status ORDER BY n DESC;

# COMMAND ----------

n = spark.table("lakeshore.silver.payments").count()
assert n == 1000, f"expected 1000, got {n}"
print("✓ silver.payments: typed, decoded, business-readable")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC DataFrames are lazy plans; actions execute · `withColumn` + `when/otherwise`
# MAGIC = the cleaning workhorses · `drop`, `withColumnRenamed`, `filter` do what they
# MAGIC say · `.write.mode("overwrite").saveAsTable()` = the
# MAGIC batch-refresh write · same medallion contract as SQL — pick the language per
# MAGIC team, not per religion.

# COMMAND ----------

dbutils.notebook.exit("pyspark-essentials-complete")
