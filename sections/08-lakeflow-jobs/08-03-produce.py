# Databricks notebook source
# MAGIC %md
# MAGIC # 8.3 — Task values: the producer
# MAGIC
# MAGIC This notebook is the FIRST task of a two-task job. Its whole job is to compute
# MAGIC something and hand it to the next task. It never writes a table to do it.

# COMMAND ----------

row_count = spark.table("lakeshore.silver.orders").count()
threshold = 500
verdict = "large" if row_count >= threshold else "small"

print(f"silver.orders holds {row_count} rows -> {verdict}")

# COMMAND ----------

# MAGIC %md
# MAGIC Two values, set by key. Values must be JSON-serialisable, and the JSON
# MAGIC representation cannot exceed **48 KiB** — this is a channel for a number, a
# MAGIC status or a small list, never for data.

# COMMAND ----------

dbutils.jobs.taskValues.set(key="row_count", value=row_count)
dbutils.jobs.taskValues.set(key="verdict", value=verdict)

print("task values set: row_count, verdict")
