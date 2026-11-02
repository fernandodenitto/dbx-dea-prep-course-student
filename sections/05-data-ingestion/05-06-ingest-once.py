# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest once (helper for lesson 5.6)
# MAGIC
# MAGIC One Auto Loader pass over the orders folder — the same ingestion as lessons
# MAGIC 5.5/5.6, isolated so a parent notebook (or a Lakeflow Job) can run it, watch
# MAGIC it fail on schema changes, and retry it. No try/except here on purpose:
# MAGIC failures must surface to the caller.

# COMMAND ----------

AL_BASE = "/Volumes/lakeshore/landing/files/_autoloader/orders"

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", f"{AL_BASE}/schema")
 .load("/Volumes/lakeshore/landing/files/orders")
 .selectExpr("*", "_metadata.file_path AS source_file")
 .writeStream
 .option("checkpointLocation", f"{AL_BASE}/checkpoint")
 .option("mergeSchema", "true")   # let the SINK table evolve too (see lesson)
 .outputMode("append")
 .trigger(availableNow=True)
 .toTable("lakeshore.bronze.orders")
 ).awaitTermination()

# COMMAND ----------

n = spark.table("lakeshore.bronze.orders").count()
dbutils.notebook.exit(f"ingested; orders={n}")
