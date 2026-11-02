# Databricks notebook source
# MAGIC %md
# MAGIC # One failOnNewColumns pass (helper for lesson 5.6)
# MAGIC
# MAGIC One Auto Loader pass over the lesson's demo folder, in `failOnNewColumns` mode. It's
# MAGIC a separate notebook because a stream that fails also fails the cell that started
# MAGIC it, even inside a try/except. Running it as a child lets the parent watch it fail.

# COMMAND ----------

DEMO = "/Volumes/lakeshore/landing/files/_schema_demo/fail"

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", f"{DEMO}/schema")
 .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
 .load(f"{DEMO}/in")
 .writeStream
 .option("checkpointLocation", f"{DEMO}/checkpoint")
 .trigger(availableNow=True)
 .toTable("lakeshore.bronze.schema_demo_fail")
 ).awaitTermination()

# COMMAND ----------

dbutils.notebook.exit("no new column")
