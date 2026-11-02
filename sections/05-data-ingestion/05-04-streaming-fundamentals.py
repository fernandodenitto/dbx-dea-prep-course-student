# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming fundamentals
# MAGIC
# MAGIC A stream treats data as an **unbounded table**: rows keep arriving, the
# MAGIC query keeps processing — in **micro-batches**, with a **checkpoint** so it
# MAGIC can die and resume exactly where it stopped. We learn the moving parts on a
# MAGIC Delta table we already trust: `bronze.customers`.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lakeshore;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# Start clean (safe re-runs): remove demo leftovers from any previous attempt
spark.sql("DELETE FROM customers WHERE customer_id = 'C99999'")
spark.sql("DROP TABLE IF EXISTS customers_stream_demo")
dbutils.fs.rm("/Volumes/lakeshore/landing/files/_checkpoints/customers_demo",
              recurse=True)
dbutils.fs.rm("/Volumes/lakeshore/landing/files/_checkpoints/customers_demo_rejected",
              recurse=True)
print("clean start")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. readStream: a Delta table as a streaming source

# COMMAND ----------

from pyspark.sql.functions import col, split

CHECKPOINT = "/Volumes/lakeshore/landing/files/_checkpoints/customers_demo"

stream_df = (spark.readStream
             .table("lakeshore.bronze.customers")
             .withColumn("email_domain", split(col("email"), "@")[1]))

# COMMAND ----------

# MAGIC %md
# MAGIC Nothing ran yet — a streaming DataFrame is a *plan*. Execution starts when
# MAGIC we attach a **writeStream** with three decisions the exam cares about:
# MAGIC
# MAGIC - **checkpointLocation** — the stream's memory: which data was processed,
# MAGIC   committed exactly-once. Unique per stream, never shared.
# MAGIC - **outputMode** — `append` (new rows only; our case), `complete`
# MAGIC   (recompute full result, for aggregations), `update` (changed rows).
# MAGIC - **trigger** — *when* micro-batches fire: `availableNow=True` (process
# MAGIC   everything pending, then STOP: incremental batch!), or `processingTime='10
# MAGIC   seconds'` (stay alive on a cadence). The second needs classic compute, as
# MAGIC   section 3 of this notebook proves.

# COMMAND ----------

(stream_df.writeStream
 .option("checkpointLocation", CHECKPOINT)
 .outputMode("append")
 .trigger(availableNow=True)
 .toTable("lakeshore.bronze.customers_stream_demo")
 ).awaitTermination()

n = spark.table("lakeshore.bronze.customers_stream_demo").count()
print(f"customers_stream_demo: {n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC 200 rows flowed through. `availableNow` processed all pending data and shut
# MAGIC down — remember lesson 2.5: *an incremental batch job is a stream that stops
# MAGIC when it catches up*. This is that sentence, running.
# MAGIC
# MAGIC ## 2. The checkpoint is the memory
# MAGIC
# MAGIC Run the same stream again — the checkpoint says "nothing new":

# COMMAND ----------

(spark.readStream.table("lakeshore.bronze.customers")
 .withColumn("email_domain", split(col("email"), "@")[1])
 .writeStream
 .option("checkpointLocation", CHECKPOINT)
 .outputMode("append")
 .trigger(availableNow=True)
 .toTable("lakeshore.bronze.customers_stream_demo")
 ).awaitTermination()

print(f"after re-run: {spark.table('lakeshore.bronze.customers_stream_demo').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC Still 200 — no reprocessing, no duplicates. **Exactly-once**, courtesy of
# MAGIC the checkpoint. Now feed the source one new row and run again:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Name the columns! COPY INTO's JSON inference ordered them alphabetically,
# MAGIC -- so positional VALUES would scramble the fields — a real bug we hit while
# MAGIC -- building this course.
# MAGIC INSERT INTO customers (customer_id, first_name, last_name, email, created_at)
# MAGIC VALUES ('C99999', 'Stream', 'Test', 'stream.test@example.com',
# MAGIC         '2026-07-01T00:00:00');

# COMMAND ----------

(spark.readStream.table("lakeshore.bronze.customers")
 .withColumn("email_domain", split(col("email"), "@")[1])
 .writeStream
 .option("checkpointLocation", CHECKPOINT)
 .outputMode("append")
 .trigger(availableNow=True)
 .toTable("lakeshore.bronze.customers_stream_demo")
 ).awaitTermination()

print(f"after new row: {spark.table('lakeshore.bronze.customers_stream_demo').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC 201: the stream picked up **only** the new row. That's incremental
# MAGIC processing in one picture — and the exact machinery Auto Loader builds on,
# MAGIC next lesson.
# MAGIC
# MAGIC ## 3. The trigger serverless refuses
# MAGIC
# MAGIC The exam still asks about `processingTime`, so here is what it does on the
# MAGIC compute you are using right now. Leaving the trigger out fails the same way,
# MAGIC because the default is `processingTime` with a 0-second interval.

# COMMAND ----------

try:
    query = (stream_df.writeStream
             .option("checkpointLocation", f"{CHECKPOINT}_rejected")
             .trigger(processingTime="10 seconds")
             .toTable("lakeshore.bronze.customers_stream_demo"))
    query.stop()     # on classic compute it starts; stop it so the demo stays a demo
    print("processingTime started: you are on classic compute")
except Exception as e:
    assert "INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED" in str(e), e
    print("Rejected on serverless: INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED")

# COMMAND ----------

# MAGIC %md
# MAGIC On serverless notebooks and jobs, use `availableNow`. For a stream that stays
# MAGIC alive on serverless, the docs point to a Lakeflow pipeline in continuous mode
# MAGIC (Section 7).
# MAGIC
# MAGIC ## 4. Cleanup (demo objects only)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM customers WHERE customer_id = 'C99999';
# MAGIC DROP TABLE IF EXISTS customers_stream_demo;

# COMMAND ----------

dbutils.fs.rm(CHECKPOINT, recurse=True)
dbutils.fs.rm(f"{CHECKPOINT}_rejected", recurse=True)
print("demo cleaned up — bronze.customers back to 200 rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Stream = query on an unbounded table, processed in micro-batches ·
# MAGIC checkpoint = exactly-once memory (unique per stream!) · outputMode:
# MAGIC append/complete/update · trigger: availableNow = catch up and stop (the one
# MAGIC serverless accepts); processingTime and the default = stay running, classic
# MAGIC compute only · Delta tables work as streaming sources
# MAGIC and sinks.

# COMMAND ----------

dbutils.notebook.exit("streaming-fundamentals-complete")
