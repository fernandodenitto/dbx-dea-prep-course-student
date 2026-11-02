# Databricks notebook source
# MAGIC %md
# MAGIC # 8.2 — `build_summary`: the last task of `lakeshore-daily`
# MAGIC
# MAGIC Runs only when the condition task says yes. It reads what the two parallel roots
# MAGIC of the job refreshed — `gold.sdp_orders_by_day` from the pipeline task and
# MAGIC `bronze.fx_rates` from the REST task — and writes one row of headline numbers.
# MAGIC
# MAGIC Overwrite on purpose: a repair run re-runs a task from the beginning, so this
# MAGIC task has to be safe to run twice.

# COMMAND ----------

# MAGIC %md
# MAGIC The job passes `{{job.run_id}}` as the `run_id` parameter. Run the notebook by
# MAGIC hand and the widget's default is what you get, so it stays developable outside a
# MAGIC job.

# COMMAND ----------

dbutils.widgets.text("run_id", "interactive")
run_id = dbutils.widgets.get("run_id")
print(f"run_id = {run_id}")

# COMMAND ----------

from pyspark.sql import functions as F

totals = spark.table("lakeshore.gold.sdp_orders_by_day").agg(
    F.count("*").alias("n_days"),
    F.sum("n_orders").alias("n_orders"),
    F.round(F.sum("revenue"), 2).alias("revenue"),
    F.max("order_date").alias("last_order_date"),
)

eur_rate = (spark.table("lakeshore.bronze.fx_rates")
            .filter("currency = 'EUR'")
            .first()["rate"])

summary = (totals
           .withColumn("eur_rate", F.lit(eur_rate))
           .withColumn("revenue_eur", F.round(F.col("revenue") * eur_rate, 2))
           .withColumn("run_id", F.lit(run_id))
           .withColumn("built_at", F.current_timestamp()))

# COMMAND ----------

(summary.write.mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("lakeshore.gold.job_summary"))

display(spark.table("lakeshore.gold.job_summary"))

# COMMAND ----------

row = spark.table("lakeshore.gold.job_summary").first()
assert spark.table("lakeshore.gold.job_summary").count() == 1
assert row["n_days"] > 0 and row["revenue"] > 0, "the summary found no data"
print(f"{row['n_days']} days, {row['n_orders']} orders, revenue {row['revenue']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC On the course data this prints 139 days, 1,080 orders, revenue 854,427. The EUR
# MAGIC figure moves with the day's rate, so yours will differ from the video. Reading the
# MAGIC output of both roots is why this task sits after the fan-in.

# COMMAND ----------

dbutils.notebook.exit("build-summary-complete")
