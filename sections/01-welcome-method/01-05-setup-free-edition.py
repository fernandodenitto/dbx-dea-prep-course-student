# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 1.5 — Hello, Lakehouse!
# MAGIC
# MAGIC Welcome to **Lakeshore Outfitters**' future data platform — a.k.a. your new
# MAGIC Databricks Free Edition workspace. You are running this from the course Git folder:
# MAGIC every notebook in this course stays where it sits in that folder.
# MAGIC
# MAGIC This notebook has one job: prove that everything works. Three cells, three checks.
# MAGIC
# MAGIC > **Note:** no cluster to create or attach — Free Edition runs on **serverless
# MAGIC compute**. The first run may take a few seconds while it spins up.

# COMMAND ----------

# Check 1 — Python runs on serverless compute
print("Hello from your lakehouse! 🏔️")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2 — Query real data with SQL
# MAGIC
# MAGIC Every workspace ships with the `samples` catalog. If this query returns taxi
# MAGIC rides, your SQL engine and catalog are both working.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   tpep_pickup_datetime,
# MAGIC   trip_distance,
# MAGIC   fare_amount
# MAGIC FROM samples.nyctaxi.trips
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3 — Who are you?
# MAGIC
# MAGIC `current_user()` returns the identity you're logged in as. Remember this function —
# MAGIC it comes back when we talk about permissions and row-level security.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS me;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ All three passed?
# MAGIC
# MAGIC You have a free, fully working lakehouse. See you in Section 2, where
# MAGIC we cover the concepts every data engineer needs — and generate Lakeshore
# MAGIC Outfitters' first data.
