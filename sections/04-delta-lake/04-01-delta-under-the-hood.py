# Databricks notebook source
# MAGIC %md
# MAGIC # Delta under the hood
# MAGIC
# MAGIC Time to create our first table — and watch what Delta Lake actually does
# MAGIC underneath. We use a small **lab table** we can abuse freely; the real
# MAGIC Lakeshore tables start in the next lesson.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lakeshore;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a table — Delta by default
# MAGIC
# MAGIC No `USING DELTA` needed: **Delta Lake is the default format** for every table
# MAGIC on Databricks. (`CREATE OR REPLACE` makes this notebook safe to re-run.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_lab (
# MAGIC   id    INT,
# MAGIC   gear  STRING,
# MAGIC   price DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO delta_lab VALUES
# MAGIC   (1, 'Trail Map',     9.90),
# MAGIC   (2, 'Carabiner',    14.50),
# MAGIC   (3, 'Water Bottle', 19.00);
# MAGIC
# MAGIC SELECT * FROM delta_lab ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. UPDATE and DELETE — the operations data lakes couldn't do
# MAGIC
# MAGIC On a plain file-based data lake, updating one row reliably was somewhere
# MAGIC between painful and impossible. On Delta, it's a transaction:

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta_lab SET price = 12.50 WHERE id = 2;
# MAGIC DELETE FROM delta_lab WHERE id = 3;
# MAGIC
# MAGIC SELECT * FROM delta_lab ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The transaction log, made visible
# MAGIC
# MAGIC Every one of those statements was a **commit** to the transaction log.
# MAGIC `DESCRIBE HISTORY` reads the log back: one **version** per operation, with
# MAGIC who, when, and what.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE DETAIL` shows the table's physical facts: format (`delta`), number
# MAGIC of data files, size, and the storage **location**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Can we peek at the files directly? (an honest experiment)
# MAGIC
# MAGIC Older courses browse the table folder and show you the Parquet files and
# MAGIC `_delta_log/`. Let's try that on our Unity Catalog **managed** table:

# COMMAND ----------

location = (spark.sql("DESCRIBE DETAIL delta_lab").first()["location"])
print(f"Table location: {location}\n")

try:
    for f in dbutils.fs.ls(location):
        print(f.name)
    log_listing = "allowed"
except Exception as e:
    log_listing = "blocked"
    print("Direct file access is BLOCKED — and that's a feature:")
    print(str(e)[:300], "…")

# COMMAND ----------

# MAGIC %md
# MAGIC If the listing was **blocked**: Unity Catalog protects managed-table storage —
# MAGIC access goes through the *table*, with governance, not through raw paths. Old
# MAGIC courses could browse freely because they used the legacy Hive metastore on
# MAGIC open storage. The files are still exactly what we described — Parquet data
# MAGIC plus a `_delta_log/` folder of JSON commits — you've just seen the log through
# MAGIC its proper window: `DESCRIBE HISTORY`.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Tables are Delta by default · INSERT/UPDATE/DELETE are transactions ·
# MAGIC every operation = a version in the log · `DESCRIBE HISTORY` / `DESCRIBE
# MAGIC DETAIL` are your X-ray · UC managed storage is governed, not browsable.
# MAGIC We keep `delta_lab` around — lesson 4.4 makes a mess with it on purpose.

# COMMAND ----------

dbutils.notebook.exit(f"delta-under-the-hood-complete; log_listing={log_listing}")
