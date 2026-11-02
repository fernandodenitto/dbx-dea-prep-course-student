# Databricks notebook source
# MAGIC %md
# MAGIC # Cloning: deep vs shallow
# MAGIC
# MAGIC "Can I get a copy of prod to test on?" — the most asked question in data
# MAGIC engineering. Delta has two answers with very different price tags.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lakeshore;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. DEEP CLONE — a full, independent copy
# MAGIC
# MAGIC Copies **metadata AND data files**. After this, the clone and the source
# MAGIC share nothing.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE products_test DEEP CLONE products;
# MAGIC
# MAGIC SELECT count(*) AS cloned_rows FROM products_test;

# COMMAND ----------

# MAGIC %md
# MAGIC Prove the independence — vandalize the clone, check the source:

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE products_test SET unit_price = 1.00;
# MAGIC
# MAGIC SELECT
# MAGIC   (SELECT round(avg(unit_price), 2) FROM products_test) AS clone_avg_price,
# MAGIC   (SELECT round(avg(unit_price), 2) FROM products)      AS source_avg_price;

# COMMAND ----------

# MAGIC %md
# MAGIC Clone trashed, source untouched. That's your **staging/test copy** pattern —
# MAGIC at the cost of duplicating storage.
# MAGIC
# MAGIC ## 2. SHALLOW CLONE — metadata only, data borrowed
# MAGIC
# MAGIC Copies just the **metadata**; data files still belong to the source. Creation
# MAGIC is near-instant and (initially) near-free:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE products_quick SHALLOW CLONE products;
# MAGIC
# MAGIC SELECT count(*) AS shallow_rows FROM products_quick;

# COMMAND ----------

rows = []
for t in ["products_test", "products_quick"]:
    d = spark.sql(f"DESCRIBE DETAIL {t}").first()
    rows.append((t, d["numFiles"], d["sizeInBytes"]))

display(spark.createDataFrame(
    rows, "clone string, num_files long, size_in_bytes long"))

# COMMAND ----------

# MAGIC %md
# MAGIC The shallow clone reports (near) zero owned storage — it reads the source's
# MAGIC files. Writes to a shallow clone create *new* files for the clone only
# MAGIC (copy-on-write); the source is never modified.
# MAGIC
# MAGIC **The fine print the exam checks:** a shallow clone depends on the source's
# MAGIC files. If the source runs `VACUUM` (or is dropped), the clone can lose the
# MAGIC files it points to and break. Shallow = fast experiments; deep = anything
# MAGIC that must outlive the source's maintenance.
# MAGIC
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS products_test;
# MAGIC DROP TABLE IF EXISTS products_quick;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC | | Deep clone | Shallow clone |
# MAGIC |---|---|---|
# MAGIC | Copies | metadata + data | metadata only |
# MAGIC | Cost/speed | storage ×2, slower | instant, ~free |
# MAGIC | Independent? | fully | depends on source files |
# MAGIC | Breaks if source VACUUMs? | no | possibly |
# MAGIC | Use for | staging, migration, backup-ish copies | quick tests, experiments |
# MAGIC
# MAGIC Both clone types are themselves Delta tables — with their own history from
# MAGIC the clone point forward.

# COMMAND ----------

dbutils.notebook.exit("cloning-complete")
