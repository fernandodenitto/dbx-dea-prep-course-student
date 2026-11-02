# Databricks notebook source
# MAGIC %md
# MAGIC # History, time travel, RESTORE
# MAGIC
# MAGIC Every operation on a Delta table becomes a **version** in the transaction
# MAGIC log. Today we use that: query the past, and undo a change for real.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lakeshore;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY products;

# COMMAND ----------

# MAGIC %md
# MAGIC Remember the current latest version — we'll need it after we "accidentally"
# MAGIC break something. We grab it in Python so the notebook works no matter how
# MAGIC many times you've re-run things:

# COMMAND ----------

v_before = spark.sql("DESCRIBE HISTORY products") \
    .selectExpr("max(version) AS v").first()["v"]
print(f"Latest products version BEFORE our change: {v_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The "oops" moment
# MAGIC
# MAGIC Marketing asks for a 10% price increase on Camping gear. We run it — and
# MAGIC then discover it was meant only for the 4-person tent. Classic.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE products
# MAGIC SET unit_price = round(unit_price * 1.10, 2)
# MAGIC WHERE category = 'Camping';
# MAGIC
# MAGIC SELECT product_id, name, unit_price FROM products
# MAGIC WHERE category = 'Camping' ORDER BY product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying the past
# MAGIC
# MAGIC Two syntaxes, both exam material — by **version** and by **timestamp**:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM products VERSION AS OF 3;
# MAGIC SELECT * FROM products@v3;                          -- shorthand
# MAGIC SELECT * FROM products TIMESTAMP AS OF '2026-08-17';
# MAGIC ```
# MAGIC
# MAGIC We use the version we saved, and compare past vs present in one query:

# COMMAND ----------

display(spark.sql(f"""
  SELECT now.product_id, now.name,
         past.unit_price AS price_before,
         now.unit_price  AS price_now
  FROM products AS now
  JOIN (SELECT * FROM products VERSION AS OF {v_before}) AS past
    ON now.product_id = past.product_id
  WHERE now.unit_price != past.unit_price
  ORDER BY now.product_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Time travel reads **do not change** the table — they read old file versions
# MAGIC that the log still references. To actually undo, we **RESTORE**:

# COMMAND ----------

spark.sql(f"RESTORE TABLE products TO VERSION AS OF {v_before}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Camping prices are back
# MAGIC SELECT product_id, name, unit_price FROM products
# MAGIC WHERE category = 'Camping' ORDER BY product_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY products;

# COMMAND ----------

# MAGIC %md
# MAGIC Look at the history: **RESTORE didn't erase anything** — it's a *new version*
# MAGIC that points back to the old data. The mistaken update is still in the log,
# MAGIC auditable forever (well — until VACUUM, next lesson).
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC `DESCRIBE HISTORY` = the audit trail · `VERSION AS OF` / `@v` /
# MAGIC `TIMESTAMP AS OF` = read the past without touching the present ·
# MAGIC `RESTORE TABLE … TO VERSION AS OF` = undo as a forward-written new version ·
# MAGIC time travel depends on old files existing — which is exactly VACUUM's topic.

# COMMAND ----------

dbutils.notebook.exit("time-travel-complete")
