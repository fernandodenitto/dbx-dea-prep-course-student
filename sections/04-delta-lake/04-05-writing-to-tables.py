# Databricks notebook source
# MAGIC %md
# MAGIC # Writing to tables: append, overwrite, MERGE
# MAGIC
# MAGIC Four ways to get data *into* a Delta table, and — more exam-relevant — four
# MAGIC different answers to "what happens to the data already there?"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lakeshore;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. INSERT INTO — append
# MAGIC
# MAGIC Adds rows; touches nothing existing. Run it twice → rows twice: **append is
# MAGIC not idempotent**, which is why re-runnable pipelines avoid raw INSERT
# MAGIC (Section 5's COPY INTO exists precisely for this).
# MAGIC
# MAGIC ## 2. INSERT OVERWRITE — replace the *content*, keep the table
# MAGIC
# MAGIC A demo table for the full-refresh pattern:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE price_snapshot AS
# MAGIC SELECT product_id, unit_price, current_date() AS snapshot_date
# MAGIC FROM products;
# MAGIC
# MAGIC SELECT count(*) AS rows_v1 FROM price_snapshot;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE price_snapshot
# MAGIC SELECT product_id, unit_price, current_date() AS snapshot_date
# MAGIC FROM products
# MAGIC WHERE category != 'Water';
# MAGIC
# MAGIC SELECT count(*) AS rows_v2 FROM price_snapshot;

# COMMAND ----------

# MAGIC %md
# MAGIC Content replaced (20 → fewer rows), schema and grants intact — and because
# MAGIC it's Delta, the overwrite is **atomic and versioned**: `DESCRIBE HISTORY`
# MAGIC shows it, time travel can still read the old snapshot. Compare with
# MAGIC `CREATE OR REPLACE TABLE`: that can also change the *schema*; INSERT
# MAGIC OVERWRITE must match the existing one.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY price_snapshot;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. MERGE — the upsert
# MAGIC
# MAGIC The real-world case: a *changes feed* arrives — some rows update existing
# MAGIC records, some are brand new. One statement handles both. Our incoming feed:
# MAGIC two price changes and one new product.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW product_updates AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('P001', 'Ridgeline 2P Tent',       'Camping', 259.00),
# MAGIC   ('P019', 'Lakeshore Touring Kayak', 'Water',   849.00),
# MAGIC   ('P021', 'Lakeshore Dry Duffel 40L','Water',    59.00)
# MAGIC AS updates(product_id, name, category, unit_price);

# COMMAND ----------

v_before_merge = spark.sql("DESCRIBE HISTORY products") \
    .selectExpr("max(version) AS v").first()["v"]
print(f"products version before MERGE: {v_before_merge}")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO products AS target
# MAGIC USING product_updates AS source
# MAGIC   ON target.product_id = source.product_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET target.unit_price = source.unit_price
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (product_id, name, category, unit_price)
# MAGIC   VALUES (source.product_id, source.name, source.category, source.unit_price);

# COMMAND ----------

# MAGIC %md
# MAGIC The MERGE metrics say it all: rows **updated** (P001, P019) and rows
# MAGIC **inserted** (P021) — in one atomic transaction. Verify:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, name, unit_price FROM products
# MAGIC WHERE product_id IN ('P001', 'P019', 'P021') ORDER BY product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC MERGE is also **idempotent by design** when keys match: run the same MERGE
# MAGIC again and matched rows just get the same values — no duplicates. That's why
# MAGIC MERGE (not INSERT) is the backbone of production upserts, and why the exam
# MAGIC loves it.
# MAGIC
# MAGIC ## 4. Putting the catalog back (a 4.3 callback)
# MAGIC
# MAGIC Our canonical catalog has 20 products — the MERGE was a demo. You know the
# MAGIC tool for "undo my last operation":

# COMMAND ----------

spark.sql(f"RESTORE TABLE products TO VERSION AS OF {v_before_merge}")
display(spark.sql("SELECT count(*) AS n_products FROM products"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS price_snapshot;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap — "what happens to existing data?"
# MAGIC
# MAGIC | Statement | Existing data | Schema | Idempotent? |
# MAGIC |---|---|---|---|
# MAGIC | `INSERT INTO` | kept (append) | must match | ✗ |
# MAGIC | `INSERT OVERWRITE` | replaced | must match | ✓ (same input → same state) |
# MAGIC | `CREATE OR REPLACE TABLE` | replaced | can change | ✓ |
# MAGIC | `MERGE INTO` | updated/inserted per keys | must match | ✓ (keyed) |
# MAGIC
# MAGIC All four are versioned transactions — history survives every one of them.

# COMMAND ----------

dbutils.notebook.exit("writing-to-tables-complete")
