# Databricks notebook source
# MAGIC %md
# MAGIC # Creating tables properly: schema, constraints, CTAS
# MAGIC
# MAGIC First **real** Lakeshore table: the product catalog. Reference data, 20 rows,
# MAGIC small enough to type — which lets us focus on doing a CREATE properly:
# MAGIC explicit schema, comments, constraints.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lakeshore;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE products (
# MAGIC   product_id STRING NOT NULL COMMENT 'Catalog id, e.g. P001',
# MAGIC   name       STRING NOT NULL,
# MAGIC   category   STRING,
# MAGIC   unit_price DOUBLE
# MAGIC )
# MAGIC COMMENT 'Lakeshore product catalog — reference data (matches datagen.py)';

# COMMAND ----------

# MAGIC %md
# MAGIC Add a **CHECK constraint**: business rules the table itself enforces.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE products ADD CONSTRAINT positive_price CHECK (unit_price > 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO products VALUES
# MAGIC   ('P001', 'Ridgeline 2P Tent',         'Camping',  249.00),
# MAGIC   ('P002', 'Ridgeline 4P Tent',         'Camping',  399.00),
# MAGIC   ('P003', 'Ember Down Sleeping Bag',   'Camping',  179.00),
# MAGIC   ('P004', 'Basecamp Camping Stove',    'Camping',   89.00),
# MAGIC   ('P005', 'Torrent 40L Backpack',      'Hiking',   139.00),
# MAGIC   ('P006', 'Torrent 65L Backpack',      'Hiking',   189.00),
# MAGIC   ('P007', 'Switchback Trekking Poles', 'Hiking',    69.00),
# MAGIC   ('P008', 'Cascade Hiking Boots',      'Footwear', 159.00),
# MAGIC   ('P009', 'Riverbed Trail Runners',    'Footwear', 129.00),
# MAGIC   ('P010', 'Summit Insulated Jacket',   'Apparel',  199.00),
# MAGIC   ('P011', 'Drizzle Rain Shell',        'Apparel',  119.00),
# MAGIC   ('P012', 'Alpenglow Fleece',          'Apparel',   79.00),
# MAGIC   ('P013', 'Merino Base Layer',         'Apparel',   59.00),
# MAGIC   ('P014', 'Glacier Sunglasses',        'Gear',      89.00),
# MAGIC   ('P015', 'Lumen 400 Headlamp',        'Gear',      45.00),
# MAGIC   ('P016', 'Hydra 2L Water Bladder',    'Gear',      35.00),
# MAGIC   ('P017', 'Trailside First Aid Kit',   'Gear',      29.00),
# MAGIC   ('P018', 'Otter Dry Bag 20L',         'Water',     39.00),
# MAGIC   ('P019', 'Lakeshore Touring Kayak',   'Water',    899.00),
# MAGIC   ('P020', 'Driftline Paddle',          'Water',    129.00);
# MAGIC
# MAGIC SELECT count(*) AS n_products FROM products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enforcement in action
# MAGIC
# MAGIC Two guardrails now protect this table. Watch both reject bad data — we catch
# MAGIC the errors in Python so you can read them calmly:

# COMMAND ----------

for label, bad_sql in [
    ("NOT NULL violation", "INSERT INTO products VALUES ('P999', NULL, 'Gear', 10.0)"),
    ("CHECK violation",    "INSERT INTO products VALUES ('P998', 'Free Gift', 'Gear', -5.0)"),
]:
    try:
        spark.sql(bad_sql)
        print(f"✗ {label}: was NOT rejected (unexpected!)")
    except Exception as e:
        print(f"✓ {label} rejected → {str(e).splitlines()[0][:120]}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Still exactly 20 rows — nothing bad got in
# MAGIC SELECT count(*) AS n_products FROM products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## CTAS: create-table-as-select
# MAGIC
# MAGIC The other way tables are born: from a **query result**. CTAS infers the
# MAGIC schema and creates + populates the table in one atomic statement — you never
# MAGIC get a half-loaded table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE category_stats AS
# MAGIC SELECT category,
# MAGIC        count(*)              AS n_products,
# MAGIC        round(avg(unit_price), 2) AS avg_price
# MAGIC FROM products
# MAGIC GROUP BY category;
# MAGIC
# MAGIC SELECT * FROM category_stats ORDER BY avg_price DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Note what CTAS **can't** do: you don't declare constraints or column defaults
# MAGIC in the statement — schema comes from the query. Add constraints afterwards
# MAGIC with `ALTER TABLE` if you need them.
# MAGIC
# MAGIC `category_stats` was just a demo — we drop it; the real gold tables come in
# MAGIC Section 6 with proper naming.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS category_stats;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Explicit schema + comments for tables that matter · `NOT NULL` and `CHECK`
# MAGIC constraints enforce quality at the table · CTAS = atomic create-and-populate
# MAGIC with inferred schema (constraints added after) · `bronze.products` is now
# MAGIC canonical Lakeshore data — 20 rows, matching `datagen.py`.

# COMMAND ----------

dbutils.notebook.exit("creating-tables-complete")
