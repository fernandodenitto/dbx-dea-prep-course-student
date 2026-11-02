# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.7 — Gold layer objects: table vs view vs materialized view vs streaming table
# MAGIC
# MAGIC Four object types can serve the business layer, and the exam LOVES asking
# MAGIC which one fits a scenario. We build them on real Lakeshore data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A gold TABLE — computed, stored, refreshed by you
# MAGIC
# MAGIC The classic: CTAS into gold, refreshed by re-running (or INSERT OVERWRITE,
# MAGIC lesson 4.5 — both idempotent).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.gold.customer_order_summary AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.first_name,
# MAGIC   c.last_name,
# MAGIC   count(DISTINCT o.order_id)               AS n_orders,
# MAGIC   round(sum(i.line_total), 2)              AS total_spent,
# MAGIC   max(o.order_ts)                          AS last_order_ts
# MAGIC FROM lakeshore.silver.customers c
# MAGIC LEFT JOIN lakeshore.silver.orders       o ON c.customer_id = o.customer_id
# MAGIC LEFT JOIN lakeshore.silver.order_items  i ON o.order_id    = i.order_id
# MAGIC GROUP BY c.customer_id, c.first_name, c.last_name;
# MAGIC
# MAGIC SELECT * FROM lakeshore.gold.customer_order_summary
# MAGIC ORDER BY total_spent DESC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. A VIEW — a stored query, computed at read time
# MAGIC
# MAGIC No data stored: every query re-runs the definition against CURRENT silver.
# MAGIC Always fresh, costs compute per read.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW lakeshore.gold.vw_revenue_by_category AS
# MAGIC SELECT
# MAGIC   p.category,
# MAGIC   round(sum(i.line_total), 2) AS revenue,
# MAGIC   sum(i.quantity)             AS units_sold
# MAGIC FROM lakeshore.silver.order_items i
# MAGIC JOIN lakeshore.silver.products    p ON i.product_id = p.product_id
# MAGIC GROUP BY p.category;
# MAGIC
# MAGIC SELECT * FROM lakeshore.gold.vw_revenue_by_category ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Temporary flavors you must recognize: `CREATE TEMPORARY VIEW` (this session
# MAGIC only) and `CREATE GLOBAL TEMPORARY VIEW` (all sessions on the same
# MAGIC compute, via the `global_temp` schema — mostly legacy exam trivia now).
# MAGIC
# MAGIC ## 3. MATERIALIZED VIEW — a view that stores (and refreshes) its result
# MAGIC
# MAGIC Best of both: precomputed like a table, declaratively refreshed like a
# MAGIC pipeline (Databricks creates a serverless pipeline for it automatically — that's
# MAGIC also its cost profile).
# MAGIC
# MAGIC Honest status: the docs list creating one from a **notebook on serverless
# MAGIC compute** as **Beta** (runtime 18.1+), next to the established routes, a SQL
# MAGIC warehouse or a Lakeflow pipeline. Measured working on Free Edition on
# MAGIC 1 November 2026, in about 16 seconds. It may change.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW lakeshore.gold.mv_daily_revenue AS
# MAGIC SELECT date(order_ts)            AS order_date,
# MAGIC        round(sum(line_total), 2) AS revenue,
# MAGIC        count(DISTINCT order_id)  AS n_orders
# MAGIC FROM lakeshore.silver.order_items
# MAGIC GROUP BY date(order_ts);
# MAGIC
# MAGIC SELECT * FROM lakeshore.gold.mv_daily_revenue ORDER BY order_date DESC LIMIT 5;

# COMMAND ----------

mv = spark.table("lakeshore.gold.mv_daily_revenue")
days = mv.count()
revenue = float(mv.agg({"revenue": "sum"}).first()[0])
assert days == 139, f"expected 139 days, got {days}"
assert round(revenue) == 854427, f"expected revenue 854,427, got {revenue}"
print(f"✓ gold.mv_daily_revenue: {days} days, revenue {revenue:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How it refreshes
# MAGIC
# MAGIC | Option | Declared how | Fits when |
# MAGIC |---|---|---|
# MAGIC | Time schedule | `SCHEDULE EVERY 1 DAY` or `SCHEDULE CRON '…'` | a fixed rhythm; `EVERY` takes hours, days, weeks, so sub-hourly needs CRON |
# MAGIC | On change | `TRIGGER ON UPDATE` | refresh when upstream data changes |
# MAGIC | A job step | `REFRESH MATERIALIZED VIEW` in a task | the next step depends on the result |
# MAGIC
# MAGIC The fine print of the notebook route: a table can only be refreshed on the **compute
# MAGIC type that created it**. Ours came from serverless compute, so `REFRESH` works here and
# MAGIC fails from a SQL warehouse (reading from a warehouse is fine). `REFRESH … ASYNC` isn't
# MAGIC supported here either, and only the owner can refresh.

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH MATERIALIZED VIEW lakeshore.gold.mv_daily_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER MATERIALIZED VIEW lakeshore.gold.mv_daily_revenue
# MAGIC   ADD SCHEDULE EVERY 1 DAY;
# MAGIC
# MAGIC DESCRIBE EXTENDED lakeshore.gold.mv_daily_revenue;

# COMMAND ----------

def refresh_schedule():
    rows = spark.sql("DESCRIBE EXTENDED lakeshore.gold.mv_daily_revenue").collect()
    return {r[0]: r[1] for r in rows}["Refresh Schedule"]

print(f"with the schedule: {refresh_schedule()}")

# A demo shouldn't leave a job running on our quota
spark.sql("ALTER MATERIALIZED VIEW lakeshore.gold.mv_daily_revenue DROP SCHEDULE")

assert refresh_schedule() == "MANUAL", refresh_schedule()
print(f"after DROP SCHEDULE: {refresh_schedule()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. STREAMING TABLE — incremental ingestion as a SQL object
# MAGIC
# MAGIC Auto-Loader-style incremental processing, declared in SQL. The query must read
# MAGIC with the `STREAM` keyword, so the source must be append-only. Same three
# MAGIC birthplaces, same Beta on the notebook route. Section 7 builds the real ones inside
# MAGIC a pipeline; here, a throwaway to watch the mechanism.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lakeshore.gold.st_demo;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lakeshore.gold.st_demo_source AS
# MAGIC SELECT order_id, order_ts FROM lakeshore.silver.orders ORDER BY order_id LIMIT 10;
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE lakeshore.gold.st_demo
# MAGIC AS SELECT order_id, order_ts FROM STREAM lakeshore.gold.st_demo_source;

# COMMAND ----------

before = spark.table("lakeshore.gold.st_demo").count()

spark.sql("""INSERT INTO lakeshore.gold.st_demo_source
             VALUES ('ST-DEMO-1', current_timestamp())""")
spark.sql("REFRESH STREAMING TABLE lakeshore.gold.st_demo")

after = spark.table("lakeshore.gold.st_demo").count()
assert (before, after) == (10, 11), (before, after)
print(f"✓ {before} rows, one new row in the source, REFRESH: {after}. Only the new row ran.")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE lakeshore.gold.st_demo;
# MAGIC DROP TABLE lakeshore.gold.st_demo_source;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The decision table (memorize this one)
# MAGIC
# MAGIC | Object | Stores data? | Freshness | Compute cost | Fits when |
# MAGIC |---|---|---|---|---|
# MAGIC | Table (CTAS) | yes | as of last refresh YOU ran | at write | full control, any engine |
# MAGIC | View | no | always current | at every read | cheap to define, light queries |
# MAGIC | Materialized view | yes | managed refresh | at refresh (pipeline) | expensive query, many readers |
# MAGIC | Streaming table | yes | incremental appends | at refresh (pipeline) | append-only ingestion in SQL |

# COMMAND ----------

n = spark.table("lakeshore.gold.customer_order_summary").count()
assert n == 200, f"expected 200 customers in summary, got {n}"
print("✓ gold layer online: summary table + revenue view + daily-revenue MV")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Table = you refresh · view = computed per read, always current · MV =
# MAGIC precomputed + managed refresh (pipeline-backed) · streaming table =
# MAGIC incremental SQL ingestion (Section 7) · born on a SQL warehouse, in a pipeline, or
# MAGIC in a serverless notebook (Beta), never on classic compute · refresh with SCHEDULE,
# MAGIC TRIGGER ON UPDATE or a job, on the compute type that created it · the exam gives
# MAGIC you readers, freshness and cost — you pick the object.

# COMMAND ----------

dbutils.notebook.exit("gold-objects-complete")
