# Databricks notebook source
# MAGIC %md
# MAGIC # 7.4 — Two roads, one truth
# MAGIC
# MAGIC Section 6 built daily revenue by hand. The pipeline built it by declaration. They
# MAGIC read different silver tables, so if both are right, they have to agree to the cent.

# COMMAND ----------

if not spark.catalog.tableExists("lakeshore.gold.sdp_orders_by_day"):
    dbutils.notebook.exit("Run the full pipeline from lesson 7.4 first")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'pipeline (gold.sdp_orders_by_day)' AS road,
# MAGIC        count(*) AS days, sum(n_orders) AS orders, round(sum(revenue), 2) AS revenue
# MAGIC FROM lakeshore.gold.sdp_orders_by_day
# MAGIC UNION ALL
# MAGIC SELECT 'by hand (gold.mv_daily_revenue)',
# MAGIC        count(*), sum(n_orders), round(sum(revenue), 2)
# MAGIC FROM lakeshore.gold.mv_daily_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC Totals can agree by luck. So compare day by day: this returns every day where the
# MAGIC two roads differ, and the only good answer is no rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT coalesce(p.order_date, h.order_date) AS order_date,
# MAGIC        p.revenue AS pipeline_revenue, h.revenue AS by_hand_revenue
# MAGIC FROM lakeshore.gold.sdp_orders_by_day p
# MAGIC FULL JOIN lakeshore.gold.mv_daily_revenue h ON p.order_date = h.order_date
# MAGIC WHERE p.revenue IS DISTINCT FROM h.revenue;

# COMMAND ----------

pipeline = spark.sql("""
    SELECT count(*) AS days, sum(n_orders) AS orders, round(sum(revenue), 2) AS revenue
    FROM lakeshore.gold.sdp_orders_by_day""").first()
by_hand = spark.sql("""
    SELECT count(*) AS days, sum(n_orders) AS orders, round(sum(revenue), 2) AS revenue
    FROM lakeshore.gold.mv_daily_revenue""").first()
day_diffs = spark.sql("""
    SELECT count(*) FROM lakeshore.gold.sdp_orders_by_day p
    FULL JOIN lakeshore.gold.mv_daily_revenue h ON p.order_date = h.order_date
    WHERE p.revenue IS DISTINCT FROM h.revenue""").first()[0]

assert pipeline["days"] == 139
assert pipeline["orders"] == 1080
assert pipeline["revenue"] == 854427
assert (pipeline["days"], pipeline["orders"], pipeline["revenue"]) == \
       (by_hand["days"], by_hand["orders"], by_hand["revenue"])
assert day_diffs == 0, f"{day_diffs} days differ between the two roads"

print(f"Both roads: {pipeline['days']} days, {pipeline['orders']} orders, "
      f"revenue {pipeline['revenue']}")

# COMMAND ----------

dbutils.notebook.exit("two-roads-agree")
