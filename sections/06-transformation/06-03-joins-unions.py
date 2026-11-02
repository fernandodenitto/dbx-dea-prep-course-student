# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.3 — Joins & unions: combining DataFrames
# MAGIC
# MAGIC The exam names them explicitly: inner, left, broadcast, multiple keys,
# MAGIC cross, union vs union all. One exploration notebook, real Lakeshore data,
# MAGIC every verb in both spellings, and the traps that come with each.

# COMMAND ----------

payments = spark.table("lakeshore.silver.payments")
refunds  = spark.table("lakeshore.bronze.refunds")
products = spark.table("lakeshore.silver.products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. INNER join — only the matches
# MAGIC
# MAGIC Which payments were refunded? Inner join keeps rows present on BOTH sides:

# COMMAND ----------

refunded = payments.join(refunds, on="payment_id", how="inner")
print(f"refunded payments: {refunded.count()}")   # = number of refunds

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. LEFT join — keep everything on the left, NULL where no match
# MAGIC
# MAGIC "All payments, with refund info *if any*" — the analyst's default:

# COMMAND ----------

from pyspark.sql.functions import col

pay_with_refunds = payments.join(
    refunds.select("payment_id",
                   col("amount").alias("refund_amount"),
                   "reason"),
    on="payment_id", how="left")

pay_with_refunds.groupBy(col("refund_amount").isNotNull().alias("was_refunded")) \
    .count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Note the **column-name collision** we defused: both tables have `amount`,
# MAGIC so we aliased the refund one *before* joining. Ambiguous-column errors are
# MAGIC the #1 join time-waster — alias early.
# MAGIC
# MAGIC ## 3. BROADCAST join — the small-table accelerator
# MAGIC
# MAGIC A join normally *shuffles* both sides across the cluster. If one side is
# MAGIC tiny (the 9-row region lookup below), broadcast it: every worker gets a full
# MAGIC copy, no shuffle for the big side.

# COMMAND ----------

from pyspark.sql.functions import broadcast

regions = spark.createDataFrame(
    [("CO", "Mountain"), ("WA", "Pacific"), ("OR", "Pacific"), ("UT", "Mountain"),
     ("MT", "Mountain"), ("NC", "South"),   ("AZ", "Southwest"), ("VT", "Northeast"),
     ("ID", "Mountain")],
    "state string, region string")

addresses = spark.table("lakeshore.bronze.addresses")
by_region = (addresses.join(broadcast(regions), on="state", how="left")
             .groupBy("region").count().orderBy("count", ascending=False))
by_region.show()

# COMMAND ----------

# The same join in SQL: the hint is a comment placed right after SELECT
regions.createOrReplaceTempView("regions")

hinted = """
SELECT /*+ BROADCAST(r) */ r.region, count(*) AS n
FROM lakeshore.bronze.addresses a
LEFT JOIN regions r ON a.state = r.state
GROUP BY r.region
"""
display(spark.sql(hinted))

plan = spark.sql(f"EXPLAIN {hinted}").first()[0]
assert "BroadcastHashJoin" in plan, plan
print("✓ the plan says BroadcastHashJoin")

# COMMAND ----------

# MAGIC %md
# MAGIC The 9-row lookup went to every worker whole; the 352 addresses never moved.
# MAGIC Serverless usually broadcasts small tables automatically (adaptive
# MAGIC execution); the explicit `broadcast()` hint matters on the exam and on
# MAGIC classic clusters. Threshold config: `spark.sql.autoBroadcastJoinThreshold`
# MAGIC — lesson 6.9.
# MAGIC
# MAGIC ## 4. Multiple keys, null-safe keys & CROSS join

# COMMAND ----------

# Multi-key: join on BOTH customer_id AND address_type (composite match)
addresses = spark.table("lakeshore.bronze.addresses")
shipping = addresses.filter(col("address_type") == "shipping")
multi_key = addresses.join(shipping,
                           on=["customer_id", "address_type"], how="inner")
print(f"multi-key matches (shipping rows): {multi_key.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The key that is NULL
# MAGIC
# MAGIC Compare a customer table with itself on `customer_id` AND `email`, the shape of
# MAGIC every "did this record change?" check. `=` never matches NULL to NULL, so the 10
# MAGIC customers without an email from lesson 6.1 fall out of the join. Same table, same
# MAGIC rows, and they look like changes. The null-safe operator keeps them.

# COMMAND ----------

customers = spark.table("lakeshore.silver.customers")
a, b = customers.alias("a"), customers.alias("b")
same_id = col("a.customer_id") == col("b.customer_id")

plain = a.join(b, same_id & (col("a.email") == col("b.email")))
safe  = a.join(b, same_id & col("a.email").eqNullSafe(col("b.email")))

print(f"with == : {plain.count()} matches · with eqNullSafe: {safe.count()} matches")
assert plain.count() == 190 and safe.count() == 200

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The SQL spelling is <=>
# MAGIC SELECT count(*) AS null_safe_matches
# MAGIC FROM lakeshore.silver.customers a
# MAGIC JOIN lakeshore.silver.customers b
# MAGIC   ON a.customer_id = b.customer_id AND a.email <=> b.email;

# COMMAND ----------

# CROSS join: every row × every row — 20 products vs 20 products = 400
pairs = products.crossJoin(products.select(col("product_id").alias("other_id")))
print(f"cross join size: {pairs.count()} (20 × 20 — imagine it on millions)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now a cross join that is on purpose: every product priced in every currency
# MAGIC -- from the snapshot we landed in lesson 5.8. It's the only join here with no key.
# MAGIC SELECT p.product_id, p.unit_price, fx.currency,
# MAGIC        round(p.unit_price * fx.rate, 2) AS price_in_currency
# MAGIC FROM lakeshore.silver.products p
# MAGIC CROSS JOIN lakeshore.bronze.fx_rates fx
# MAGIC ORDER BY p.product_id, fx.currency
# MAGIC LIMIT 6;

# COMMAND ----------

# MAGIC %md
# MAGIC Cross joins are almost always an *accident* (a join with a missing
# MAGIC condition). If an exam option produces a cartesian product on big tables —
# MAGIC that's your wrong answer.
# MAGIC
# MAGIC ## 5. UNION ALL vs UNION — the duplicate question

# COMMAND ----------

completed = payments.filter(col("status") == "completed")
pending   = payments.filter(col("status") == "pending")

stacked = completed.unionByName(pending)          # SQL: UNION ALL
print(f"completed + pending stacked: {stacked.count()}")

doubled = completed.unionByName(completed)
print(f"same set twice, unionAll: {doubled.count()}")
print(f"after distinct (SQL UNION): {doubled.distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark's `union`/`unionByName` = SQL **UNION ALL** (keeps duplicates);
# MAGIC SQL **UNION** deduplicates — that's `.distinct()` after. Prefer
# MAGIC `unionByName`: it matches columns by *name*, not position — positional
# MAGIC union with reordered columns is silent data corruption.
# MAGIC
# MAGIC The same proof in SQL, where the guide's two words live:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM (
# MAGIC      SELECT payment_id FROM lakeshore.silver.payments WHERE status = 'completed'
# MAGIC      UNION ALL
# MAGIC      SELECT payment_id FROM lakeshore.silver.payments WHERE status = 'completed'
# MAGIC   )) AS with_union_all,
# MAGIC   (SELECT count(*) FROM (
# MAGIC      SELECT payment_id FROM lakeshore.silver.payments WHERE status = 'completed'
# MAGIC      UNION
# MAGIC      SELECT payment_id FROM lakeshore.silver.payments WHERE status = 'completed'
# MAGIC   )) AS with_union;

# COMMAND ----------

n_all = spark.sql("""
    SELECT payment_id FROM lakeshore.silver.payments WHERE status = 'completed'
    UNION ALL
    SELECT payment_id FROM lakeshore.silver.payments WHERE status = 'completed'
""").count()
n_dedup = spark.sql("""
    SELECT payment_id FROM lakeshore.silver.payments WHERE status = 'completed'
    UNION
    SELECT payment_id FROM lakeshore.silver.payments WHERE status = 'completed'
""").count()
assert n_all == 2 * n_dedup, f"{n_all} vs {n_dedup}"
assert n_dedup == completed.count()

n_prices = spark.sql("""
    SELECT 1 FROM lakeshore.silver.products CROSS JOIN lakeshore.bronze.fx_rates
""").count()
assert n_prices == 20 * spark.table("lakeshore.bronze.fx_rates").count()
print(f"✓ UNION ALL {n_all} · UNION {n_dedup} · 20 products × currencies = {n_prices}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC inner = both sides · left = keep left, NULL-fill · broadcast = copy the
# MAGIC small side, skip the shuffle (`broadcast()` or `/*+ BROADCAST(t) */`) ·
# MAGIC multi-key = list of columns · `=` drops NULL keys, `<=>`/`eqNullSafe` keeps them ·
# MAGIC cross = usually a bug · union in Spark = UNION ALL; distinct() for SQL-UNION
# MAGIC semantics · alias colliding columns BEFORE joining.

# COMMAND ----------

dbutils.notebook.exit("joins-unions-complete")
