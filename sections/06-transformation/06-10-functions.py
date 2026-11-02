# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.10 — Functions you can grant
# MAGIC
# MAGIC **Beyond the guide:** no skill statement names UDFs. This lesson is here for the
# MAGIC job, and because the masking functions of lessons 11.4 and 11.5 have this shape.
# MAGIC
# MAGIC Every transformation so far has been written *inside* a query. This notebook
# MAGIC takes a piece of logic out of the query and gives it a **name in Unity
# MAGIC Catalog** — which makes it a securable object like a table: owned, grantable,
# MAGIC and reusable by anyone you allow.
# MAGIC
# MAGIC Two flavours, and the difference matters in the bill.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A SQL scalar UDF
# MAGIC
# MAGIC The shape is `CREATE FUNCTION name(args) RETURNS type RETURN <expression>`.
# MAGIC One expression. No procedural code, no loops — and that restriction is exactly
# MAGIC what lets the engine **inline it into the query plan**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.order_size_band(n_items INT)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Buckets an order by line count: single / small / bulk'
# MAGIC RETURN CASE
# MAGIC          WHEN n_items IS NULL THEN 'unknown'
# MAGIC          WHEN n_items = 1     THEN 'single'
# MAGIC          WHEN n_items <= 4    THEN 'small'
# MAGIC          ELSE 'bulk'
# MAGIC        END;

# COMMAND ----------

# MAGIC %md
# MAGIC Now use it exactly like a built-in. Note the three-level name — it is a catalog
# MAGIC object, not something that lives in this notebook's session.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lakeshore.silver.order_size_band(n_items) AS band,
# MAGIC        count(*)                                  AS orders
# MAGIC FROM lakeshore.silver.orders
# MAGIC GROUP BY ALL
# MAGIC ORDER BY orders DESC;

# COMMAND ----------

bands = {r["band"] for r in spark.sql("""
    SELECT DISTINCT lakeshore.silver.order_size_band(n_items) AS band
    FROM lakeshore.silver.orders
""").collect()}
assert bands <= {"single", "small", "bulk", "unknown"}, f"unexpected bands: {bands}"
print(f"OK — the function returned only declared bands: {sorted(bands)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. It is a catalog object, not a snippet
# MAGIC
# MAGIC This is the part that separates a Unity Catalog function from the
# MAGIC session-scoped UDFs you may have met elsewhere. It shows up in the catalog, it
# MAGIC has an owner, and access to it is granted.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED lakeshore.silver.order_size_band;

# COMMAND ----------

# MAGIC %md
# MAGIC The privilege to call one is **`EXECUTE`**, and it is held on the *function*.
# MAGIC Creating one needs **`CREATE FUNCTION`** on the schema or catalog. A preview, this:
# MAGIC `GRANT` and the privilege model get their proper treatment in lesson 11.3.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT EXECUTE ON FUNCTION lakeshore.silver.order_size_band TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON FUNCTION lakeshore.silver.order_size_band;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A Python UDF, for what SQL cannot say
# MAGIC
# MAGIC Reach for Python when the logic genuinely is not an expression. Here: pick the
# MAGIC email domain apart and classify it against a set — readable in three lines of
# MAGIC Python, awkward as nested `CASE` and `split_part`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.email_kind(email STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Classifies an email address as consumer, corporate or missing'
# MAGIC AS $$
# MAGIC     CONSUMER = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com"}
# MAGIC     if not email or "@" not in email:
# MAGIC         return "missing"
# MAGIC     domain = email.rsplit("@", 1)[1].strip().lower()
# MAGIC     return "consumer" if domain in CONSUMER else "corporate"
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lakeshore.silver.email_kind(email) AS kind,
# MAGIC        count(*)                           AS customers
# MAGIC FROM lakeshore.silver.customers
# MAGIC GROUP BY ALL
# MAGIC ORDER BY customers DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Our generated emails are all `example.com`, so they classify as `consumer` —
# MAGIC and the 10 NULL emails we documented back in lesson 6.1 come back as
# MAGIC `missing`. The function handles the null case explicitly, which is the habit
# MAGIC lesson 6.2 asked for: close the ladder, never let a NULL fall through.

# COMMAND ----------

kinds = {r["kind"]: r["customers"] for r in spark.sql("""
    SELECT lakeshore.silver.email_kind(email) AS kind, count(*) AS customers
    FROM lakeshore.silver.customers GROUP BY ALL
""").collect()}
assert kinds == {"consumer": 190, "missing": 10}, f"expected 190 and 10: {kinds}"
print(f"OK — {kinds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The honest trade
# MAGIC
# MAGIC A SQL UDF is an expression the optimizer can see through and inline. A Python
# MAGIC UDF is a black box: rows leave the engine, get handed to a Python process, and
# MAGIC come back. That boundary costs, and the optimizer cannot push filters through
# MAGIC it.
# MAGIC
# MAGIC So the rule is simple: **if it can be a SQL UDF, make it one.** Reach for Python when the logic genuinely is not expressible as an
# MAGIC expression — and then keep it small.
# MAGIC
# MAGIC Let us look at the two plans rather than take that on trust.

# COMMAND ----------

print("=== SQL UDF — look for the CASE expression inlined into the plan ===")
spark.sql("""
    SELECT lakeshore.silver.order_size_band(n_items) FROM lakeshore.silver.orders
""").explain()

# COMMAND ----------

print("=== Python UDF — look for a separate UDF / Python evaluation node ===")
spark.sql("""
    SELECT lakeshore.silver.email_kind(email) FROM lakeshore.silver.customers
""").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Where this goes next
# MAGIC
# MAGIC Keep `order_size_band` and `email_kind` in mind for two later lessons: in
# MAGIC **11.4** a function of exactly this shape becomes a *column mask*, and in
# MAGIC **11.5** a policy selects one by attribute. A masking function is not a special
# MAGIC kind of object — it is this object, used for that.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Clean up
# MAGIC
# MAGIC These two are teaching objects, not part of the Lakeshore model, so they go.
# MAGIC If you want to keep them, drop the next cell instead — but then register them
# MAGIC in your own notes, because an unexplained function in a catalog is exactly the
# MAGIC kind of thing lesson 6.8 was about.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS lakeshore.silver.order_size_band;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS lakeshore.silver.email_kind;

# COMMAND ----------

# A quirk worth knowing: unlike almost everything else, SHOW FUNCTIONS will not take
# a three-level name. `SHOW FUNCTIONS IN lakeshore.silver` fails with
# CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED — you set the catalog first, then name
# the schema on its own.
spark.sql("USE CATALOG lakeshore")
left = [r[0] for r in spark.sql("SHOW FUNCTIONS IN silver").collect()
        if r[0].endswith(("order_size_band", "email_kind"))]
assert not left, f"a demo function survived cleanup: {left}"
print("OK — both demo functions removed; lakeshore.silver is as we found it")
