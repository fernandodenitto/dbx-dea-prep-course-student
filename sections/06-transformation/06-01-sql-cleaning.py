# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.1 — Bronze → silver in SQL: nulls, duplicates, casts
# MAGIC
# MAGIC Section 5 left all six entities raw in bronze. This section interprets them.
# MAGIC
# MAGIC Plot twist: the customer system just re-sent a **full export with quality
# MAGIC problems** — missing emails, duplicated records. Perfect: cleaning data
# MAGIC nobody broke on purpose teaches nothing; we know exactly what's wrong with
# MAGIC ours, so we can verify the cleanup completely.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")           # make datagen.py and feed.py importable

# COMMAND ----------

# The "bad export" arrives: same 200 customers, regenerated with dirty=True
# (10 null emails, 6 duplicated rows with a later created_at)
import datagen

dirty = datagen.generate_customers(200, seed=2026, dirty=True)
datagen.write_json(
    dirty, "/Volumes/lakeshore/landing/files/customers/customers_batch_002_dirty.json")
print(f"landed dirty batch: {len(dirty)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC Ingesting it is a one-liner — the **same COPY INTO from lesson 5.3** picks
# MAGIC up only the new file. Idempotency paying rent:

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO lakeshore.bronze.customers
# MAGIC FROM '/Volumes/lakeshore/landing/files/customers/'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Audit before you clean
# MAGIC
# MAGIC Never clean blind. Three numbers first — total, null emails, surplus rows.
# MAGIC Expect **406 / 10 / 206**. The 206 surplus is two different things: 200 rows
# MAGIC because the whole file was re-sent, plus the 6 records that really do carry a
# MAGIC later `created_at`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)                                   AS total_rows,
# MAGIC   count_if(email IS NULL)                    AS null_emails,
# MAGIC   count(*) - count(DISTINCT customer_id)     AS surplus_rows
# MAGIC FROM lakeshore.bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Who exactly is duplicated?
# MAGIC SELECT customer_id, count(*) AS copies
# MAGIC FROM lakeshore.bronze.customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING count(*) > 1
# MAGIC ORDER BY copies DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The silver rules
# MAGIC
# MAGIC Our contract for `silver.customers`, decided *before* writing SQL:
# MAGIC 1. **One row per customer** — on conflict, the most recent `created_at` wins.
# MAGIC 2. **A tie needs a rule too.** The re-export repeats the same `created_at`, so
# MAGIC    190 customers tie on identical rows and 10 tie between a real email and a
# MAGIC    NULL. Our rule: the NULL wins. The export is the source's latest word, and
# MAGIC    if the CRM blanked an address, silver must not bring it back.
# MAGIC 3. **Typed columns** — `created_at` becomes a real TIMESTAMP, not a string.
# MAGIC 4. **Null emails survive** (a customer without an email is still a customer)
# MAGIC    — but they're counted and visible. Dropping rows silently is how data
# MAGIC    teams lose trust.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.customers AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   email,
# MAGIC   CAST(created_at AS TIMESTAMP) AS created_at
# MAGIC FROM lakeshore.bronze.customers
# MAGIC QUALIFY ROW_NUMBER() OVER (
# MAGIC   PARTITION BY customer_id
# MAGIC   ORDER BY created_at DESC, email IS NULL DESC
# MAGIC ) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC The window function does the deduplication: rows are ranked per customer by
# MAGIC recency, and `QUALIFY … = 1` keeps only each customer's newest version.
# MAGIC
# MAGIC The second sort key is the part people skip. With `ORDER BY created_at DESC`
# MAGIC alone, those 10 ties resolve however the engine happens to read them: the docs
# MAGIC say that if the order is not unique, the result is non-deterministic. Your
# MAGIC silver could hold anywhere from 0 to 10 NULL emails, and a re-run could differ.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)                    AS customers,
# MAGIC   count_if(email IS NULL)     AS still_missing_email,
# MAGIC   typeof(any_value(created_at)) AS created_at_type
# MAGIC FROM lakeshore.silver.customers;

# COMMAND ----------

audit = spark.sql("""
    SELECT count(*) AS total, count_if(email IS NULL) AS nulls,
           count(*) - count(DISTINCT customer_id) AS surplus
    FROM lakeshore.bronze.customers
""").first()
assert (audit.total, audit.nulls, audit.surplus) == (406, 10, 206), audit

customers = spark.table("lakeshore.silver.customers")
n = customers.count()
missing = customers.filter("email IS NULL").count()
assert n == 200, f"expected 200 customers, got {n}"
assert missing == 10, f"expected exactly 10 NULL emails, got {missing}"

# The other defensible policy, computed but not kept: prefer the real email on a tie
other_policy = spark.sql("""
    SELECT count_if(email IS NULL) AS nulls
    FROM (SELECT email FROM lakeshore.bronze.customers
          QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id
                                     ORDER BY created_at DESC, email IS NULL ASC) = 1)
""").first().nulls
assert other_policy == 0, f"expected 0 NULL emails under the other policy, got {other_policy}"
print(f"✓ silver.customers: {n} customers, {missing} NULL emails (5%), typed")
print(f"  flip the tie-breaker and the same data gives {other_policy}: the policy is a decision")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cleaning nulls: the toolbox
# MAGIC
# MAGIC Silver keeps its NULLs, because that is the contract we chose. Plenty of other
# MAGIC jobs need them gone: a report that must show `unknown`, a mailing list that
# MAGIC needs an address. So here is every tool, run for real over `silver.customers`,
# MAGIC **without writing anything**. Row counts in silver do not change.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- count(col) skips NULLs. And `= NULL` is never true: use IS NULL.
# MAGIC SELECT
# MAGIC   count(*)                 AS all_rows,
# MAGIC   count(email)             AS with_email,
# MAGIC   count_if(email IS NULL)  AS without_email,
# MAGIC   count_if(email = NULL)   AS the_trap
# MAGIC FROM lakeshore.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replace at read time. coalesce takes N arguments; ifnull and nvl take exactly 2.
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   email,
# MAGIC   coalesce(email, 'unknown')  AS with_coalesce,
# MAGIC   ifnull(email, 'unknown')    AS with_ifnull,
# MAGIC   nvl(email, 'unknown')       AS with_nvl
# MAGIC FROM lakeshore.silver.customers
# MAGIC WHERE email IS NULL
# MAGIC ORDER BY customer_id
# MAGIC LIMIT 3;

# COMMAND ----------

from pyspark.sql.functions import coalesce, col, lit

customers = spark.table("lakeshore.silver.customers")

# Replace: three spellings, one result
filled    = customers.fillna({"email": "unknown"})
filled_na = customers.na.fill("unknown", subset=["email"])
defaulted = customers.withColumn("email", coalesce(col("email"), lit("unknown")))

# Remove: two spellings of dropna, plus the explicit filter
dropped    = customers.dropna(subset=["email"])
dropped_na = customers.na.drop(subset=["email"])
filtered   = customers.filter(col("email").isNotNull())

# how="all": drop a row only if EVERY listed column is NULL. first_name is never NULL
kept_all = customers.dropna(how="all", subset=["email", "first_name"])
# And the mirror image, to look at the problem rows
only_missing = customers.filter(col("email").isNull())

for df in (filled, filled_na, defaulted):
    assert df.count() == 200 and df.filter(col("email") == "unknown").count() == 10
for df in (dropped, dropped_na, filtered):
    assert df.count() == 190
assert kept_all.count() == 200
assert only_missing.count() == 10
assert customers.fillna(0).filter(col("email").isNull()).count() == 10   # wrong type: ignored
print("replace → 200 rows, 10 'unknown' | remove → 190 rows | how='all' → 200 rows")

# COMMAND ----------

# MAGIC %md
# MAGIC | Tool | What it does | Reach for it when |
# MAGIC |---|---|---|
# MAGIC | `coalesce(a, b, c)` | first non-NULL of N values | a chain of fallbacks, in SQL or PySpark |
# MAGIC | `ifnull(a, b)` / `nvl(a, b)` | two-argument coalesce | one simple default in SQL |
# MAGIC | `fillna` / `na.fill` | replaces NULLs across columns | bulk defaults on a DataFrame, per column with a dict |
# MAGIC | `dropna` / `na.drop` | removes rows, `how="any"` by default | the row is useless without that value; always pass `subset` |
# MAGIC | `IS NULL` / `isNotNull()` | explicit filter | you want the rule readable in the query |
# MAGIC
# MAGIC Two traps. `dropna()` with no arguments drops a row if **any** column is NULL,
# MAGIC which is how one sparse column quietly eats your table. And `fillna` only touches
# MAGIC columns of a matching type: `fillna(0)` leaves a NULL string alone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The opposite case: `products`
# MAGIC
# MAGIC `bronze.products` is reference data — 20 rows, clean, nobody re-exports it
# MAGIC badly. Tempting conclusion: nothing to clean, so let gold read bronze
# MAGIC directly. **Don't.** The layer boundary is a contract, not a filter: gold
# MAGIC reads silver, always. The day a broken price does arrive, you fix it in one
# MAGIC place and everything downstream inherits the fix — instead of hunting for
# MAGIC the queries that reached around the layer.
# MAGIC
# MAGIC A silver table that is *almost* a copy still earns its keep. Ours does one
# MAGIC real job: it types the price properly.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.products AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   name,
# MAGIC   category,
# MAGIC   CAST(unit_price AS DECIMAL(10,2)) AS unit_price
# MAGIC FROM lakeshore.bronze.products;

# COMMAND ----------

p = spark.table("lakeshore.silver.products")
assert p.count() == 20, f"expected 20 products, got {p.count()}"
assert dict(p.dtypes)["unit_price"] == "decimal(10,2)", dict(p.dtypes)["unit_price"]
print("✓ silver.products: 20 rows, price typed — gold now has a silver source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Audit first (count, count_if, GROUP BY … HAVING) · dedupe = window +
# MAGIC `QUALIFY ROW_NUMBER() = 1` with an explicit tie-breaker · cast types at
# MAGIC silver · decide a policy for bad values — and make it visible, never silent
# MAGIC · replace NULLs with coalesce/ifnull/nvl/fillna, remove them with
# MAGIC dropna/`IS NOT NULL` · a clean source still gets a silver table, because
# MAGIC layers are a contract.

# COMMAND ----------

dbutils.notebook.exit("sql-cleaning-complete")
