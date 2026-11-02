# Databricks notebook source
# MAGIC %md
# MAGIC # COPY INTO: idempotent incremental loading
# MAGIC
# MAGIC `INSERT` loads data; run it twice, get it twice. **COPY INTO** loads data
# MAGIC and *remembers which files it already loaded* — re-runs skip them. That
# MAGIC bookkeeping is the whole point, and today it fills our bronze layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lakeshore;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customers (JSON Lines)
# MAGIC
# MAGIC COPY INTO needs the target table to exist — it can be created **schemaless**
# MAGIC and let the load define columns (`mergeSchema` = true):

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers;
# MAGIC
# MAGIC COPY INTO customers
# MAGIC FROM '/Volumes/lakeshore/landing/files/customers/'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC The result reports rows and files loaded. Now the magic — **run it again**:

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO customers
# MAGIC FROM '/Volumes/lakeshore/landing/files/customers/'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC **Zero rows loaded.** The file was already ingested, COPY INTO knew, and
# MAGIC skipped it. *That* is idempotency — safe to schedule, safe to retry.
# MAGIC
# MAGIC ## 2. Same pattern, three more entities
# MAGIC
# MAGIC Payments and refunds are plain CSV; addresses is tab-separated — the format
# MAGIC options do the adapting:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS payments;
# MAGIC
# MAGIC COPY INTO payments
# MAGIC FROM '/Volumes/lakeshore/landing/files/payments/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS refunds;
# MAGIC
# MAGIC COPY INTO refunds
# MAGIC FROM '/Volumes/lakeshore/landing/files/refunds/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS addresses;
# MAGIC
# MAGIC COPY INTO addresses
# MAGIC FROM '/Volumes/lakeshore/landing/files/addresses/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'sep' = '\t')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify the bronze layer

# COMMAND ----------

# Lesson 6.1 later lands a dirty batch in the customers folder (206 more rows). If you
# come back here after that lesson, COPY INTO picks it up, so the expected count follows.
dirty_landed = any("dirty" in f.name
                   for f in dbutils.fs.ls("/Volumes/lakeshore/landing/files/customers/"))
expected = {"customers": 406 if dirty_landed else 200,
            "addresses": 352, "payments": 1000, "refunds": 67}
for table, exp in expected.items():
    n = spark.table(f"lakeshore.bronze.{table}").count()
    status = "✓" if n == exp else "✗"
    print(f"{status} bronze.{table:<10} {n:>5} rows (expected {exp})")
    assert n == exp, f"{table}: {n} != {exp}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 📖 The same command against cloud storage (theory, not on Free Edition)
# MAGIC
# MAGIC Free Edition has no bucket of yours to point at, so this block stays as text.
# MAGIC Only the `FROM` path and the access model change:
# MAGIC
# MAGIC ```sql
# MAGIC COPY INTO lakeshore.bronze.customers
# MAGIC FROM 's3://landing-bucket/customers/'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC ```
# MAGIC
# MAGIC On Azure the path is `abfss://container@account.dfs.core.windows.net/customers/`, on
# MAGIC Google `gs://landing-bucket/customers/`.
# MAGIC
# MAGIC | Access path | What you need | Status in the docs |
# MAGIC |---|---|---|
# MAGIC | Volume, `/Volumes/...` | `READ VOLUME` on the volume | **Recommended** |
# MAGIC | External location, `s3://...` | `READ FILES` on the external location | Supported, not the recommended approach |
# MAGIC | Credential clause, `FROM 's3://...' WITH (CREDENTIAL ...)` | A named storage credential, or inline temporary credentials | For paths outside an external location |
# MAGIC
# MAGIC In every case you also need `USE CATALOG`, `USE SCHEMA` and write access on the
# MAGIC target table.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC COPY INTO = incremental batch loading with per-file bookkeeping → idempotent
# MAGIC re-runs · target table may start schemaless (`mergeSchema`) · FORMAT_OPTIONS
# MAGIC adapt per format (header, sep, inferSchema) · bronze now holds customers,
# MAGIC addresses, payments, refunds (+products from S4). Orders are missing on
# MAGIC purpose — they arrive continuously, and that's a job for streaming. Next.

# COMMAND ----------

dbutils.notebook.exit("copy-into-complete")
