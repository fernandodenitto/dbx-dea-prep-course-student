# Databricks notebook source
# MAGIC %md
# MAGIC # 12.3 — Scan volume: small files, OPTIMIZE, liquid clustering
# MAGIC
# MAGIC Lesson 4.4 gave you OPTIMIZE and VACUUM. This notebook answers the next question: **how
# MAGIC many files does one query have to read?** We build a table badly on purpose, run the same
# MAGIC filter after each fix, and compare files read and files pruned.
# MAGIC
# MAGIC The canonical `order_items` is only 2,602 rows, too small to show any of this, so we
# MAGIC generate a throwaway table.

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import QueryFilter, TimeRange

TABLE = "lakeshore.silver.scan_lab"
w = WorkspaceClient()
started_ms = int(time.time() * 1000)

def scan_stats(tag):
    """Files read and pruned by the query carrying this tag, from the query history."""
    window = QueryFilter(query_start_time_range=TimeRange(start_time_ms=started_ms))
    for _ in range(18):                                   # the history lags by a few seconds
        for q in w.query_history.list(filter_by=window, include_metrics=True, max_results=200).res or []:
            if tag in (q.query_text or "") and q.metrics and q.metrics.read_files_count:
                return q.metrics.read_files_count, q.metrics.pruned_files_count or 0
        time.sleep(10)
    raise TimeoutError(f"{tag} did not reach the query history; use See performance instead")

def num_files():
    return spark.sql(f"DESCRIBE DETAIL {TABLE}").first()["numFiles"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Manufacture the disease: 400 small files
# MAGIC
# MAGIC 6 million rows in random order. We turn off the write-time compaction a managed table
# MAGIC would normally do for us, because we want to see the damage.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
spark.sql(f"""
  CREATE TABLE {TABLE} (item_id BIGINT, customer_id STRING, product_id INT, amount DOUBLE, note STRING)
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
""")

source = spark.range(6_000_000).selectExpr(
    "id AS item_id",
    "concat('C', lpad(cast(pmod(xxhash64(id), 200000) AS STRING), 6, '0')) AS customer_id",
    "cast(pmod(xxhash64(id, 1), 500) AS INT) AS product_id",
    "round(pmod(xxhash64(id, 2), 100000) / 100.0, 2) AS amount",
    "sha2(cast(id AS STRING), 256) AS note")

source.repartition(400).write.mode("append").saveAsTable(TABLE)
print("files on disk:", num_files())
assert num_files() == 400

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. One customer, every file
# MAGIC
# MAGIC Delta keeps min and max per column for each file: those are the **data skipping
# MAGIC statistics**. Here every file spans nearly the whole customer range, so none can be
# MAGIC skipped. Click **See performance** under this cell: the same numbers are in the query
# MAGIC details.

# COMMAND ----------

display(spark.sql("SELECT /* scan_small_files */ count(*), sum(amount) FROM lakeshore.silver.scan_lab WHERE customer_id = 'C100042'"))

# COMMAND ----------

results = {"400 small files": (num_files(), *scan_stats("scan_small_files"))}
print(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. OPTIMIZE: fewer files, nothing skipped yet

# COMMAND ----------

spark.sql(f"OPTIMIZE {TABLE}").select("metrics.numFilesRemoved", "metrics.numFilesAdded").show()
display(spark.sql("SELECT /* scan_compacted */ count(*), sum(amount) FROM lakeshore.silver.scan_lab WHERE customer_id = 'C100043'"))

# COMMAND ----------

results["after OPTIMIZE"] = (num_files(), *scan_stats("scan_compacted"))
print(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Liquid clustering: now the statistics can skip
# MAGIC
# MAGIC `ALTER TABLE … CLUSTER BY` changes metadata and rewrites nothing, so existing data needs
# MAGIC `OPTIMIZE FULL`. On a real table the platform picks large files. We force 16 MB, so that
# MAGIC this small table behaves like a big one.

# COMMAND ----------

spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES ('delta.targetFileSize' = '16mb')")
spark.sql(f"ALTER TABLE {TABLE} CLUSTER BY (customer_id)")
spark.sql(f"OPTIMIZE {TABLE} FULL").select("metrics.numFilesRemoved", "metrics.numFilesAdded").show()
display(spark.sql("SELECT /* scan_clustered */ count(*), sum(amount) FROM lakeshore.silver.scan_lab WHERE customer_id = 'C100044'"))

# COMMAND ----------

results["after CLUSTER BY + OPTIMIZE FULL"] = (num_files(), *scan_stats("scan_clustered"))
print(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The same table, and a predicate the statistics cannot use
# MAGIC
# MAGIC Wrap the column in a function and min and max are no use.

# COMMAND ----------

display(spark.sql("SELECT /* scan_wrapped */ count(*), sum(amount) FROM lakeshore.silver.scan_lab WHERE upper(customer_id) = 'C100045'"))

# COMMAND ----------

results["same table, column wrapped in upper()"] = (num_files(), *scan_stats("scan_wrapped"))

print(f"{'':<40}{'on disk':>8}{'read':>8}{'pruned':>8}")
for step, (on_disk, read, pruned) in results.items():
    print(f"{step:<40}{on_disk:>8}{read:>8}{pruned:>8}")

assert results["400 small files"][2] == 0
assert results["after CLUSTER BY + OPTIMIZE FULL"][1] <= 2
assert results["same table, column wrapped in upper()"][2] == 0

# COMMAND ----------

# MAGIC %md
# MAGIC Read that table top to bottom. Compaction reduced the files. Clustering is what let the
# MAGIC statistics skip them. And a careless predicate throws it all away.
# MAGIC
# MAGIC ## 6. Changing the keys rewrites nothing until you say so

# COMMAND ----------

spark.sql(f"ALTER TABLE {TABLE} CLUSTER BY (product_id)")
plain = spark.sql(f"OPTIMIZE {TABLE}").select("metrics.numFilesAdded").first()[0]
full = spark.sql(f"OPTIMIZE {TABLE} FULL").select("metrics.numFilesAdded").first()[0]
print(f"plain OPTIMIZE after the key change added {plain} files. OPTIMIZE FULL added {full}.")
assert plain == 0 and full > 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The rules, measured

# COMMAND ----------

def show_refusal(sql):
    """Run a statement that must fail, and print which error it raised."""
    try:
        spark.sql(sql)
    except Exception as e:
        print(f"{str(e).split(']')[0].strip('[ ' + chr(10))}\n    {sql}\n")
        return
    raise AssertionError(f"expected a refusal: {sql}")

show_refusal(f"ALTER TABLE {TABLE} CLUSTER BY (item_id, customer_id, product_id, amount, note)")
show_refusal(f"OPTIMIZE {TABLE} ZORDER BY (customer_id)")
show_refusal("CREATE OR REPLACE TABLE lakeshore.silver.scan_lab_bad (a INT, b INT) PARTITIONED BY (a) CLUSTER BY (b)")

wide_columns = ", ".join(f"c{i} INT" for i in range(35))
show_refusal(f"CREATE OR REPLACE TABLE lakeshore.silver.scan_lab_bad ({wide_columns}) CLUSTER BY (c33)")

# COMMAND ----------

# MAGIC %md
# MAGIC Four keys at most · no ZORDER on a clustered table · no partitioning with clustering ·
# MAGIC keys need statistics, and by default those stop at the first 32 columns.
# MAGIC
# MAGIC ## 8. Removing keys, and AUTO

# COMMAND ----------

spark.sql(f"ALTER TABLE {TABLE} CLUSTER BY NONE")
print("after NONE:", spark.sql(f"DESCRIBE DETAIL {TABLE}").first()["clusteringColumns"])

spark.sql(f"ALTER TABLE {TABLE} CLUSTER BY AUTO")
display(spark.sql(f"SHOW TBLPROPERTIES {TABLE}").filter("key = 'clusterByAuto'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Predictive optimization: is it on, and what has it done?
# MAGIC
# MAGIC `AUTO` depends on it. The setting inherits from account to catalog to schema to table,
# MAGIC and `DESCRIBE EXTENDED` shows where this table gets it from.

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {TABLE}").filter("col_name = 'Predictive Optimization'"))

# COMMAND ----------

# MAGIC %md
# MAGIC Beyond the exam, and worth one look: the history table shows what the platform ran on
# MAGIC its own. A brand-new workspace may have few rows, or none.

# COMMAND ----------

display(spark.sql("""
  SELECT operation_type, operation_status, count(*) AS operations
  FROM system.storage.predictive_optimization_operations_history
  GROUP BY operation_type, operation_status
  ORDER BY operations DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Clean up

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
spark.sql("DROP TABLE IF EXISTS lakeshore.silver.scan_lab_bad")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Files read and files pruned are the numbers to watch · statistics skip files only when the
# MAGIC data is laid out by the filtered column and the predicate leaves the column bare ·
# MAGIC `CLUSTER BY` then `OPTIMIZE FULL` for existing data · four keys, with statistics · `NONE`
# MAGIC removes keys · `AUTO` needs predictive optimization, which runs OPTIMIZE, VACUUM and ANALYZE
# MAGIC on managed tables.

# COMMAND ----------

dbutils.notebook.exit("optimize-clustering-complete")
