# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIMIZE & VACUUM essentials
# MAGIC
# MAGIC Delta's superpowers come from files + a log. Files accumulate; old versions
# MAGIC pile up. Two commands keep a table healthy — and both have exam-favorite
# MAGIC fine print. (Deep performance work continues in Section 12.)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lakeshore;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Manufacturing the small-file problem
# MAGIC
# MAGIC Our `delta_lab` table is about to receive 10 separate single-row inserts —
# MAGIC each one a commit, each one writing its own tiny file. This is exactly what
# MAGIC drip-feeding ingestion does to real tables.

# COMMAND ----------

for i in range(100, 110):
    spark.sql(f"INSERT INTO delta_lab VALUES ({i}, 'Sticker #{i}', 2.50)")

n_files = spark.sql("DESCRIBE DETAIL delta_lab").first()["numFiles"]
print(f"delta_lab now has {n_files} data files")

# COMMAND ----------

# MAGIC %md
# MAGIC Why is that bad? Every query pays a fixed cost *per file* — opening, reading
# MAGIC metadata, closing. Hundreds of tiny files can cost more than the data itself.
# MAGIC
# MAGIC ## 2. OPTIMIZE: compaction

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta_lab;

# COMMAND ----------

n_files_after = spark.sql("DESCRIBE DETAIL delta_lab").first()["numFiles"]
print(f"Data files after OPTIMIZE: {n_files_after}")

# COMMAND ----------

# MAGIC %md
# MAGIC The result column of `OPTIMIZE` reports files added and removed: many small
# MAGIC files were rewritten into few larger ones. OPTIMIZE rewrites files, so it's a
# MAGIC *new version* in the log; the old small files are now unreferenced, but still
# MAGIC on storage. Hold that thought for VACUUM.
# MAGIC
# MAGIC ## 3. Why layout matters: data skipping and ZORDER
# MAGIC
# MAGIC On every write, Delta stores **per-file statistics** in the log: minimum and
# MAGIC maximum values, null counts, record counts. A query with `WHERE id = 105` reads
# MAGIC those first and skips every file whose range can't contain 105. That's **data
# MAGIC skipping**, and it's automatic. Small, randomly mixed files defeat it: more files
# MAGIC to open, and ranges that all overlap your filter.
# MAGIC
# MAGIC `ZORDER BY` rewrites the files so similar values sit together, which tightens the
# MAGIC ranges:

# COMMAND ----------

result = spark.sql("OPTIMIZE delta_lab ZORDER BY (id)").first()["metrics"]
print("zOrderStats:", result["zOrderStats"])
assert result["zOrderStats"] is not None

# COMMAND ----------

# MAGIC %md
# MAGIC Our table is far too small to feel the difference; the mechanism is the point, and
# MAGIC the exam still asks for this syntax. Two facts for 2026:
# MAGIC
# MAGIC - For **new** tables the docs recommend **liquid clustering** over `ZORDER`, and
# MAGIC   the two can't be combined on a table. Recognize Z-order; use liquid clustering.
# MAGIC - **Predictive optimization** can run OPTIMIZE for you on managed tables.
# MAGIC
# MAGIC Section 12 picks this up and builds liquid clustering for real.
# MAGIC
# MAGIC ## 4. VACUUM: deleting what the log no longer needs

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM delta_lab DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC Almost nothing was deleted. Why? **The default retention period is 7 days**:
# MAGIC VACUUM only removes unreferenced files *older* than that. Our files are
# MAGIC minutes old, so they survive — and that's the safety design, because:
# MAGIC
# MAGIC > **Once VACUUM deletes a version's files, time travel to that version is
# MAGIC > gone.** Permanently. This is the single most-tested VACUUM fact.
# MAGIC
# MAGIC You *can* shorten retention — a deliberate, config-guarded choice we don't run
# MAGIC here (reference card 4.8 has the fine print). The exam wants you to know: 7 days default,
# MAGIC VACUUM trades history for storage cost, `DRY RUN` previews.
# MAGIC
# MAGIC ## 5. Lab cleanup
# MAGIC
# MAGIC `delta_lab` has served its purpose — it taught transactions, versions, and
# MAGIC small files. We drop it; from here on, only real Lakeshore tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Many small files = slow queries → `OPTIMIZE` compacts (new version, old files
# MAGIC unreferenced) · data skipping = per-file min/max stats, `ZORDER BY` tightens them,
# MAGIC liquid clustering is the modern answer for new tables · `VACUUM` deletes unreferenced files older than retention
# MAGIC (default **7 days**) · vacuumed versions are gone from time travel ·
# MAGIC `DRY RUN` previews · predictive optimization can run maintenance for you
# MAGIC (Section 12).

# COMMAND ----------

dbutils.notebook.exit("optimize-vacuum-complete")
