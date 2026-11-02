# Databricks notebook source
# MAGIC %md
# MAGIC # The landing zone: querying files before ingesting them
# MAGIC
# MAGIC Before loading anything, a data engineer *looks at the files*. SQL can query
# MAGIC files directly — schema-on-read — which is perfect for inspection, and a
# MAGIC terrible long-term substitute for tables. We do both, knowingly.

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/lakeshore/landing/files/customers';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Query JSON directly: `json.` + backticked path

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`/Volumes/lakeshore/landing/files/customers/`
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Spark read the folder, inferred the schema, returned rows — zero setup.
# MAGIC The same trick works with `parquet.`, `csv.`, `text.`, `binaryFile.`. This is
# MAGIC the older form of the idea and it is still valid, so recognize it on the exam:
# MAGIC ``SELECT * FROM json.`/path/` `` is a query, not a table.
# MAGIC
# MAGIC `text.` is the escape hatch: read raw lines when a file is too broken to
# MAGIC parse (it returns for messy JSON in lesson 6.4):

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM text.`/Volumes/lakeshore/landing/files/orders/`
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The limit of direct paths — and `read_files`
# MAGIC
# MAGIC `csv.`/`json.` direct queries accept **no options**. Our addresses file is
# MAGIC tab-separated with a header — direct `csv.` would mangle it. The modern
# MAGIC answer is the **`read_files`** table-valued function, which takes options:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/lakeshore/landing/files/addresses/',
# MAGIC   format => 'csv',
# MAGIC   sep    => '\t',
# MAGIC   header => true
# MAGIC )
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Note the extra `_rescued_data` column `read_files` adds — anything that
# MAGIC doesn't fit the inferred schema lands there instead of being lost. Remember
# MAGIC it: Auto Loader uses the same mechanism in lesson 5.6.
# MAGIC
# MAGIC ## 3. Which file did this row come from? `_metadata`
# MAGIC
# MAGIC Every file-based read also carries a hidden `_metadata` struct: `file_path`,
# MAGIC `file_name`, `file_size`, `file_modification_time`, `file_block_start`,
# MAGIC `file_block_length`. `SELECT *` does **not** return it, so you name it:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_name, _metadata.file_size, _metadata.file_modification_time,
# MAGIC        customer_id, email
# MAGIC FROM read_files('/Volumes/lakeshore/landing/files/customers/', format => 'json')
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC The older path syntax carries it too:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_path, customer_id
# MAGIC FROM json.`/Volumes/lakeshore/landing/files/customers/`
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC This is how bronze records provenance: lesson 5.5 stores
# MAGIC `_metadata.file_path` as a `source_file` column.
# MAGIC
# MAGIC ## 4. Why we don't stop here
# MAGIC
# MAGIC Direct file queries re-read and re-infer **every time** — no Delta
# MAGIC transactions, no time travel, no OPTIMIZE, no permissions on rows. Fine for
# MAGIC a look, wrong for a pipeline. Loading files into **bronze tables** is the
# MAGIC next lesson's job.
# MAGIC
# MAGIC ## 5. Meet `feed.py` — our live data feed
# MAGIC
# MAGIC One more tool to know *of* today (we run it in lesson 5.5): `feed.py`
# MAGIC generates **new order files on demand**, deterministic and
# MAGIC integrity-preserving, so our streaming lessons ingest genuinely fresh data.
# MAGIC It sits next to `datagen.py` in `datagen/`, in the Git folder you
# MAGIC cloned in lesson 1.5. The cell below finds that folder and imports it, the
# MAGIC same way lesson 2.6 imported `datagen`.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")           # make datagen.py and feed.py importable

import feed

help(feed.emit_batch)
help(feed.run_feed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC `json.`/`text.`/`parquet.` + backticked path = instant schema-on-read (older
# MAGIC form, still valid) · options need **`read_files`** (note `_rescued_data`) ·
# MAGIC `_metadata.file_path` must be selected by name · direct queries are
# MAGIC for inspection, tables are for pipelines · `feed.py` will feed our streams.

# COMMAND ----------

dbutils.notebook.exit("landing-zone-complete")
