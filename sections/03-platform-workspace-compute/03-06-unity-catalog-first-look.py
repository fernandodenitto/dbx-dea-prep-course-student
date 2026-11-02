# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 3.6 — Namespace explorer: Unity Catalog first look
# MAGIC
# MAGIC Catalog Explorer showed us the object tree with clicks; now we drive the same
# MAGIC tree with SQL — the way jobs and pipelines see it.
# MAGIC
# MAGIC The Unity Catalog hierarchy: **metastore → catalog → schema → objects**
# MAGIC (tables, views, volumes, functions). You address everything with a
# MAGIC **three-level name**: `catalog.schema.object`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC The four that matter to us: `samples` (read-only demo data), `workspace` (the default
# MAGIC catalog every Free Edition ships with), `system` (platform tables), and ours:
# MAGIC `lakeshore`. Your list may be longer if you've used this workspace for anything else.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() AS catalog_now, current_schema() AS schema_now;

# COMMAND ----------

# MAGIC %md
# MAGIC Every session has a *current* catalog and schema — that's what unqualified
# MAGIC table names resolve against. `USE` changes them, so short names point where
# MAGIC you want:

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lakeshore;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() AS catalog_now, current_schema() AS schema_now;

# COMMAND ----------

# MAGIC %md
# MAGIC From here, a bare name like `orders` will mean `lakeshore.bronze.orders`.
# MAGIC Best practice for anything shared or scheduled: **write the full three-level
# MAGIC name anyway** — jobs shouldn't depend on session state.
# MAGIC
# MAGIC Now inspect what exists — schemas, volumes, and (spoiler) still no tables:

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN lakeshore;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN lakeshore.landing;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN lakeshore.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Empty — as designed. Files are waiting in the landing volume; ingestion into
# MAGIC bronze **tables** starts in the Delta Lake and Ingestion sections.
# MAGIC
# MAGIC SQL can list volume files too (the code twin of browsing in Catalog Explorer,
# MAGIC and of `dbutils.fs.ls` from the previous lesson):

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/lakeshore/landing/files/orders';

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, objects carry **metadata and comments** — governance starts with
# MAGIC being able to describe what you own:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED lakeshore.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Metastore → catalog → schema → objects · three-level names ·
# MAGIC `USE CATALOG` / `USE SCHEMA` set the session default (but jobs deserve full
# MAGIC names) · `SHOW` and `DESCRIBE` inspect anything · volumes hold our files,
# MAGIC bronze is still empty — next stop: Delta Lake tables.

# COMMAND ----------

dbutils.notebook.exit("namespace-explorer-complete")
