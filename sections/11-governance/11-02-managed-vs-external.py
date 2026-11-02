# Databricks notebook source
# MAGIC %md
# MAGIC # 11.2 — Table operations: create, modify, delete, and the conversion rules
# MAGIC
# MAGIC Everything a **managed** table can do, run for real on a throwaway table. Then the
# MAGIC two things Free Edition refuses, run on purpose so you can read the errors:
# MAGIC external tables and `SET MANAGED`. Those errors *are* the rule.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create, then ask the table what it is

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.table_ops_demo AS
# MAGIC SELECT product_id, name, unit_price FROM lakeshore.silver.products;

# COMMAND ----------

def table_fact(table, field):
    """One row of DESCRIBE EXTENDED, for example the table's Type or Owner."""
    rows = spark.sql(f"DESCRIBE EXTENDED {table}").collect()
    return next(r["data_type"] for r in rows if r["col_name"] == field)

table_type = table_fact("lakeshore.silver.table_ops_demo", "Type")
print("Type:", table_type)
assert table_type == "MANAGED"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Modify: the same ALTER on either table type
# MAGIC
# MAGIC Schema, comment, properties, name. None of these care who owns the files.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakeshore.silver.table_ops_demo ADD COLUMN stock_note STRING COMMENT 'Added in lesson 11.2';
# MAGIC COMMENT ON TABLE lakeshore.silver.table_ops_demo IS 'Throwaway table for lesson 11.2';
# MAGIC ALTER TABLE lakeshore.silver.table_ops_demo SET TBLPROPERTIES ('lakeshore.purpose' = 'demo');
# MAGIC ALTER TABLE lakeshore.silver.table_ops_demo RENAME TO lakeshore.silver.table_ops_renamed;
# MAGIC ALTER TABLE lakeshore.silver.table_ops_renamed RENAME TO lakeshore.silver.table_ops_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delete, and change your mind
# MAGIC
# MAGIC On a managed table `DROP` starts a recovery window, 7 days by default, and after it
# MAGIC the files are purged. `UNDROP TABLE` works inside that window.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE lakeshore.silver.table_ops_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES DROPPED IN lakeshore.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC UNDROP TABLE lakeshore.silver.table_ops_demo;

# COMMAND ----------

n = spark.table("lakeshore.silver.table_ops_demo").count()
print(f"back from the dead: {n} rows")
assert n == 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The rules, measured
# MAGIC
# MAGIC Free Edition has no external location, so these four commands cannot succeed here.
# MAGIC We run them anyway and read the error class. Each one states a rule the exam tests.

# COMMAND ----------

def show_refusal(sql):
    """Run a statement that must fail, and print which error it raised."""
    try:
        spark.sql(sql)
    except Exception as e:
        print(f"{e.getErrorClass()}\n    {sql}\n")
        return e.getErrorClass()
    raise AssertionError(f"expected a refusal: {sql}")

# An external table needs an external location covering that path. We have none.
show_refusal("CREATE EXTERNAL TABLE lakeshore.silver.ops_external (id INT) LOCATION 's3://lakeshore-archive/orders/'")

# SET MANAGED converts an EXTERNAL table. This one is already managed.
show_refusal("ALTER TABLE lakeshore.silver.table_ops_demo SET MANAGED")

# UNSET MANAGED only rolls back a table that SET MANAGED converted.
show_refusal("ALTER TABLE lakeshore.silver.table_ops_demo UNSET MANAGED")

# SET EXTERNAL is real, but it converts FOREIGN tables. It is not managed-to-external.
show_refusal("ALTER TABLE lakeshore.silver.table_ops_demo SET EXTERNAL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The copy route: it works anywhere, and it starts a new table
# MAGIC
# MAGIC CTAS and deep clone are copies, so they are always available. This is also the only
# MAGIC general route from managed to external: copy into a new table with a `LOCATION`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.table_ops_copy AS
# MAGIC SELECT * FROM lakeshore.silver.table_ops_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, operation FROM (DESCRIBE HISTORY lakeshore.silver.table_ops_copy) ORDER BY version;

# COMMAND ----------

# MAGIC %md
# MAGIC A fresh history. `SET MANAGED` is what keeps the name, grants and history. CTAS does not.
# MAGIC
# MAGIC ## 6. Clean up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.table_ops_demo;
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.table_ops_copy;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Managed is the default · `ALTER` works the same on both types · `DROP` plus `UNDROP`
# MAGIC inside the window · external needs an external location · `SET MANAGED` is
# MAGIC external-to-managed · `UNSET MANAGED` is the 14-day rollback · `SET EXTERNAL` is for
# MAGIC foreign tables.

# COMMAND ----------

dbutils.notebook.exit("table-operations-complete")
