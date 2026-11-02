# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeshore health check
# MAGIC
# MAGIC Deployed by the bundle — a minimal daily sanity probe over the platform we
# MAGIC built in Sections 2–8. The catalog name arrives as a job parameter (widget).

# COMMAND ----------

dbutils.widgets.text("catalog", "lakeshore")
catalog = dbutils.widgets.get("catalog")

checks = {
    f"{catalog}.silver.customers":    200,
    f"{catalog}.silver.payments":    1000,
    f"{catalog}.bronze.products":      20,
}

for table, minimum in checks.items():
    n = spark.table(table).count()
    print(f"{'✓' if n >= minimum else '✗'} {table}: {n} rows (expected ≥ {minimum})")
    assert n >= minimum, f"{table} below expected volume"

# COMMAND ----------

dbutils.notebook.exit("healthy")
