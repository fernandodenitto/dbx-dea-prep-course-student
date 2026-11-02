# Databricks notebook source
# MAGIC %md
# MAGIC # 8.3 — The task inside a For each loop
# MAGIC
# MAGIC One notebook, one parameter. The job runs it once per input. Each iteration gets
# MAGIC its value through `{{input}}`, mapped to the `table_name` parameter on the nested
# MAGIC task, and reads it here with a widget.

# COMMAND ----------

dbutils.widgets.text("table_name", "lakeshore.silver.orders")
table_name = dbutils.widgets.get("table_name")

n = spark.table(table_name).count()
print(f"{table_name}: {n} rows")

assert n > 0, f"{table_name} is empty"

# COMMAND ----------

dbutils.notebook.exit(f"{table_name}={n}")
