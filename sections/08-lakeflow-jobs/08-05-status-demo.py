# Databricks notebook source
# MAGIC %md
# MAGIC # 8.5 — A notebook that fails on request
# MAGIC
# MAGIC Every task of `lakeshore-status-demo` runs this notebook. Two parameters decide how
# MAGIC it ends, so you can produce each status on purpose and then repair it, without
# MAGIC touching a real table.

# COMMAND ----------

import time

dbutils.widgets.text("should_fail", "false")
dbutils.widgets.text("sleep_seconds", "0")

should_fail = dbutils.widgets.get("should_fail")
sleep_seconds = int(dbutils.widgets.get("sleep_seconds"))

print(f"should_fail={should_fail}, sleep_seconds={sleep_seconds}")

# COMMAND ----------

time.sleep(sleep_seconds)

if should_fail == "true":
    raise ValueError("Deliberate failure: should_fail is true")

print("finished normally")

# COMMAND ----------

dbutils.notebook.exit("status-demo-complete")
