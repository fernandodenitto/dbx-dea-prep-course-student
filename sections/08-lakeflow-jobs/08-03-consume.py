# Databricks notebook source
# MAGIC %md
# MAGIC # 8.3 — Task values: the consumer
# MAGIC
# MAGIC The SECOND task. It reads what the first one set — two different ways, because
# MAGIC the exam cares about the difference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Way one: the Python API
# MAGIC
# MAGIC `debugValue` is the detail worth knowing. Outside a job there is no upstream
# MAGIC task, so the call would fail; `debugValue` is what you get when you run the
# MAGIC notebook interactively, which is what makes this notebook developable.

# COMMAND ----------

row_count = dbutils.jobs.taskValues.get(
    taskKey="produce", key="row_count", debugValue=0)
verdict = dbutils.jobs.taskValues.get(
    taskKey="produce", key="verdict", debugValue="unknown")

print(f"read from the upstream task: row_count={row_count}, verdict={verdict}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Way two: a dynamic value reference, injected as a parameter
# MAGIC
# MAGIC The job passes `{{tasks.produce.values.verdict}}` as a notebook parameter, and
# MAGIC we pick it up with a widget — lesson 3.3's widgets, finally doing the job they
# MAGIC were introduced for.
# MAGIC
# MAGIC This is the form the docs prefer: `taskValues.get()` hard-codes the upstream
# MAGIC task's name in your code, so renaming a task silently breaks it.

# COMMAND ----------

dbutils.widgets.text("verdict_param", "unset")
verdict_param = dbutils.widgets.get("verdict_param")

print(f"read from a dynamic value reference: verdict_param={verdict_param}")

# COMMAND ----------

assert row_count > 0, "the producer should have set a real row count"
assert verdict == verdict_param, (
    f"both routes must agree: API said {verdict!r}, parameter said {verdict_param!r}")
print(f"OK — both routes agree: {verdict}")
