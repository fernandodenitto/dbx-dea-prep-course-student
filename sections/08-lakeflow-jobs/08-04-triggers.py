# Databricks notebook source
# MAGIC %md
# MAGIC # 8.4 — Land one file, fire one run
# MAGIC
# MAGIC The file arrival trigger in this lesson watches a folder of its own,
# MAGIC `landing/files/trigger_demo`. The orders folder would feed the pipeline, and that
# MAGIC would move every canonical number in the course, so we leave it alone.
# MAGIC
# MAGIC Run this once before you add the trigger, which creates the folder, and once after,
# MAGIC which lands the new file that fires it.

# COMMAND ----------

from datetime import datetime, timezone

folder = "/Volumes/lakeshore/landing/files/trigger_demo"
stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
path = f"{folder}/arrival_{stamp}.txt"

dbutils.fs.mkdirs(folder)
dbutils.fs.put(path, f"landed at {stamp}\n", overwrite=False)
print(f"landed {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC The name carries a timestamp because only **new** files fire the trigger:
# MAGIC overwriting an existing file does not. The check runs about once a minute, so give
# MAGIC it a minute or two, then look at the job's **Runs** tab.

# COMMAND ----------

files = sorted(f.name for f in dbutils.fs.ls(folder))
print(f"{len(files)} file(s) in trigger_demo: {files}")
assert path.split("/")[-1] in files

# COMMAND ----------

dbutils.notebook.exit("file-landed")
