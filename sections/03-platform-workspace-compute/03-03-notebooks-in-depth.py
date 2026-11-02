# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 3.3 — Notebook power tools
# MAGIC
# MAGIC Databricks notebooks look like Jupyter, but they ship with tools you'll use in
# MAGIC every lesson from now on: **magic commands**, **dbutils**, and **widgets**.
# MAGIC This cell itself is one of them — `%md` renders Markdown.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Magic commands: switching languages per cell
# MAGIC
# MAGIC This is a **Python** notebook (see the default language next to the title), but
# MAGIC any single cell can switch language with a magic command: `%sql`, `%python`,
# MAGIC `%md`. One notebook, several languages — that's normal Databricks style.
# MAGIC
# MAGIC > Legacy note: you may see `%fs` and `%sh` in older courses. `%fs` is the old
# MAGIC > file-system magic — prefer `dbutils.fs` (below). `%sh` runs shell commands on
# MAGIC > the driver — rarely needed in this course.

# COMMAND ----------

print(f"This cell is Python. Spark {spark.version} is already here.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same notebook, SQL cell: query the Lakeshore catalog we built in Section 2
# MAGIC SHOW SCHEMAS IN lakeshore;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dbutils: the notebook's Swiss army knife
# MAGIC
# MAGIC `dbutils` is a utility object available in every notebook. The module you'll
# MAGIC use constantly is `dbutils.fs` — file-system operations on volumes.

# COMMAND ----------

files = dbutils.fs.ls("/Volumes/lakeshore/landing/files/orders")
for f in files:
    print(f"{f.name:<28} {f.size:>8,} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC Other `dbutils` modules, so you recognize them when they appear:
# MAGIC
# MAGIC | Module | What it does | Where we use it |
# MAGIC |---|---|---|
# MAGIC | `dbutils.fs` | files on volumes (ls, cp, rm, mkdirs) | everywhere |
# MAGIC | `dbutils.widgets` | notebook parameters | below + Lakeflow Jobs (S8) |
# MAGIC | `dbutils.notebook` | exit with a result, run other notebooks | job tasks (S8) |
# MAGIC | `dbutils.secrets` | read secret scopes (credentials) | lesson 5.8 |
# MAGIC
# MAGIC Full reference: run `dbutils.help()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Widgets: parameters for your notebook
# MAGIC
# MAGIC Widgets add input fields at the top of the notebook — and, crucially, **they're
# MAGIC how Lakeflow Jobs pass parameters to a notebook task** (Section 8). Define a
# MAGIC default, read the value, and the notebook works both interactively and in a job.

# COMMAND ----------

dbutils.widgets.text("entity", "orders", "Entity to inspect")
entity = dbutils.widgets.get("entity")

print(f"Inspecting entity: {entity}")
for f in dbutils.fs.ls(f"/Volumes/lakeshore/landing/files/{entity}"):
    print(f"  {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Try it: change the widget at the top to `payments` — the cell above re-runs
# MAGIC against a different folder. Same code, parameterized.
# MAGIC
# MAGIC ## 4. Reusing code: `%run` vs importing files
# MAGIC
# MAGIC Two ways to share code between notebooks:
# MAGIC
# MAGIC - **`%run ./other-notebook`** — executes another notebook inside this one and
# MAGIC   inherits everything it defines. The traditional pattern; you'll see it in most
# MAGIC   older courses.
# MAGIC - **Importing a workspace `.py` file** — what we do with `datagen.py`:
# MAGIC   `sys.path` + `import`. The modern pattern: real Python modules, testable and
# MAGIC   reusable outside notebooks too.
# MAGIC
# MAGIC This course prefers imports; you need to *recognize* `%run` when you meet it.
# MAGIC
# MAGIC The rule: **a module imports only from a folder on `sys.path`.** Databricks adds the
# MAGIC notebook's folder and, in a Git folder, the repo root. `datagen.py` sits in
# MAGIC `datagen`, which is neither, so we walk up to the repo root and add it:

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")           # make datagen.py and feed.py importable

import datagen

print(f"Notebook folder, from the repo root: {os.path.relpath(os.getcwd(), root)}")
print(f"Module loaded from:                  {os.path.relpath(datagen.__file__, root)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Genie Code (formerly Databricks Assistant)
# MAGIC
# MAGIC The sparkle icon (top right of every cell) opens **Genie Code** (formerly the Databricks Assistant):
# MAGIC it can explain a cell, fix an error, or draft code from a comment. Try asking it
# MAGIC to explain the widgets cell above. Use it like a colleague — verify what it
# MAGIC writes, especially column names.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Magics switch languages per cell · `dbutils.fs` handles volume files ·
# MAGIC widgets parameterize notebooks (and connect them to Jobs) · imports beat `%run`
# MAGIC for shared code · Genie Code is your pair programmer.

# COMMAND ----------

dbutils.notebook.exit("notebook-tools-complete")
