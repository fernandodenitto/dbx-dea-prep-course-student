# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader: schema enforcement & evolution
# MAGIC
# MAGIC Sources change. One day the orders feed adds a `sales_channel` field nobody
# MAGIC announced. What should the pipeline do — crash, quarantine it, or adapt?
# MAGIC Auto Loader lets you *choose*, and the exam tests exactly that choice.
# MAGIC
# MAGIC To see it the way production sees it, we run the real ingestion through a helper
# MAGIC notebook, `05-06-ingest-once`, sitting next to this one in the lesson's
# MAGIC folder — the same role a task plays inside a Lakeflow Job: it can fail, and the
# MAGIC caller decides to retry.

# COMMAND ----------

import os, sys

root = os.getcwd()                                  # the notebook's folder
while root != "/" and not os.path.isdir(f"{root}/datagen"):
    root = os.path.dirname(root)                    # walk up to the repo root
sys.path.append(f"{root}/datagen")           # make datagen.py and feed.py importable

import feed

ORDERS_SRC = "/Volumes/lakeshore/landing/files/orders"

def ingest_once():
    """Run one ingestion pass as a child notebook — like an orchestrated task."""
    return dbutils.notebook.run("./05-06-ingest-once", timeout_seconds=600)

def task_failed(error):
    """True when the child notebook ran and failed. A wrong path or a timeout must surface."""
    return "Unable to access the notebook" not in str(error) and "TIMEDOUT" not in str(error)

# Catch up on anything pending. If a previous attempt of this lesson left a schema
# change half-absorbed, the first pass fails exactly like an orchestrated task would,
# so we do what an orchestrator does: retry once. A wrong path or a timeout is raised,
# and so is anything that fails twice.
try:
    print(ingest_once())
except Exception as e:
    if not task_failed(e):
        raise
    print("… the task failed, possibly on a pending schema change — retrying:")
    print(ingest_once())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Enforcement: a record that does not fit
# MAGIC
# MAGIC Enforcement means a value that breaks the schema never lands in the typed
# MAGIC column. We prove it on a small demo folder with its own checkpoint, schema and
# MAGIC tables, so the real orders stay untouched. `D2` carries text where `amount`
# MAGIC should be a number, and a schema hint says `amount` is a DOUBLE.

# COMMAND ----------

import json

DEMO = "/Volumes/lakeshore/landing/files/_schema_demo"
MODES = ["hint", "rescue", "fail"]

def drop_demo():
    """Remove every demo object, so the lesson can be re-run from a clean slate."""
    dbutils.fs.rm(DEMO, recurse=True)
    for name in MODES:
        spark.sql(f"DROP TABLE IF EXISTS lakeshore.bronze.schema_demo_{name}")

def land(name, filename, rows):
    """Write one small JSON Lines file into the demo folder of one mode."""
    os.makedirs(f"{DEMO}/{name}/in", exist_ok=True)
    with open(f"{DEMO}/{name}/in/{filename}", "w") as f:
        f.write("\n".join(json.dumps(r) for r in rows))

def demo_pass(name, options):
    """One Auto Loader pass for one mode. Returns the demo table."""
    table = f"lakeshore.bronze.schema_demo_{name}"
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", f"{DEMO}/{name}/schema"))
    for key, value in options.items():
        reader = reader.option(key, value)
    (reader.load(f"{DEMO}/{name}/in")
     .writeStream
     .option("checkpointLocation", f"{DEMO}/{name}/checkpoint")
     .trigger(availableNow=True)
     .toTable(table)).awaitTermination()
    return spark.table(table)

drop_demo()
land("hint", "day1.json", [{"order_id": "D1", "amount": 19.99},
                           {"order_id": "D2", "amount": "not-a-number"}])

df = demo_pass("hint", {"cloudFiles.schemaHints": "amount DOUBLE"})
df.orderBy("order_id").show(truncate=False)

bad = df.where("order_id = 'D2'").first()
assert bad["amount"] is None and "not-a-number" in bad["_rescued_data"]

# COMMAND ----------

# MAGIC %md
# MAGIC `D2` did not crash the stream and did not corrupt `amount`: the column is NULL,
# MAGIC and the original value sits in `_rescued_data`, with the file it came from.
# MAGIC That is enforcement. Without the hint, inference would make `amount` a STRING
# MAGIC and there would be nothing to enforce.
# MAGIC
# MAGIC ## 2. The surprise: a batch with a NEW column
# MAGIC
# MAGIC Now the real orders. Batch 101 carries `sales_channel` — a field the schema has
# MAGIC never seen. In the default evolution mode (`addNewColumns`) the stream is expected
# MAGIC to **stop** on first contact:

# COMMAND ----------

feed.emit_batch("orders", ORDERS_SRC, batch_num=101,
                extra_fields={"sales_channel": "mobile_app"})

try:
    print(ingest_once())
    print("(No failure — the schema had already evolved in a previous attempt.)")
except Exception as e:
    if not task_failed(e):
        raise
    print("⚡ The ingestion task FAILED — by design.")
    print("Auto Loader met an unknown column, updated its schemaLocation,")
    print("and stopped the stream so nothing gets written half-understood.")
    print("Rows in bronze right now:", spark.table("lakeshore.bronze.orders").count())

# COMMAND ----------

# MAGIC %md
# MAGIC **This is not a bug.** Default mode fails the stream on a new column —
# MAGIC *after* recording the new schema — and expects the orchestrator (a Lakeflow
# MAGIC Job with retries, Section 8) to restart it. So we do what the orchestrator
# MAGIC does — run it again:

# COMMAND ----------

print(ingest_once())

# COMMAND ----------

new_col_rows = spark.sql("""
  SELECT count(*) AS n FROM lakeshore.bronze.orders
  WHERE sales_channel IS NOT NULL
""").first()["n"]

total = spark.table("lakeshore.bronze.orders").count()
print(f"orders total: {total} — rows with sales_channel: {new_col_rows}")
assert total == 1080
assert new_col_rows == 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, sales_channel, source_file
# MAGIC FROM lakeshore.bronze.orders
# MAGIC WHERE sales_channel IS NOT NULL
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC The table evolved: old rows have `sales_channel = NULL`, new rows carry the
# MAGIC value. Zero data lost, one controlled restart — failure as a *feature*. Bronze
# MAGIC orders now hold 1,080 rows, the number the rest of the course builds on.
# MAGIC
# MAGIC ## 3. Two other choices, in practice
# MAGIC
# MAGIC Same new field, on the demo folder, under two other modes. First `rescue`: one
# MAGIC clean file, then a file with an extra `coupon` field.

# COMMAND ----------

clean = [{"order_id": "D1", "amount": "19.99"}]
extra = [{"order_id": "D3", "amount": "5.00", "coupon": "WELCOME10"}]

land("rescue", "day1.json", clean)
demo_pass("rescue", {"cloudFiles.schemaEvolutionMode": "rescue"})
land("rescue", "day2.json", extra)
df = demo_pass("rescue", {"cloudFiles.schemaEvolutionMode": "rescue"})

df.orderBy("order_id").show(truncate=False)
assert "coupon" not in df.columns
assert "WELCOME10" in df.where("order_id = 'D3'").first()["_rescued_data"]

# COMMAND ----------

# MAGIC %md
# MAGIC No failure, no new column, and `WELCOME10` is safe in `_rescued_data`. Now
# MAGIC `failOnNewColumns`, with the same two files, and we restart it twice. It runs as
# MAGIC a child notebook, `05-06-fail-on-new-columns`, because a failed stream fails
# MAGIC its cell even inside a try/except.

# COMMAND ----------

def fail_mode_pass():
    return dbutils.notebook.run("./05-06-fail-on-new-columns", timeout_seconds=600)

land("fail", "day1.json", clean)
print(fail_mode_pass())
land("fail", "day2.json", extra)

for attempt in (1, 2):
    try:
        fail_mode_pass()
        raise AssertionError("failOnNewColumns should have stopped the stream")
    except Exception as e:
        assert task_failed(e) and not isinstance(e, AssertionError), e
        print(f"attempt {attempt}: the task failed on the new column")

assert "coupon" not in spark.table("lakeshore.bronze.schema_demo_fail").columns

# COMMAND ----------

drop_demo()
print("demo objects removed — bronze.orders untouched")

# COMMAND ----------

# MAGIC %md
# MAGIC Restarting does not help here: a human has to change the schema or remove the
# MAGIC file. That is the tripwire you choose on purpose.
# MAGIC
# MAGIC ## 4. The five evolution modes (exam table)
# MAGIC
# MAGIC `cloudFiles.schemaEvolutionMode`:
# MAGIC
# MAGIC | Mode | New column arrives → |
# MAGIC |---|---|
# MAGIC | `addNewColumns` (default without a declared schema) | stream stops, schema updated; restart ingests it |
# MAGIC | `addNewColumnsWithTypeWidening` | same, and also widens supported types such as `int` to `long`; other changes go to the rescued column |
# MAGIC | `rescue` | schema frozen, stream keeps going; new fields land in `_rescued_data` |
# MAGIC | `failOnNewColumns` | stream stops and KEEPS failing until you act |
# MAGIC | `none` (default when you declare `.schema(...)`) | new fields ignored, not rescued unless `rescuedDataColumn` is set |
# MAGIC
# MAGIC `addNewColumns` is not allowed with a declared `.schema(...)`. For columns you
# MAGIC know in advance, `cloudFiles.schemaHints` (e.g. `'unit_price DOUBLE'`) beats
# MAGIC letting inference guess, and it keeps evolution available.
# MAGIC
# MAGIC **Evolution has two ends** — a detail we learned by breaking it while
# MAGIC building this course: `schemaEvolutionMode` evolves Auto Loader's *source*
# MAGIC schema, but the *sink* Delta table only accepts the new column if the
# MAGIC writer carries `.option("mergeSchema", "true")`. Forget it and the restart
# MAGIC fails again — with a `DELTA_METADATA_MISMATCH` telling you exactly that.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Enforcement: a value that doesn't fit lands in `_rescued_data`, never in the
# MAGIC typed column · default evolution: fail once (schema recorded) → restart ingests
# MAGIC the new column — which is WHY streams live inside orchestrators with retries ·
# MAGIC `rescue` freezes schema and quarantines novelty in `_rescued_data` ·
# MAGIC `failOnNewColumns` keeps failing until a human acts · hints > inference for
# MAGIC known types.

# COMMAND ----------

dbutils.notebook.exit("schema-evolution-complete")
