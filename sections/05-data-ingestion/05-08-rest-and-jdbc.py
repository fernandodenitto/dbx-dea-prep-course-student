# Databricks notebook source
# MAGIC %md
# MAGIC # Landing data with clients: REST hands-on (JDBC, ODBC and Kafka in theory)
# MAGIC
# MAGIC Not everything is a file. Lakeshore sells internationally — pricing wants
# MAGIC **exchange rates**, and those live behind a REST API. A notebook, `requests`
# MAGIC and a Delta write are a perfectly respectable ingestion pattern (the exam
# MAGIC guide says so too: "JDBC/ODBC or REST clients in notebooks… orchestrated and
# MAGIC scheduled with Lakeflow Jobs").

# COMMAND ----------

import json
import os

import requests

API_URL = "https://api.frankfurter.dev/v1/latest?base=USD&symbols=EUR,GBP,CAD,AUD,JPY"

# Free Edition allows outbound calls only to a set of trusted domains. This host answered
# when we recorded. If yours is blocked or down, the lesson still runs from this sample,
# and the print below tells you which path it took.
SAMPLE = {"base": "USD", "date": "2026-08-14",
          "rates": {"EUR": 0.92, "GBP": 0.79, "CAD": 1.37, "AUD": 1.52, "JPY": 147.2}}

try:
    response = requests.get(API_URL, timeout=15)
    response.raise_for_status()                      # a 4xx or 5xx must not look like data
    payload = response.json()
    assert {"base", "date", "rates"} <= payload.keys(), f"unexpected body: {payload}"
    source = "live API"
except Exception as e:
    payload = SAMPLE
    source = f"embedded sample (API call failed: {type(e).__name__})"

print(f"Rates from {source}: base={payload['base']} date={payload['date']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land it in storage first
# MAGIC
# MAGIC The exam statement says "into cloud storage **or** directly into Unity
# MAGIC Catalog-governed tables". We do both. The raw response goes to the volume exactly
# MAGIC as it came: if the parsing is wrong next month, you can replay it. One file per
# MAGIC API date, so a re-run on the same day overwrites instead of piling up.

# COMMAND ----------

RAW_DIR = "/Volumes/lakeshore/landing/files/fx_rates"
os.makedirs(RAW_DIR, exist_ok=True)

raw_path = f"{RAW_DIR}/fx_rates_{payload['date']}.json"
with open(raw_path, "w") as f:
    json.dump(payload, f)
print(f"raw response → {raw_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## JSON → rows → Delta
# MAGIC
# MAGIC The API returns one object with a nested `rates` map; we normalize it to
# MAGIC one row per currency and overwrite the bronze table — a snapshot pattern,
# MAGIC idempotent by construction:

# COMMAND ----------

rows = [(payload["date"], payload["base"], currency, float(rate))
        for currency, rate in payload["rates"].items()]

fx = spark.createDataFrame(
    rows, "rate_date string, base_currency string, currency string, rate double")

(fx.write.mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("lakeshore.bronze.fx_rates"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lakeshore.bronze.fx_rates ORDER BY currency;

# COMMAND ----------

n = spark.table("lakeshore.bronze.fx_rates").count()
assert n == 5
print(f"bronze.fx_rates: {n} currencies")

# COMMAND ----------

# MAGIC %md
# MAGIC In production this notebook runs **on a schedule as a Lakeflow Job task**, with
# MAGIC retries on the task — that's the "orchestrated and scheduled" part of the
# MAGIC statement. Section 8 teaches how.
# MAGIC
# MAGIC ## 📖 JDBC, ODBC and Kafka (theory, no live demo)
# MAGIC
# MAGIC Free Edition has no database or broker for us to dial, and a fake demo teaches
# MAGIC fake lessons.
# MAGIC
# MAGIC **JDBC, the current governed form:** a Unity Catalog connection holds driver, URL
# MAGIC and credentials. Users need `USE CONNECTION` and never see the password.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE CONNECTION shop_pg TYPE JDBCENVIRONMENT (
# MAGIC   java_dependencies '["/Volumes/<catalog>/<schema>/<volume>/postgresql.jar"]')
# MAGIC OPTIONS (url 'jdbc:postgresql://host:5432/shopdb', user '...', password '...',
# MAGIC          externalOptionsAllowList 'dbtable,query');
# MAGIC ```
# MAGIC ```python
# MAGIC df = (spark.read.format("jdbc")
# MAGIC       .option("databricks.connection", "shop_pg")
# MAGIC       .option("dbtable", "public.refunds")
# MAGIC       .load())
# MAGIC ```
# MAGIC
# MAGIC **JDBC, the form older material and exam options still show:** URL plus secrets.
# MAGIC
# MAGIC ```python
# MAGIC df = (spark.read.format("jdbc")
# MAGIC       .option("url", "jdbc:postgresql://host:5432/shopdb")
# MAGIC       .option("dbtable", "public.refunds")
# MAGIC       .option("user", dbutils.secrets.get("lakeshore", "pg_user"))
# MAGIC       .option("password", dbutils.secrets.get("lakeshore", "pg_password"))
# MAGIC       .load())
# MAGIC ```
# MAGIC
# MAGIC - Hardcoded credentials are always wrong.
# MAGIC - JDBC reads use one connection by default. Parallelize with `partitionColumn`,
# MAGIC   `lowerBound`, `upperBound`, `numPartitions`.
# MAGIC - Supported source? A **Lakeflow Connect** connector beats all of this.
# MAGIC
# MAGIC **ODBC:** same idea as JDBC, different driver standard. Spark's distributed reader
# MAGIC speaks JDBC, so in a notebook JDBC is the answer. On Free Edition serverless,
# MAGIC `pyodbc` installs but finds no drivers. The Databricks ODBC Driver is for the other
# MAGIC direction: outside tools connecting into Databricks.
# MAGIC
# MAGIC **Kafka and Event Hubs:** the same stream you know, with a different source. Event
# MAGIC Hubs goes through its Kafka endpoint, `<namespace>.servicebus.windows.net:9093`.
# MAGIC
# MAGIC ```python
# MAGIC raw = (spark.readStream.format("kafka")
# MAGIC        .option("kafka.bootstrap.servers", "broker:9092")
# MAGIC        .option("subscribe", "orders")
# MAGIC        .option("startingOffsets", "earliest")
# MAGIC        .load())
# MAGIC (raw.selectExpr("CAST(value AS STRING) AS value", "topic", "partition", "offset")
# MAGIC  .writeStream.option("checkpointLocation", checkpoint)
# MAGIC  .trigger(availableNow=True)
# MAGIC  .toTable("lakeshore.bronze.orders_events"))
# MAGIC ```
# MAGIC
# MAGIC The default `startingOffsets` for a stream is `latest`, and `key` and `value` arrive
# MAGIC as BINARY, so you cast them yourself.
# MAGIC
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC REST → `requests` → raw file in a volume → normalize → Delta write, scheduled by
# MAGIC a job · snapshot overwrites are naturally idempotent · JDBC = same idea for
# MAGIC databases, governed connection first, secrets otherwise, Lakeflow Connect preferred
# MAGIC when the source is supported.

# COMMAND ----------

dbutils.notebook.exit("rest-ingestion-complete")
