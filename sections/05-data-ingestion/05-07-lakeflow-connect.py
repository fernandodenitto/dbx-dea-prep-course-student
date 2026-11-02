# Databricks notebook source
# MAGIC %md
# MAGIC # Create a table from a file upload
# MAGIC
# MAGIC The smallest ingestion path in the product: a file on your laptop becomes a
# MAGIC governed Delta table, no code. This notebook makes the file for you, checks the
# MAGIC result, and cleans up. The upload itself happens in the UI, because that is the
# MAGIC point of the path.

# COMMAND ----------

UPLOAD_DIR = "/Volumes/lakeshore/landing/files/_upload_demo"
UPLOAD_FILE = f"{UPLOAD_DIR}/shipping_regions.csv"
TARGET = "lakeshore.bronze.shipping_regions_upload"

CSV = """region_code,region_name,free_shipping_threshold,delivery_days
NE,North East,75.00,2
SE,South East,75.00,3
MW,Midwest,90.00,3
SW,South West,90.00,4
WC,West Coast,100.00,4
AK,Alaska and Hawaii,150.00,7
"""

dbutils.fs.mkdirs(UPLOAD_DIR)
dbutils.fs.put(UPLOAD_FILE, CSV, overwrite=True)
print(f"Wrote {UPLOAD_FILE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your turn, in the UI
# MAGIC
# MAGIC 1. **Catalog** → `lakeshore` → `landing` → volume `files` → `_upload_demo`. Download
# MAGIC    `shipping_regions.csv` to your laptop.
# MAGIC 2. Sidebar → **Data Ingestion** → under Files, **Create or modify table**.
# MAGIC 3. Drop the file on the **File upload** tab. The page accepts up to 10 files,
# MAGIC    2 GB in total.
# MAGIC 4. Set the target: catalog `lakeshore`, schema `bronze`, table
# MAGIC    `shipping_regions_upload`. Check the 50-row preview and the inferred types,
# MAGIC    then create the table.
# MAGIC 5. Come back and run the next cell.

# COMMAND ----------

if spark.catalog.tableExists(TARGET):
    df = spark.table(TARGET)
    df.show()
    assert df.count() == 6
    print(dict(df.dtypes))
else:
    print(f"{TARGET} is not there yet: do the upload above, then run this cell again.")

# COMMAND ----------

# MAGIC %md
# MAGIC Six rows and typed columns: a managed Delta table, with an owner and a history,
# MAGIC and nothing remembers which file it came from. Upload is a one-off path with no
# MAGIC incremental bookkeeping, which is why it answers "someone needs this spreadsheet
# MAGIC in a table once" and never "this file arrives every night".
# MAGIC
# MAGIC ## Clean up
# MAGIC
# MAGIC The table was a demo, and no later lesson uses it.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
dbutils.fs.rm(UPLOAD_DIR, recurse=True)
print("demo table and file removed")

# COMMAND ----------

dbutils.notebook.exit("file-upload-complete")
