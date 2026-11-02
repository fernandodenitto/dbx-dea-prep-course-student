# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.4 — Reshaping: split, parse, explode, and changing the table itself
# MAGIC
# MAGIC Two entities still stuck in bronze with structural problems: refunds carry a
# MAGIC **packed column** (`"DAMAGED|self_service"`), and orders hide their items in
# MAGIC a **nested array**. Today both reach silver — and orders become TWO tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Refunds: splitting a packed column

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT refund_id, reason FROM lakeshore.bronze.refunds LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.refunds AS
# MAGIC SELECT
# MAGIC   refund_id,
# MAGIC   payment_id,
# MAGIC   CAST(refund_ts AS TIMESTAMP)      AS refund_ts,
# MAGIC   amount                            AS refund_amount,
# MAGIC   split(reason, '\\|')[0]           AS reason,
# MAGIC   split(reason, '\\|')[1]           AS refund_source
# MAGIC FROM lakeshore.bronze.refunds;
# MAGIC
# MAGIC SELECT reason, refund_source, count(*) AS n
# MAGIC FROM lakeshore.silver.refunds
# MAGIC GROUP BY ALL ORDER BY n DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC `split` returns an array; `[0]`/`[1]` index into it. The `\\|` escapes the
# MAGIC pipe (it's a regex). The rename happened inline with `AS`; section 3 runs the
# MAGIC PySpark version. One packed column → two queryable ones.
# MAGIC
# MAGIC ## 2. Orders: the nested array problem
# MAGIC
# MAGIC One order row contains a whole shopping cart. Worse: remember lesson 5.5 —
# MAGIC Auto Loader's JSON inference kept bronze raw, so `items` is a JSON
# MAGIC **string**, not an array yet. Two moves, then: **parse** (`from_json` with
# MAGIC an explicit schema), and **explode**. The relational fix: two tables.

# COMMAND ----------

from pyspark.sql.functions import col, explode, from_json, size, to_timestamp

ITEMS_SCHEMA = "ARRAY<STRUCT<product_id: STRING, quantity: INT, unit_price: DOUBLE>>"

orders_parsed = (spark.table("lakeshore.bronze.orders")
                 .withColumn("order_ts", to_timestamp(col("order_ts")))
                 .withColumn("items", from_json(col("items"), ITEMS_SCHEMA)))

orders_parsed.select("order_id", "items").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC `from_json(column, schema)` turns the string into a real
# MAGIC `array<struct<…>>` — and note we DECLARED the schema instead of inferring
# MAGIC it: at silver, types are a contract, not a guess. (This string→structure
# MAGIC step is exactly why bronze-as-raw is safe: parsing logic can change and
# MAGIC replay.)

# COMMAND ----------

# Table 1: order HEADERS — one row per order, array summarized
orders_silver = (orders_parsed
                 .withColumn("n_items", size(col("items")))
                 .select("order_id", "customer_id", "order_ts",
                         "sales_channel", "n_items", "source_file"))

orders_silver.write.mode("overwrite").saveAsTable("lakeshore.silver.orders")
print(f"silver.orders: {spark.table('lakeshore.silver.orders').count()} headers")

# COMMAND ----------

# Table 2: order ITEMS — explode() turns each array element into its own row
order_items = (orders_parsed
               .select("order_id", "customer_id", "order_ts",
                       explode(col("items")).alias("item"))
               .select("order_id", "customer_id", "order_ts",
                       col("item.product_id").alias("product_id"),
                       col("item.quantity").alias("quantity"),
                       col("item.unit_price").alias("unit_price"))
               .withColumn("line_total", col("quantity") * col("unit_price")))

order_items.write.mode("overwrite").saveAsTable("lakeshore.silver.order_items")
order_items.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC `explode(items)` is the star: one order with 3 items becomes 3 rows, each
# MAGIC carrying its parent's keys. Then dot-notation (`item.product_id`) unpacks
# MAGIC the struct fields, and a derived `line_total` column completes the
# MAGIC reshape.
# MAGIC
# MAGIC One silent trap: `explode` on a NULL or empty array produces **no rows**, so the
# MAGIC parent disappears. `explode_outer` keeps it, with NULLs. Three tiny rows make the
# MAGIC point:

# COMMAND ----------

from pyspark.sql.functions import explode_outer

tags = spark.createDataFrame(
    [("A", ["x", "y"]), ("B", None), ("C", [])], "id string, tags array<string>")

n_explode = tags.select("id", explode("tags")).count()
n_outer   = tags.select("id", explode_outer("tags")).count()
print(f"explode: {n_explode} rows · explode_outer: {n_outer} rows")
assert (n_explode, n_outer) == (2, 4)

# COMMAND ----------

n_orders = spark.table("lakeshore.silver.orders").count()
n_items  = spark.table("lakeshore.silver.order_items").count()
n_sum    = spark.table("lakeshore.silver.orders").agg({"n_items": "sum"}).first()[0]
assert n_items == n_sum, f"items {n_items} != sum of n_items {n_sum}"
assert (n_orders, n_items) == (1080, 2602), (n_orders, n_items)
print(f"✓ {n_orders} orders exploded into {n_items} item rows — totals reconcile")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The column and row verbs, for real
# MAGIC
# MAGIC Add, drop, rename, filter. Nothing is written here: silver stays as it is.

# COMMAND ----------

items = spark.table("lakeshore.silver.order_items")

reshaped = (items
            .withColumn("is_bulk", col("quantity") >= 3)      # add a column
            .drop("customer_id")                              # drop a column
            .withColumnRenamed("line_total", "gross_total")   # rename a column
            .filter(col("unit_price") > 50))                  # keep some rows

print(reshaped.columns)
print(f"{reshaped.count()} of {items.count()} lines cost over 50 a unit")
assert "customer_id" not in reshaped.columns and "gross_total" in reshaped.columns
assert reshaped.count() < items.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The SQL spelling of all four
# MAGIC SELECT * EXCEPT (customer_id, line_total),
# MAGIC        quantity >= 3 AS is_bulk,
# MAGIC        line_total    AS gross_total
# MAGIC FROM lakeshore.silver.order_items
# MAGIC WHERE unit_price > 50
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Changing the table itself: `ALTER TABLE`
# MAGIC
# MAGIC Everything so far changed a *query's* output. "Table structures" means the stored
# MAGIC table. A throwaway table, because we don't experiment on silver. `ADD COLUMN`
# MAGIC just works:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.alter_demo (
# MAGIC   order_id   STRING,
# MAGIC   qty        INT,
# MAGIC   unit_price DOUBLE
# MAGIC );
# MAGIC INSERT INTO lakeshore.silver.alter_demo VALUES ('O-1', 2, 10.0);
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.alter_demo ADD COLUMN currency STRING;

# COMMAND ----------

# Renaming is another story: on a default table, this fails
try:
    spark.sql("ALTER TABLE lakeshore.silver.alter_demo RENAME COLUMN qty TO quantity")
    outcome = "renamed"
except Exception as e:
    outcome = str(e)

assert "ENABLE_COLUMN_MAPPING" in outcome, outcome
print(outcome.splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC The error tells you the fix. **Column mapping** separates a column's logical name
# MAGIC from the physical name in the Parquet files, so a rename or a drop only has to
# MAGIC change metadata. `RENAME COLUMN` and `DROP COLUMN` both require it.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakeshore.silver.alter_demo
# MAGIC   SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.alter_demo RENAME COLUMN qty TO quantity;
# MAGIC ALTER TABLE lakeshore.silver.alter_demo DROP COLUMN currency;
# MAGIC
# MAGIC SELECT * FROM lakeshore.silver.alter_demo;

# COMMAND ----------

demo = spark.table("lakeshore.silver.alter_demo")
assert demo.columns == ["order_id", "quantity", "unit_price"], demo.columns
assert demo.first().quantity == 2
print(f"✓ columns now {demo.columns}, and the row survived")
spark.sql("DROP TABLE lakeshore.silver.alter_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Beyond the guide: arrays without exploding
# MAGIC
# MAGIC The guide names exploding; it doesn't name these. They're here because explode
# MAGIC changes the grain, and sometimes you want to stay at one row per order.
# MAGIC **Higher-order functions** take an array and a lambda:
# MAGIC
# MAGIC | Function | Does | Returns |
# MAGIC |---|---|---|
# MAGIC | `transform(arr, x -> …)` | computes something per element | array |
# MAGIC | `filter(arr, x -> …)` | keeps some elements | array |
# MAGIC | `exists(arr, x -> …)` | asks whether any element matches | boolean |
# MAGIC | `aggregate(arr, start, (acc, x) -> …)` | folds the array to one value | one value |

# COMMAND ----------

from pyspark.sql.functions import expr

carts = orders_parsed.select(
    "order_id",
    expr("transform(items, x -> x.quantity * x.unit_price)").alias("line_totals"),
    expr("filter(items, x -> x.quantity >= 2)").alias("multi_unit_lines"),
    expr("exists(items, x -> x.unit_price > 100)").alias("has_premium_item"),
    expr("aggregate(items, 0D, (acc, x) -> acc + x.quantity * x.unit_price)").alias("cart_total"),
)
carts.show(3, truncate=60)

# COMMAND ----------

# The fold and the explode must tell the same story, to the cent
fold    = carts.agg({"cart_total": "sum"}).first()[0]
explded = spark.table("lakeshore.silver.order_items").agg({"line_total": "sum"}).first()[0]
assert abs(fold - explded) < 0.01, f"{fold} vs {explded}"
assert carts.count() == 1080
print(f"✓ one row per order, and the same revenue: {fold:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Beyond the guide: reading JSON strings in place
# MAGIC
# MAGIC `from_json` is the silver answer. For a quick look at a raw string column, the
# MAGIC **colon** walks a JSON path without declaring a schema. It returns strings, so
# MAGIC cast with `::`. And `parse_json` gives you a `VARIANT`, a typed semi-structured
# MAGIC column that takes the same path syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   items:[0].product_id              AS first_product,
# MAGIC   items:[0].quantity::int           AS first_quantity,
# MAGIC   items:[*].product_id              AS all_products,
# MAGIC   parse_json(items):[0].unit_price::double AS first_price_via_variant
# MAGIC FROM lakeshore.bronze.orders
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC And the promise from lesson 5.2: `text.` reads a file as raw lines when it's too
# MAGIC broken for its own format, in a single column called `value`. The colon then walks
# MAGIC the JSON straight out of the line, with no parser having to agree first.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   value:order_id                 AS order_id,
# MAGIC   value:items[0].product_id      AS first_product
# MAGIC FROM text.`/Volumes/lakeshore/landing/files/orders/`
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC `split(col, '\\|')[i]` unpacks packed strings · `explode(array)` = one row
# MAGIC per element, `explode_outer` keeps the parents of NULL arrays · dot-notation reads
# MAGIC struct fields · header/detail split is the relational answer to nested data, and
# MAGIC always reconcile the counts · `withColumn`, `drop`, `withColumnRenamed`, `filter`
# MAGIC change a query's output · `ALTER TABLE` changes the table: ADD COLUMN works,
# MAGIC RENAME and DROP COLUMN need column mapping · beyond the guide: higher-order
# MAGIC functions, the colon, `VARIANT`.

# COMMAND ----------

dbutils.notebook.exit("reshaping-complete")
