# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.6 — Data quality checks & validation rules
# MAGIC
# MAGIC The silver layer is built. Before anything downstream trusts it, we PROVE
# MAGIC it: a battery of explicit checks — reconciliation, integrity, validity,
# MAGIC freshness. Manual today (so you understand each one), automated by pipeline
# MAGIC expectations in Section 7.

# COMMAND ----------

# MAGIC %md
# MAGIC Each check is a SQL query expected to return **zero bad rows** (or a
# MAGIC reconciling count). We run them as a battery and fail loudly on the first
# MAGIC violation — quality checks that only print warnings get ignored.

# COMMAND ----------

CHECKS = [
    ("customers: exactly one row per customer_id",
     """SELECT count(*) FROM (SELECT customer_id FROM lakeshore.silver.customers
        GROUP BY customer_id HAVING count(*) > 1)"""),

    ("orders: header count matches bronze distinct orders",
     """SELECT (SELECT count(*) FROM lakeshore.silver.orders) -
               (SELECT count(DISTINCT order_id) FROM lakeshore.bronze.orders)"""),

    ("order_items: every item's order exists (no orphans)",
     """SELECT count(*) FROM lakeshore.silver.order_items i
        LEFT ANTI JOIN lakeshore.silver.orders o ON i.order_id = o.order_id"""),

    ("addresses: every address belongs to a known customer",
     """SELECT count(*) FROM lakeshore.silver.addresses a
        LEFT ANTI JOIN lakeshore.silver.customers c ON a.customer_id = c.customer_id"""),

    ("order_items: every product_id exists in the catalog",
     """SELECT count(*) FROM lakeshore.silver.order_items i
        LEFT ANTI JOIN lakeshore.silver.products p ON i.product_id = p.product_id"""),

    ("payments: every payment maps to an existing order",
     """SELECT count(*) FROM lakeshore.silver.payments p
        LEFT ANTI JOIN lakeshore.silver.orders o ON p.order_id = o.order_id"""),

    ("refunds: every refund maps to a payment, never exceeding it",
     """SELECT count(*) FROM lakeshore.silver.refunds r
        LEFT JOIN lakeshore.silver.payments p ON r.payment_id = p.payment_id
        WHERE p.payment_id IS NULL OR r.refund_amount > p.amount"""),

    ("payments: no negative or zero amounts",
     """SELECT count(*) FROM lakeshore.silver.payments WHERE amount <= 0"""),

    ("orders: no timestamps in the future",
     """SELECT count(*) FROM lakeshore.silver.orders
        WHERE order_ts > current_timestamp()"""),
]

failures = 0
for name, sql in CHECKS:
    bad = spark.sql(sql).first()[0]
    status = "✓" if bad == 0 else "✗"
    if bad != 0:
        failures += 1
    print(f"{status} {name} — violations: {bad}")

assert failures == 0, f"{failures} quality checks failed"
print("\nALL QUALITY CHECKS PASSED — silver is trustworthy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the battery
# MAGIC
# MAGIC Four families, and every real quality suite is built from them:
# MAGIC
# MAGIC - **Uniqueness** — one row per business key (customers).
# MAGIC - **Reconciliation** — silver explains 100% of bronze (order counts).
# MAGIC - **Referential integrity** — the `LEFT ANTI JOIN` pattern: rows on the
# MAGIC   left with NO match on the right. Zero is the only good answer.
# MAGIC - **Validity & freshness** — domain rules (amounts > 0, refund ≤ payment,
# MAGIC   no future timestamps).
# MAGIC
# MAGIC One known blemish, on purpose: exactly 10 of 200 silver customers have NULL
# MAGIC emails, 5.0% — our documented policy from lesson 6.1, monitored rather than hidden:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count_if(email IS NULL) AS customers_without_email,
# MAGIC        round(100 * count_if(email IS NULL) / count(*), 1) AS pct
# MAGIC FROM lakeshore.silver.customers;

# COMMAND ----------

missing = spark.sql("SELECT count_if(email IS NULL) FROM lakeshore.silver.customers").first()[0]
assert missing == 10, f"policy says 10 NULL emails from lesson 6.1, found {missing}"
print(f"✓ known blemish unchanged: {missing} customers without an email")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Quality = explicit, runnable rules that FAIL loudly · four families:
# MAGIC uniqueness, reconciliation, integrity (LEFT ANTI JOIN), validity/freshness ·
# MAGIC document accepted blemishes instead of hiding them · Section 7 turns these
# MAGIC into pipeline **expectations** that run on every update automatically.

# COMMAND ----------

dbutils.notebook.exit("quality-checks-complete")
