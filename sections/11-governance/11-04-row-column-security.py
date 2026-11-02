# Databricks notebook source
# MAGIC %md
# MAGIC # 11.4 — Row and column security: masks, filters, dynamic views
# MAGIC
# MAGIC | Tool | Attached to | Reach for it when |
# MAGIC |---|---|---|
# MAGIC | Column mask + row filter | one table | the rule belongs to this table |
# MAGIC | Dynamic view | a view you grant instead of the table | you expose a curated or joined dataset |
# MAGIC | ABAC policy (next lesson) | a catalog or schema, matched by tag | same rule, many tables |
# MAGIC
# MAGIC We work on a deep clone (lesson 4.6), so the real `silver.customers` stays untouched.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.customers_secure;
# MAGIC CREATE TABLE lakeshore.silver.customers_secure
# MAGIC DEEP CLONE lakeshore.silver.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Column mask
# MAGIC
# MAGIC A mask is a SQL function applied at read time. It gates on a `data_admins` group that
# MAGIC does not exist here, so we see the locked-out experience.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.mask_email(email STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('data_admins') THEN email
# MAGIC   ELSE '### masked ###'
# MAGIC END;
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.customers_secure
# MAGIC ALTER COLUMN email SET MASK lakeshore.silver.mask_email;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, first_name, email
# MAGIC FROM lakeshore.silver.customers_secure ORDER BY customer_id LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row filter
# MAGIC
# MAGIC A function returning a boolean. Non-admins see only the first 100 customers.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.early_customers_only(id STRING)
# MAGIC RETURN is_account_group_member('data_admins') OR id <= 'C00100';
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.customers_secure
# MAGIC SET ROW FILTER lakeshore.silver.early_customers_only ON (customer_id);

# COMMAND ----------

n = spark.sql("SELECT count(*) AS n FROM lakeshore.silver.customers_secure").first()["n"]
masked = spark.sql("""
  SELECT count(*) AS n FROM lakeshore.silver.customers_secure
  WHERE email = '### masked ###'
""").first()["n"]
print(f"visible rows: {n} (of 200), masked emails among them: {masked}")
assert n == 100, f"row filter not applied: {n}"
assert masked == 100, "column mask not applied"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Taking them off
# MAGIC
# MAGIC `DROP`, on the column and on the table. The functions survive: they were never part of
# MAGIC the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakeshore.silver.customers_secure ALTER COLUMN email DROP MASK;
# MAGIC ALTER TABLE lakeshore.silver.customers_secure DROP ROW FILTER;

# COMMAND ----------

n = spark.sql("SELECT count(*) AS n FROM lakeshore.silver.customers_secure").first()["n"]
masked = spark.sql("""
  SELECT count(*) AS n FROM lakeshore.silver.customers_secure
  WHERE email = '### masked ###'
""").first()["n"]
print(f"after DROP: {n} rows, {masked} masked")
assert n == 200 and masked == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The other tool: a dynamic view
# MAGIC
# MAGIC Same two rules, written in the body of a view. The base table stays as it is, and you
# MAGIC grant the **view**, never the table underneath.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW lakeshore.silver.v_customers_support AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   CASE WHEN is_account_group_member('data_admins') THEN email
# MAGIC        ELSE '### masked ###' END AS email
# MAGIC FROM lakeshore.silver.customers_secure
# MAGIC WHERE is_account_group_member('data_admins') OR customer_id <= 'C00100';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lakeshore.silver.v_customers_support ORDER BY customer_id LIMIT 5;

# COMMAND ----------

n = spark.sql("SELECT count(*) AS n FROM lakeshore.silver.v_customers_support").first()["n"]
base = spark.sql("SELECT count(*) AS n FROM lakeshore.silver.customers_secure").first()["n"]
print(f"through the view: {n} rows. Directly on the table: {base} rows, because we own it.")
assert n == 100 and base == 200

# COMMAND ----------

# MAGIC %md
# MAGIC That second number is the whole risk of a view: the rule only protects people who can
# MAGIC reach nothing but the view. A row filter has no such gap.
# MAGIC
# MAGIC ### Row logic by person, with current_user()
# MAGIC
# MAGIC A view can join, which is where it earns its place. Here each support agent sees only
# MAGIC the customers assigned to them, from a small assignments table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.support_assignments AS
# MAGIC SELECT customer_id, current_user() AS agent
# MAGIC FROM lakeshore.silver.customers_secure
# MAGIC WHERE customer_id <= 'C00010';
# MAGIC
# MAGIC CREATE OR REPLACE VIEW lakeshore.silver.v_my_customers AS
# MAGIC SELECT c.customer_id, c.first_name, c.email
# MAGIC FROM lakeshore.silver.customers_secure c
# MAGIC JOIN lakeshore.silver.support_assignments a ON c.customer_id = a.customer_id
# MAGIC WHERE a.agent = current_user();

# COMMAND ----------

mine = spark.sql("SELECT count(*) AS n FROM lakeshore.silver.v_my_customers").first()["n"]
print(f"assigned to {spark.sql('SELECT current_user()').first()[0]}: {mine} customers")
assert mine == 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant the view, not the table

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON VIEW lakeshore.silver.v_customers_support TO `account users`;
# MAGIC SHOW GRANTS ON VIEW lakeshore.silver.v_customers_support;

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE SELECT ON VIEW lakeshore.silver.v_customers_support FROM `account users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clean up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS lakeshore.silver.v_my_customers;
# MAGIC DROP VIEW IF EXISTS lakeshore.silver.v_customers_support;
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.support_assignments;
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.customers_secure;
# MAGIC DROP FUNCTION IF EXISTS lakeshore.silver.mask_email;
# MAGIC DROP FUNCTION IF EXISTS lakeshore.silver.early_customers_only;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Column mask = function + `SET MASK`, off with `DROP MASK` · row filter = boolean function
# MAGIC + `SET ROW FILTER`, off with `DROP ROW FILTER` · both hold for every access path · dynamic
# MAGIC view = the same logic in a view body, safe only if users cannot read the base table.

# COMMAND ----------

dbutils.notebook.exit("row-column-security-complete")
