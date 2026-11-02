# Databricks notebook source
# MAGIC %md
# MAGIC # 11.3 — Grants: the path, inheritance, principals, ownership
# MAGIC
# MAGIC Every statement here also has a button: Catalog Explorer's **Permissions** tab. We
# MAGIC do both. Everything is revoked at the end, so a retake starts clean.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Who can do what right now?

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON CATALOG lakeshore;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE CATALOG EXTENDED lakeshore;

# COMMAND ----------

# MAGIC %md
# MAGIC No grant row for you. **Ownership is not a grant**: `SHOW GRANTS` lists grants, and the
# MAGIC owner shows up in `DESCRIBE`.
# MAGIC
# MAGIC ## 2. The path down the hierarchy
# MAGIC
# MAGIC `account users` is the built-in group holding every user in the account. It stands in
# MAGIC for "the analysts". Reading one table takes all three privileges.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG lakeshore TO `account users`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  lakeshore.gold TO `account users`;
# MAGIC GRANT SELECT      ON TABLE   lakeshore.gold.customer_order_summary TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE lakeshore.gold.customer_order_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inheritance, on a table that does not exist yet
# MAGIC
# MAGIC Grant on the schema, then create a table. It arrives already readable.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON SCHEMA lakeshore.gold TO `account users`;
# MAGIC CREATE OR REPLACE TABLE lakeshore.gold.grants_demo (id INT) COMMENT 'Throwaway table for lesson 11.3';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE lakeshore.gold.grants_demo;

# COMMAND ----------

inherited = [r["ObjectType"] for r in spark.sql("SHOW GRANTS ON TABLE lakeshore.gold.grants_demo").collect()
             if r["Principal"] == "account users" and r["ActionType"] == "SELECT"]
print("SELECT on the new table comes from:", inherited)
assert inherited == ["SCHEMA"], inherited

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Same thing with the buttons (not run by this notebook)
# MAGIC
# MAGIC **Catalog** → `lakeshore` → `gold` → `grants_demo` → **Permissions** tab. The inherited
# MAGIC row is there too. Then **Grant**, pick the principal, tick `MODIFY`, **OK**. Run the
# MAGIC `SHOW GRANTS` cell above to see it. Select the row and **Revoke**.
# MAGIC
# MAGIC ## 5. A service principal is granted by its application ID
# MAGIC
# MAGIC A machine identity, like the one that deploys our bundle in Section 9. We create a real
# MAGIC one, grant to it, and delete it. The name you see in the UI is not what `GRANT` takes.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
for old in w.service_principals.list(filter='displayName eq "lakeshore-demo-sp"'):
    w.service_principals.delete(old.id)                 # left over from an aborted run

sp = w.service_principals.create(display_name="lakeshore-demo-sp")
app_id = sp.application_id
print("applicationId:", app_id)

# COMMAND ----------

spark.sql(f"GRANT SELECT ON TABLE lakeshore.gold.grants_demo TO `{app_id}`")
display(spark.sql("SHOW GRANTS ON TABLE lakeshore.gold.grants_demo"))

# COMMAND ----------

spark.sql(f"REVOKE SELECT ON TABLE lakeshore.gold.grants_demo FROM `{app_id}`")
w.service_principals.delete(sp.id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ownership moves with OWNER TO
# MAGIC
# MAGIC Owners should be groups, because people leave. We hand the table to a group, read the
# MAGIC owner back, and take it home again.

# COMMAND ----------

me = spark.sql("SELECT current_user()").first()[0]

def owner_of(table):
    """The Owner row of DESCRIBE EXTENDED."""
    rows = spark.sql(f"DESCRIBE EXTENDED {table}").collect()
    return next(r["data_type"] for r in rows if r["col_name"] == "Owner")

spark.sql("ALTER TABLE lakeshore.gold.grants_demo OWNER TO `account users`")
print("owner now:", owner_of("lakeshore.gold.grants_demo"))
assert owner_of("lakeshore.gold.grants_demo") == "account users"

spark.sql(f"ALTER TABLE lakeshore.gold.grants_demo OWNER TO `{me}`")
print("owner back to:", owner_of("lakeshore.gold.grants_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. And DENY?
# MAGIC
# MAGIC The exam guide says "GRANT, REVOKE, and DENY". Here is the SQL statement on a Unity
# MAGIC Catalog table.

# COMMAND ----------

try:
    spark.sql("DENY SELECT ON TABLE lakeshore.gold.grants_demo TO `account users`")
    raise AssertionError("DENY should have been refused")
except Exception as e:
    print(e.getErrorClass())
    assert "UC_COMMAND_NOT_SUPPORTED" in str(e.getErrorClass())

# COMMAND ----------

# MAGIC %md
# MAGIC Refused. The SQL `DENY` statement applies only to the legacy `hive_metastore` catalog.
# MAGIC ABAC **DENY policies** do exist, in Beta, and they cover one privilege only:
# MAGIC `MANAGE ACCESS CONTROL`. There is still no way to deny `SELECT` or `MODIFY` in Unity
# MAGIC Catalog.
# MAGIC
# MAGIC ## 8. Clean up: revoke everything we granted

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE SELECT      ON TABLE   lakeshore.gold.customer_order_summary FROM `account users`;
# MAGIC REVOKE SELECT      ON SCHEMA  lakeshore.gold FROM `account users`;
# MAGIC REVOKE USE SCHEMA  ON SCHEMA  lakeshore.gold FROM `account users`;
# MAGIC REVOKE USE CATALOG ON CATALOG lakeshore FROM `account users`;
# MAGIC DROP TABLE IF EXISTS lakeshore.gold.grants_demo;

# COMMAND ----------

for grants_on in ["CATALOG lakeshore", "SCHEMA lakeshore.gold", "TABLE lakeshore.gold.customer_order_summary"]:
    left = [r for r in spark.sql(f"SHOW GRANTS ON {grants_on}").collect() if r["Principal"] == "account users"]
    assert not left, f"{grants_on} still grants to account users: {left}"
print("OK: account users holds nothing we gave it")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC Reading a table = `USE CATALOG` + `USE SCHEMA` + `SELECT` · a schema grant covers
# MAGIC current and future tables · Catalog Explorer → Permissions does the same as SQL ·
# MAGIC service principals are granted by `applicationId` · `ALTER … OWNER TO` moves ownership ·
# MAGIC no SQL `DENY` on Unity Catalog securables.

# COMMAND ----------

dbutils.notebook.exit("grants-complete")
