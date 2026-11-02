# Databricks notebook source
# MAGIC %md
# MAGIC # 11.5 — ABAC: one policy, every table that carries the tag
# MAGIC
# MAGIC Lesson 11.4 attached a mask to **one column of one table**. That is correct and
# MAGIC it does not scale: forty tables means forty `ALTER TABLE` statements, and table
# MAGIC forty-one arrives on a Tuesday when nobody is looking.
# MAGIC
# MAGIC ABAC binds the rule to an **attribute** instead of to an object. In this notebook
# MAGIC we write **one** policy, on the schema, and then create a brand-new table and
# MAGIC watch it come out masked without touching the policy again.
# MAGIC
# MAGIC **Before you run this**, the governed tag has to exist. It is an *account-level*
# MAGIC object, so it is not created in SQL — an account admin creates it once, with the
# MAGIC CLI:
# MAGIC
# MAGIC ```
# MAGIC databricks tag-policies create-tag-policy --json '{
# MAGIC   "tag_key": "lakeshore_pii",
# MAGIC   "description": "Lakeshore governed tag: marks columns holding personal data",
# MAGIC   "values": [{"name": "email"}, {"name": "phone"}, {"name": "none"}]
# MAGIC }'
# MAGIC ```
# MAGIC
# MAGIC That account/workspace split is the first exam-relevant fact in this lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Pre-flight: the tag exists, and no old policy is left behind
# MAGIC
# MAGIC Two checks that make this notebook safe to retake. The tag is an account object, so it
# MAGIC survives between runs. And an aborted run could leave a policy on the schema, which would
# MAGIC make the deep clone below fail.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

tag = WorkspaceClient().api_client.do("GET", "/api/2.1/tag-policies/lakeshore_pii")
print("governed tag:", tag["tag_key"], [v["name"] for v in tag["values"]])

def drop_policy_if_present(name):
    """DROP POLICY has no IF EXISTS, so look first."""
    existing = [r["Policy Name"] for r in spark.sql("SHOW POLICIES ON SCHEMA lakeshore.silver").collect()]
    if name in existing:
        spark.sql(f"DROP POLICY {name} ON SCHEMA lakeshore.silver")
        print("dropped", name)

drop_policy_if_present("lakeshore_mask_pii")
drop_policy_if_present("lakeshore_filter_phone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A demo table, so the canonical silver tables stay untouched
# MAGIC
# MAGIC Deep clone from lesson 4.6, earning its keep again: a real copy of real data
# MAGIC that we can mask, break and drop without touching `silver.customers`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.abac_customers
# MAGIC   DEEP CLONE lakeshore.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, first_name, email FROM lakeshore.silver.abac_customers
# MAGIC ORDER BY customer_id LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC Emails in the clear. Now we take them away — once, for every table that says
# MAGIC it holds them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The mask function
# MAGIC
# MAGIC Exactly the shape from 11.4, and exactly the shape from lesson 6.10: a Unity
# MAGIC Catalog function, governed like any other object. ABAC does not invent a new
# MAGIC masking mechanism — it only changes how the function gets *selected*.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.mask_pii(value STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Redacts a value unless the caller is in lakeshore_pii_readers'
# MAGIC RETURN CASE
# MAGIC          WHEN is_account_group_member('lakeshore_pii_readers') THEN value
# MAGIC          ELSE '****'
# MAGIC        END;

# COMMAND ----------

# MAGIC %md
# MAGIC The group `lakeshore_pii_readers` does not exist, and that is deliberate: we are
# MAGIC not in it, so we get to see the **locked-out** experience rather than the
# MAGIC privileged one. Same trick as 11.4.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Classify: put the governed tag on the column
# MAGIC
# MAGIC This is the step a data steward does — or an automatic classifier. Note the
# MAGIC value has to be one the tag policy allows (`email`, `phone`, `none`); that
# MAGIC controlled vocabulary is what "governed" means.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakeshore.silver.abac_customers
# MAGIC   ALTER COLUMN email SET TAGS ('lakeshore_pii' = 'email');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. One policy, on the SCHEMA
# MAGIC
# MAGIC Read the clauses out loud — every one of them is exam vocabulary:
# MAGIC
# MAGIC - `ON SCHEMA` — the level it is attached at. Catalog, schema or table (metastore in Beta).
# MAGIC - `COLUMN MASK <function>` — what to do.
# MAGIC - `TO \`account users\`` — who it applies to. `EXCEPT` carves out exemptions.
# MAGIC - `MATCH COLUMNS has_tag_value(...) AS c` — **which columns**, by attribute.
# MAGIC - `ON COLUMN c` — feed that matched column into the function.
# MAGIC
# MAGIC `has_tag_value` and `has_tag` are the current snake_case names. The older
# MAGIC `hasTagValue` / `hasTag` still work for backward compatibility, and this course
# MAGIC has warned you about exactly this kind of drift since lesson 2.1.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE POLICY lakeshore_mask_pii
# MAGIC   ON SCHEMA lakeshore.silver
# MAGIC   COMMENT 'Mask any column governed-tagged lakeshore_pii=email'
# MAGIC   COLUMN MASK lakeshore.silver.mask_pii
# MAGIC   TO `account users`
# MAGIC   FOR TABLES
# MAGIC   MATCH COLUMNS has_tag_value('lakeshore_pii', 'email') AS c
# MAGIC   ON COLUMN c;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, first_name, email FROM lakeshore.silver.abac_customers
# MAGIC ORDER BY customer_id LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC Masked. Note what we did **not** do: we never named `abac_customers` in the
# MAGIC policy. The policy is attached to the schema and selects columns by attribute.

# COMMAND ----------

masked = spark.sql("""
    SELECT count(*) AS n
    FROM lakeshore.silver.abac_customers
    WHERE email = '****'
""").collect()[0]["n"]
total = spark.table("lakeshore.silver.abac_customers").count()
assert masked == total, f"expected every row masked, got {masked} of {total}"
print(f"OK — {masked} of {total} rows masked by a policy that never names this table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The payoff: table forty-one
# MAGIC
# MAGIC A brand-new table lands. Nobody edits the policy. Nobody runs `ALTER TABLE …
# MAGIC SET MASK`. The only thing that happens is that its column gets **classified**.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.abac_contacts (
# MAGIC   contact_id INT,
# MAGIC   email      STRING,
# MAGIC   phone      STRING,
# MAGIC   note       STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakeshore.silver.abac_contacts VALUES
# MAGIC   (1, 'ada@lakeshore.example',   '+39 02 5550 0101', 'newsletter'),
# MAGIC   (2, 'grace@lakeshore.example', '+1 212 555 0102',  'support'),
# MAGIC   (3, 'linus@lakeshore.example', '+39 06 5550 0103', 'returns');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Before classification: the policy does not apply, because nothing says this
# MAGIC -- column holds personal data.
# MAGIC SELECT * FROM lakeshore.silver.abac_contacts ORDER BY contact_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakeshore.silver.abac_contacts
# MAGIC   ALTER COLUMN email SET TAGS ('lakeshore_pii' = 'email');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- After classification: masked. The policy was never touched.
# MAGIC SELECT * FROM lakeshore.silver.abac_contacts ORDER BY contact_id;

# COMMAND ----------

rows = spark.sql(
    "SELECT email FROM lakeshore.silver.abac_contacts ORDER BY contact_id"
).collect()
assert all(r["email"] == "****" for r in rows), f"new table not masked: {rows}"
print("OK — a table created AFTER the policy is covered, because it carries the tag")

# COMMAND ----------

# MAGIC %md
# MAGIC That is the whole idea, and it is the sentence to carry into the exam: row
# MAGIC filters and column masks secure **specific objects**; ABAC policies secure
# MAGIC **classes of data**, centrally, by attribute.
# MAGIC
# MAGIC ## 6. The same idea for rows: a row filter policy
# MAGIC
# MAGIC The guide names both. The function returns a boolean. Non-readers keep only contacts with
# MAGIC an Italian number, and the policy finds the phone column by its tag. Two differences from
# MAGIC the mask: `ROW FILTER` instead of `COLUMN MASK`, and `USING COLUMNS` instead of `ON COLUMN`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lakeshore.silver.only_italian_phones(phone STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'Keeps +39 numbers unless the caller is in lakeshore_pii_readers'
# MAGIC RETURN is_account_group_member('lakeshore_pii_readers') OR phone LIKE '+39%';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE POLICY lakeshore_filter_phone
# MAGIC   ON SCHEMA lakeshore.silver
# MAGIC   COMMENT 'Row filter on any column governed-tagged lakeshore_pii=phone'
# MAGIC   ROW FILTER lakeshore.silver.only_italian_phones
# MAGIC   TO `account users`
# MAGIC   FOR TABLES
# MAGIC   MATCH COLUMNS has_tag_value('lakeshore_pii', 'phone') AS p
# MAGIC   USING COLUMNS (p);

# COMMAND ----------

before = spark.table("lakeshore.silver.abac_contacts").count()
spark.sql("ALTER TABLE lakeshore.silver.abac_contacts ALTER COLUMN phone SET TAGS ('lakeshore_pii' = 'phone')")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Classified: one row gone, emails still masked. Two policies, zero table names.
# MAGIC SELECT * FROM lakeshore.silver.abac_contacts ORDER BY contact_id;

# COMMAND ----------

after = spark.table("lakeshore.silver.abac_contacts").count()
print(f"rows visible: {before} before the tag, {after} after it")
assert (before, after) == (3, 2), (before, after)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Inspecting what exists

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW POLICIES ON SCHEMA lakeshore.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Clean up — the self-cleaning contract
# MAGIC
# MAGIC Every demo object this notebook made, it removes. An orphaned schema-level
# MAGIC policy is not a tidy-desk issue: it would silently mask columns for the rest
# MAGIC of the course.

# COMMAND ----------

# MAGIC %md
# MAGIC One real syntax gap, found the hard way while building this lesson:
# MAGIC `DROP POLICY` has **no `IF EXISTS`**. `DROP POLICY IF EXISTS foo ON SCHEMA …`
# MAGIC fails to parse with `missing 'ON'`, because the parser reads `IF` as the policy
# MAGIC name. So an idempotent teardown has to look before it leaps.

# COMMAND ----------

drop_policy_if_present("lakeshore_mask_pii")
drop_policy_if_present("lakeshore_filter_phone")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.abac_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lakeshore.silver.abac_contacts;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS lakeshore.silver.mask_pii;
# MAGIC DROP FUNCTION IF EXISTS lakeshore.silver.only_italian_phones;

# COMMAND ----------

left = spark.sql("SHOW POLICIES ON SCHEMA lakeshore.silver").count()
assert left == 0, f"a policy survived cleanup — {left} left on lakeshore.silver"
print("OK — no policies left on lakeshore.silver; the catalog is as we found it")

# COMMAND ----------

# MAGIC %md
# MAGIC The governed tag itself is deliberately **not** dropped here: it is an
# MAGIC account-level object created outside this notebook, and dropping it is the
# MAGIC account admin's call, with the same CLI:
# MAGIC
# MAGIC ```
# MAGIC databricks tag-policies delete-tag-policy lakeshore_pii
# MAGIC ```
