# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 6.8 — Documenting the model: comments, primary keys, foreign keys
# MAGIC
# MAGIC We have a silver layer and a gold layer. Every table works. Not one of
# MAGIC them says what it *is*, and nothing records that `order_items.product_id`
# MAGIC points at `products.product_id` — the relationship lives in our heads and
# MAGIC in the JOINs we happen to write.
# MAGIC
# MAGIC This lesson fixes both. Metadata is engineering work, not paperwork:
# MAGIC Catalog Explorer, workspace search and (Section 10) Genie all read it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Comments — the searchable sentence
# MAGIC
# MAGIC A good table comment answers three questions in one breath: **what is one
# MAGIC row** (the grain), **where does it come from**, **who owns it**. Write it
# MAGIC once and every reader — human or AI — gets the same answer.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE lakeshore.silver.customers IS
# MAGIC   'One row per customer, deduplicated on latest created_at. Source: bronze.customers. Owner: Lakeshore data platform.';
# MAGIC
# MAGIC COMMENT ON TABLE lakeshore.silver.products IS
# MAGIC   'Product catalog, one row per product. Reference data, source: bronze.products. Owner: Lakeshore data platform.';
# MAGIC
# MAGIC COMMENT ON TABLE lakeshore.silver.orders IS
# MAGIC   'One row per order header. Source: bronze.orders (Auto Loader). Owner: Lakeshore data platform.';
# MAGIC
# MAGIC COMMENT ON TABLE lakeshore.silver.order_items IS
# MAGIC   'One row per order LINE — exploded from the nested items array in bronze.orders. Owner: Lakeshore data platform.';

# COMMAND ----------

# MAGIC %md
# MAGIC Column comments are where the real ambiguity lives. `line_total` — total of
# MAGIC what, before or after refunds, in which currency? One sentence ends the
# MAGIC Slack thread that would otherwise happen every quarter.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakeshore.silver.order_items
# MAGIC   ALTER COLUMN line_total COMMENT 'quantity * unit_price at order time, before refunds (EUR)';
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.orders
# MAGIC   ALTER COLUMN n_items COMMENT 'Number of distinct order lines, not total units';
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.customers
# MAGIC   ALTER COLUMN email COMMENT 'Nullable by design: 10 of 200 customers (5%) have no email';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED lakeshore.silver.order_items;

# COMMAND ----------

# MAGIC %md
# MAGIC Catalog Explorer can also **draft comments for you with AI** (Genie Code,
# MAGIC formerly Databricks Assistant). Treat the draft as a first pass: accept, *edit*, then commit.
# MAGIC A confidently wrong description is worse than no description at all —
# MAGIC people stop reading them, and then nobody writes them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Two families of constraints
# MAGIC
# MAGIC Databricks has both, and the difference is the whole point:
# MAGIC
# MAGIC | Family | Constraints | Behaviour |
# MAGIC |---|---|---|
# MAGIC | **Enforced** | `NOT NULL`, `CHECK` | Bad rows are **rejected** at write time |
# MAGIC | **Informational** | `PRIMARY KEY`, `FOREIGN KEY`, `UNIQUE` | Declared, **never checked** |
# MAGIC
# MAGIC We met the enforced pair in lesson 4.2 (`positive_price`). Informational
# MAGIC keys are new — they document the model, drive the Catalog Explorer
# MAGIC relationship diagram, and let the optimizer reason about the data. They
# MAGIC do **not** protect it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Declaring keys
# MAGIC
# MAGIC One requirement first: **primary key columns must be `NOT NULL`**. That
# MAGIC part *is* enforced, and Delta validates the existing rows before accepting
# MAGIC it — if any row has a NULL id, this statement fails, which is exactly the
# MAGIC behaviour you want.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakeshore.silver.customers ALTER COLUMN customer_id SET NOT NULL;
# MAGIC ALTER TABLE lakeshore.silver.products  ALTER COLUMN product_id  SET NOT NULL;
# MAGIC ALTER TABLE lakeshore.silver.orders    ALTER COLUMN order_id    SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP … IF EXISTS first, so this notebook is safe to re-run: ADD CONSTRAINT
# MAGIC -- on a name that already exists is an error, not a no-op. And CASCADE, because
# MAGIC -- a primary key with foreign keys pointing at it refuses to be dropped alone —
# MAGIC -- Unity Catalog tracks the dependency. (We re-create the children below.)
# MAGIC ALTER TABLE lakeshore.silver.customers DROP CONSTRAINT IF EXISTS customers_pk CASCADE;
# MAGIC ALTER TABLE lakeshore.silver.products  DROP CONSTRAINT IF EXISTS products_pk  CASCADE;
# MAGIC ALTER TABLE lakeshore.silver.orders    DROP CONSTRAINT IF EXISTS orders_pk    CASCADE;
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.customers ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id);
# MAGIC ALTER TABLE lakeshore.silver.products  ADD CONSTRAINT products_pk  PRIMARY KEY (product_id);
# MAGIC ALTER TABLE lakeshore.silver.orders    ADD CONSTRAINT orders_pk    PRIMARY KEY (order_id);

# COMMAND ----------

# MAGIC %md
# MAGIC Now the relationships. A foreign key must point at a **primary key or a
# MAGIC unique constraint** — you cannot reference an arbitrary column. That's why
# MAGIC the three PKs came first.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakeshore.silver.orders      DROP CONSTRAINT IF EXISTS orders_customer_fk;
# MAGIC ALTER TABLE lakeshore.silver.order_items DROP CONSTRAINT IF EXISTS items_order_fk;
# MAGIC ALTER TABLE lakeshore.silver.order_items DROP CONSTRAINT IF EXISTS items_product_fk;
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.orders
# MAGIC   ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id)
# MAGIC   REFERENCES lakeshore.silver.customers;
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.order_items
# MAGIC   ADD CONSTRAINT items_order_fk FOREIGN KEY (order_id)
# MAGIC   REFERENCES lakeshore.silver.orders;
# MAGIC
# MAGIC ALTER TABLE lakeshore.silver.order_items
# MAGIC   ADD CONSTRAINT items_product_fk FOREIGN KEY (product_id)
# MAGIC   REFERENCES lakeshore.silver.products;

# COMMAND ----------

# MAGIC %md
# MAGIC That is the Lakeshore model, now written down where tools can read it:
# MAGIC
# MAGIC ```
# MAGIC customers ──< orders ──< order_items >── products
# MAGIC ```
# MAGIC
# MAGIC In Catalog Explorer, open `silver.order_items` → Overview → **View
# MAGIC relationships**: the entity-relationship diagram is generated from exactly
# MAGIC these constraints. Nothing else. No constraints, no diagram.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Constraints show up in the extended description, and in information_schema
# MAGIC SELECT constraint_name, constraint_type
# MAGIC FROM lakeshore.information_schema.table_constraints
# MAGIC WHERE table_schema = 'silver'
# MAGIC ORDER BY constraint_type, constraint_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Proving they are NOT enforced
# MAGIC
# MAGIC This is the part to remember. A primary key does not stop duplicates and
# MAGIC a foreign key does not stop orphans. Watch — on a throwaway table, because
# MAGIC we are not putting garbage in silver to make a point.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeshore.silver.pk_demo (
# MAGIC   id   STRING NOT NULL,
# MAGIC   note STRING,
# MAGIC   CONSTRAINT pk_demo_pk PRIMARY KEY (id)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO lakeshore.silver.pk_demo VALUES ('A', 'first'), ('A', 'duplicate key');

# COMMAND ----------

n = spark.table("lakeshore.silver.pk_demo").count()
assert n == 2, f"expected the duplicate to be accepted, got {n} rows"
print(f"✓ {n} rows with the same PRIMARY KEY — accepted without a murmur.")
print("  Informational means informational. Uniqueness is YOUR job:")
print("  the QUALIFY/ROW_NUMBER dedup from 6.1 and the anti-join battery from 6.6.")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE lakeshore.silver.pk_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Document gold too
# MAGIC
# MAGIC Gold is the layer non-engineers actually open. It deserves the *best*
# MAGIC comments in the platform, not the leftovers.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE lakeshore.gold.customer_order_summary IS
# MAGIC   'One row per customer with lifetime order count and spend. Rebuilt from silver.orders + silver.order_items. Owner: Lakeshore data platform.';
# MAGIC
# MAGIC ALTER TABLE lakeshore.gold.customer_order_summary
# MAGIC   ALTER COLUMN total_spent COMMENT 'Lifetime gross spend, before refunds (EUR)';
# MAGIC
# MAGIC ALTER TABLE lakeshore.gold.customer_order_summary
# MAGIC   DROP CONSTRAINT IF EXISTS summary_customer_fk;
# MAGIC
# MAGIC ALTER TABLE lakeshore.gold.customer_order_summary
# MAGIC   ADD CONSTRAINT summary_customer_fk FOREIGN KEY (customer_id)
# MAGIC   REFERENCES lakeshore.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Views take a comment too — same discipline, different DDL
# MAGIC COMMENT ON VIEW lakeshore.gold.vw_revenue_by_category IS
# MAGIC   'Revenue and units per product category, computed live from silver at query time.';

# COMMAND ----------

commented = spark.sql("""
    SELECT count(*) AS n
    FROM lakeshore.information_schema.tables
    WHERE table_schema IN ('silver', 'gold') AND comment IS NOT NULL
""").first()["n"]
print(f"✓ {commented} silver/gold objects now carry a description")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC
# MAGIC `COMMENT ON TABLE` / `ALTER COLUMN … COMMENT` — grain, source, owner ·
# MAGIC enforced constraints (`NOT NULL`, `CHECK`) reject bad rows; informational
# MAGIC ones (`PRIMARY KEY`, `FOREIGN KEY`, `UNIQUE`) only *describe* · PK columns
# MAGIC must be NOT NULL · an FK must reference a PK or UNIQUE · the Catalog
# MAGIC Explorer relationship diagram is generated from these constraints · none of
# MAGIC it validates your data — that stays your pipeline's job.

# COMMAND ----------

dbutils.notebook.exit("document-and-model-complete")
