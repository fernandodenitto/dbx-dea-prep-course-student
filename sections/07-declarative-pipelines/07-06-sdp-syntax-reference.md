---
title: "Reference card: Lakeflow pipelines syntax and the DLT name map"
section: S7 — Lakeflow Spark Declarative Pipelines
duration: ~7 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D3.6, D3.7
verified: docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every clause below was checked against
> the docs on the date in the header, and the SQL is copied from the pipeline files you ran
> in lessons 7.2 to 7.4.

---

## Start with the names, because the exam hasn't caught up

**Lakeflow pipelines** (Spark Declarative Pipelines; the exam guide says Lakeflow Declarative
Pipelines; formerly Delta Live Tables). SDP is this course's shorthand for them.

| Name | Where you'll see it |
|---|---|
| **Lakeflow pipelines** | Current docs, under the title "Spark Declarative Pipelines on Lakeflow" |
| **Apache Spark Declarative Pipelines** | The open-source framework Lakeflow pipelines are built on |
| **Lakeflow Spark Declarative Pipelines** | The CLI's help text, older docs |
| **Lakeflow Declarative Pipelines** | The August 2026 exam guide. The docs path `/ldp/` comes from this name |
| **Delta Live Tables (DLT)** | The original name, older courses and blog posts. Old `/dlt/` links now redirect to `/ldp/` |
| Streaming table, materialized view | *(unchanged)* |

When you see any of these, read the same product. The legacy syntax is what actually
changed: `CREATE LIVE TABLE`, the `LIVE.` prefix, `import dlt` and `APPLY CHANGES` are all
older forms.

---

## The three dataset types

| Type | Persisted? | Use for |
|---|---|---|
| **Streaming table** | Yes, in the catalog | Ingestion and incremental processing of append-only, continuously growing data. Each record processed **exactly once** |
| **Materialized view** | Yes, in the catalog | Transformations and aggregations, recomputed to reflect current state. Correct when the source **changes** — updates and deletes |
| **Temporary view** | **No** | Intermediate logic, pipeline-private, never published |

**The decision fork the exam loves:** append-only source, incremental processing →
**streaming table**. Source rows get updated or deleted, or you need an aggregate that
stays correct → **materialized view**.

---

## Syntax

```sql
CREATE OR REFRESH STREAMING TABLE lakeshore.bronze.sdp_orders_raw
COMMENT 'Raw orders, ingested declaratively'
AS SELECT *, _metadata.file_path AS source_file
FROM STREAM read_files('/Volumes/lakeshore/landing/files/orders', format => 'json');
```

```sql
CREATE TEMPORARY VIEW vw_order_items AS
SELECT
  order_id,
  order_ts,
  item.product_id,
  item.quantity,
  item.unit_price,
  item.quantity * item.unit_price AS line_total
FROM (
  SELECT order_id, order_ts, explode(items) AS item
  FROM lakeshore.silver.sdp_orders
);
```

```sql
CREATE OR REFRESH MATERIALIZED VIEW lakeshore.gold.sdp_orders_by_day
COMMENT 'Daily order revenue, declaratively maintained (SDP demo flow)'
AS SELECT
  date(order_ts)             AS order_date,
  count(DISTINCT order_id)   AS n_orders,
  round(sum(line_total), 2)  AS revenue
FROM vw_order_items
GROUP BY date(order_ts);
```

**Reading another dataset as a stream** — the `STREAM()` wrapper is what makes the read
incremental:

```sql
FROM STREAM(lakeshore.bronze.sdp_orders_raw)   -- incremental
FROM        lakeshore.bronze.sdp_orders_raw    -- full re-read, every update
```

**And a real gotcha from building this pipeline:** `read_files` **infers typed columns by
default** — the nested `items` array arrives as `ARRAY<STRUCT<…>>`, ready to use. The same
data read through raw `cloudFiles` in lesson 5.5 arrived as a **JSON string** and needed
`from_json`. Same files, opposite defaults: the docs say so in as many words. The setting
behind it is `inferColumnTypes`, and this is a difference between the two readers, not
something the pipeline does.

---

## Expectations

```sql
CREATE OR REFRESH STREAMING TABLE lakeshore.silver.sdp_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT sane_timestamp EXPECT (order_ts <= current_timestamp())
)
AS SELECT … FROM STREAM(lakeshore.bronze.sdp_orders_raw);
```

| Clause | Bad rows | The update |
|---|---|---|
| `EXPECT (…)` — **no `ON VIOLATION`** | **Written to the target anyway** | Continues. Violations are counted and reported |
| `EXPECT (…) ON VIOLATION DROP ROW` | **Dropped** before the write | Continues |
| `EXPECT (…) ON VIOLATION FAIL UPDATE` | — | **Update fails.** Manual intervention required before reprocessing. No metrics are recorded |

**Trap:** the default is *warn*, and warn **keeps the bad rows**. An answer claiming a bare
`EXPECT` filters data is wrong — it only counts and reports. That metric is the point: you
see quality degrade without losing data. Find it on the dataset's **Data quality** tab.

### Python equivalents

```python
@dp.expect("name", "clause")            # warn
@dp.expect_or_drop("name", "clause")    # drop row
@dp.expect_or_fail("name", "clause")    # fail update
```

Plural forms take a dict of constraints: `@dp.expect_all`, `@dp.expect_all_or_drop`,
`@dp.expect_all_or_fail`.

---

## Pipeline modes — two independent switches

People conflate these constantly. They are unrelated.

**Update mode — how the pipeline runs:**

| | Triggered | Continuous |
|---|---|---|
| Behaviour | Refreshes what's available, then **stops** | Keeps tables fresh as data arrives |
| Cost | Pay per update | Compute stays up |

**Run behavior — how the update was started.** The docs now describe this by trigger; older
material and exam questions call the two sides development mode and production mode.

| | Fast-start (development) | Automatic retry (production) |
|---|---|---|
| Started by | **Run** in the UI, ad-hoc updates | Jobs, API, every continuous pipeline |
| Compute | Reused between updates, for fast iteration | Shuts down as soon as the run ends |
| Failures | Pipeline retries are off, so you see the error | Retries on certain errors |

**Trap:** "development mode" does not mean "triggered", and "production" does not mean
"continuous". A triggered pipeline run by a job is the normal daily-batch shape.

---

## Exam traps

- **DLT, Lakeflow Declarative Pipelines, SDP, Lakeflow pipelines.** One product.
- **A bare `EXPECT` keeps the bad rows.** Only `DROP ROW` removes them; only `FAIL UPDATE`
  stops the pipeline.
- **`STREAM()` is what makes a read incremental.** Without it the source is re-read in full.
- **Streaming table for append-only; materialized view when the source mutates.**
- **Temporary views are not published** to the catalog.
- **Update mode and deployment mode are independent switches.**

## Resources

- Spark Declarative Pipelines on Lakeflow: https://docs.databricks.com/aws/en/ldp/
- Pipeline concepts: https://docs.databricks.com/aws/en/ldp/concepts
- Expectations: https://docs.databricks.com/aws/en/ldp/expectations
- Updates and run behaviors: https://docs.databricks.com/aws/en/ldp/updates
- read_files: https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files
- Pipeline observability: https://docs.databricks.com/aws/en/ldp/observability
