---
title: "Reference card: Delta DDL and DML"
section: S4 — Delta Lake
duration: ~7 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D3.1, D3.2
verified: docs.databricks.com, 2026-08-29
---

> **📄 Reference card.** No video, nothing to run. Every clause below was checked against
> the docs on the date in the header.

Lessons 4.2, 4.5 and 4.6 wrote to tables, merged into them and cloned them. This card is
the full syntax of everything you touched, plus the clauses the exam adds.

---

## Creating tables

```sql
CREATE TABLE            catalog.schema.name (col type, …)     -- fails if it exists
CREATE TABLE IF NOT EXISTS catalog.schema.name (…)            -- ignored if it exists
CREATE OR REPLACE TABLE catalog.schema.name (…)               -- replaces, keeps history + grants
CREATE TABLE catalog.schema.name AS SELECT …                  -- CTAS: schema comes from the query
CREATE TABLE catalog.schema.name LIKE other_table             -- schema only, no data
```

| Fact | Value |
|---|---|
| Default provider when `USING` is omitted | **`DELTA`** |
| `CREATE OR REPLACE` | Delta and Iceberg only. **Preserves history and granted privileges** |
| `IF NOT EXISTS` + `OR REPLACE` | Cannot be combined |
| Managed vs external | **The `LOCATION` clause is the whole difference** |

**Managed** — no `LOCATION`. Databricks owns the files; `DROP TABLE` deletes them.
**External** — with `LOCATION`. `DROP TABLE` removes the metadata and **leaves the files**.

**Trap:** the exam phrases this as a data-loss question. Dropping an external table does
not delete data. Dropping a managed one does. Section 11 teaches external tables properly.

---

## Constraints — enforced vs informational

This split is tested constantly.

| Constraint | Enforced? | Syntax |
|---|---|---|
| `NOT NULL` | **Yes** | `ALTER TABLE t ALTER COLUMN c SET NOT NULL` |
| `CHECK` | **Yes** | `ALTER TABLE t ADD CONSTRAINT name CHECK (expr)` |
| `PRIMARY KEY` | **No — informational** | `ALTER TABLE t ADD CONSTRAINT name PRIMARY KEY(cols)` |
| `FOREIGN KEY` | **No — informational** | `ALTER TABLE t ADD CONSTRAINT name FOREIGN KEY(cols) REFERENCES parent` |
| `UNIQUE` | **No — informational** | `ALTER TABLE t ADD CONSTRAINT name UNIQUE(col)` |

`NOT NULL` and `CHECK` **fail the transaction** on violation — that is what made
`positive_price` on `bronze.products` real in lesson 4.2. Primary and foreign keys are
**declarations for humans and query optimisers**, available on Unity Catalog Delta tables;
nothing stops you inserting a duplicate key. You will see the consequence in lesson 6.8,
which declares the model *and* keeps a quality battery: the constraints document intent,
the anti-joins enforce it.

Dropping any of them: `ALTER TABLE t DROP CONSTRAINT name`
(except `NOT NULL`, which uses `ALTER COLUMN c DROP NOT NULL`).

---

## Writing data

```sql
INSERT INTO t  ...                    -- append
INSERT OVERWRITE t ...                -- replace the whole table
INSERT INTO t REPLACE WHERE pred ...  -- replace only the rows matching pred
```

**Trap from our own validation** (you will meet JSON inference in lesson 5.3):
`INSERT INTO t VALUES (…)` is **positional**. When a table's columns were inferred from
JSON, they are in **alphabetical** order, not the order
you expect — so positional inserts land values in the wrong columns without error. Name
your columns: `INSERT INTO t (id, name) VALUES (…)`.

---

## MERGE INTO — the full shape

```sql
[ WITH cte ] MERGE [ WITH SCHEMA EVOLUTION ] INTO target [alias]
  USING source [alias]
  ON merge_condition
  WHEN MATCHED [ AND cond ] THEN { UPDATE SET * | UPDATE SET c = expr | DELETE }
  WHEN NOT MATCHED [BY TARGET] [ AND cond ] THEN { INSERT * | INSERT (cols) VALUES (…) }
  WHEN NOT MATCHED BY SOURCE [ AND cond ] THEN { UPDATE SET c = expr | DELETE }
```

| Clause | Fires when | Allowed actions |
|---|---|---|
| `WHEN MATCHED` | The row is in both | `UPDATE SET *`, `UPDATE SET c = …`, `DELETE` |
| `WHEN NOT MATCHED [BY TARGET]` | In the source, not in the target | `INSERT *`, `INSERT (cols) VALUES (…)` |
| `WHEN NOT MATCHED BY SOURCE` | In the target, not in the source | `UPDATE SET c = …`, `DELETE` — **no INSERT** |

Rules that turn into exam questions:

- You may have **multiple clauses of each type**; all but the last must carry an `AND`.
- **`WHEN NOT MATCHED BY SOURCE` cannot INSERT.** There is no source row to insert.
- If **more than one source row matches the same target row**, the merge **fails** with
  `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`. Deduplicate the source first —
  `QUALIFY`, introduced in lesson 6.1, is the tool for it.
- `WITH SCHEMA EVOLUTION` lets the target gain columns from the source.

**Decision fork:** `MERGE` for upserts and deletes keyed on a match ·
`INSERT OVERWRITE` to replace a whole table · `REPLACE WHERE` to replace a partition-shaped
slice. Reaching for `MERGE` when you mean "replace yesterday" is the classic over-engineer.

---

## Cloning

```sql
CREATE TABLE t CLONE      src   -- DEEP is the default
CREATE TABLE t DEEP CLONE src
CREATE TABLE t SHALLOW CLONE src
CREATE TABLE t SHALLOW CLONE src VERSION AS OF 12
CREATE TABLE t SHALLOW CLONE src TIMESTAMP AS OF '2026-08-01'
```

| | Deep clone | Shallow clone |
|---|---|---|
| Copies data files | **Yes** | **No** — metadata only |
| Independent of source | Fully | **Depends on the source's files** |
| Cost to create | Proportional to data | Near-instant |
| Risk | None | `VACUUM` on the **source** breaks it |

Both copy schema, partitioning, invariants, nullability and properties, and **writes to
either clone never affect the source**.

**Trap:** the shallow-clone failure mode is a `FileNotFoundException` after someone
vacuums the source. Use shallow clones for short-lived experiments — which is exactly how
lesson 4.6 used them — and deep clones when the copy must outlive the source's maintenance.

---

## Exam traps

- **`DROP TABLE` on an external table does not delete the data.** On a managed one it does.
- **PK/FK/UNIQUE are informational.** Only `NOT NULL` and `CHECK` are enforced.
- **`WHEN NOT MATCHED BY SOURCE` cannot insert.**
- **Duplicate source rows fail a MERGE** — dedupe first.
- **Positional `INSERT` + JSON-inferred columns = silent misalignment.** Name the columns.
- **A shallow clone dies when the source is vacuumed.**

## Resources

- MERGE INTO: https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into
- CREATE TABLE: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using
- Constraints: https://docs.databricks.com/aws/en/tables/constraints
- Clone: https://docs.databricks.com/aws/en/delta/clone
