---
title: "Reference card: COPY INTO"
section: S5 — Data Ingestion & Loading
duration: ~5 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D2.2
verified: docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every clause below was checked against
> the docs on the date in the header.

You used `COPY INTO` in lesson 5.3 to land customers, addresses, payments and refunds.
This is the whole command — including the clauses that lesson didn't need and the exam
does.

---

## Full syntax

```sql
COPY INTO target_table [ BY POSITION | ( col_name [, ...] ) ]
  FROM { source_clause | ( SELECT expression_list FROM source_clause ) }
  FILEFORMAT = data_source
  [ VALIDATE [ ALL | num_rows ROWS ] ]
  [ FILES = ( 'file_name' [, ...] ) | PATTERN = 'glob_pattern' ]
  [ FORMAT_OPTIONS ( 'option' = 'value' [, ...] ) ]
  [ COPY_OPTIONS  ( 'option' = 'value' [, ...] ) ]

source_clause:
  source [ WITH ( [ CREDENTIAL { credential_name | (temporary_credential_options) } ]
                  [ ENCRYPTION (encryption_options) ] ) ]
```

**Supported formats:** `CSV`, `JSON`, `AVRO`, `ORC`, `PARQUET`, `TEXT`, `BINARYFILE`.

---

## The clauses

| Clause | What it does |
|---|---|
| `source_clause` | The path, plus an optional credential or encryption. `s3://`, `abfss://`, `gs://` or `/Volumes/…` |
| `BY POSITION` / column list | How source columns map to target columns |
| `FROM ( SELECT … FROM source )` | Transform on the way in — cast, rename, add a literal |
| `VALIDATE` | Preview and validate without loading. `ALL` or `n ROWS` |
| `FILES` | An explicit list of file names — **max 1000**, mutually exclusive with `PATTERN` |
| `PATTERN` | A glob over the source directory |
| `FORMAT_OPTIONS` | Passed straight to the Spark reader for that format (`header`, `delimiter`, `inferSchema`, …) |
| `COPY_OPTIONS` | Options of the `COPY INTO` command itself — the table below |

**Trap:** `FORMAT_OPTIONS` and `COPY_OPTIONS` are not interchangeable, and the exam swaps
them. `mergeSchema` in `FORMAT_OPTIONS` is the reader's; `mergeSchema` in `COPY_OPTIONS`
is the command's. Reader options describe *the files*; copy options describe *the load*.

---

## Access: who may read the source (📖 theory on Free Edition)

| Source | Privilege | Status in the docs |
|---|---|---|
| Volume, `/Volumes/…` | `READ VOLUME` | **Recommended** |
| External location, `s3://…`, `abfss://…`, `gs://…` | `READ FILES` on the external location | Supported, not the recommended approach |
| `WITH (CREDENTIAL credential_name)` | A named storage credential; usable only if the path isn't in an external location | Niche |
| `WITH (CREDENTIAL (AWS_ACCESS_KEY = …, AWS_SECRET_KEY = …, AWS_SESSION_TOKEN = …))`, or `AZURE_SAS_TOKEN` | Temporary credentials | Niche |

You also need `USE CATALOG` and `USE SCHEMA` on the target's parents. Privileges on an
external location cover nested directories, not parents or siblings. Encryption, on S3:
`ENCRYPTION (TYPE = 'AWS_SSE_C', MASTER_KEY = '…')`.

---

## COPY_OPTIONS

| Option | Default | What it does |
|---|---|---|
| `force` | `false` | `true` **disables idempotency** — files load again even if already loaded |
| `mergeSchema` | `false` | `true` lets the target schema evolve to fit incoming data |

---

## Idempotency — the whole point

> Files already loaded are **skipped**, and they stay skipped **even if the file has been
> modified since**.

That is the guarantee: re-running `COPY INTO` is safe and loads nothing new. It is why
lesson 5.3 could run the same command twice and the row count didn't move.

Two consequences the exam tests:

1. **Fixing a bad file by editing it in place does nothing.** The file is on the loaded
   list. You need `force = true`, or a new path.
2. **`force = true` reloads everything the statement matches**, so it duplicates rows
   unless you also truncate or restrict the file set. It is a repair tool, not a switch to
   leave on.

---

## COPY INTO vs Auto Loader — the decision fork

| | COPY INTO | Auto Loader |
|---|---|---|
| Shape | SQL command, batch | Streaming source (`cloudFiles`) |
| Scale | Files in the order of thousands | Millions or more |
| State | Loaded-file list on the table | Checkpoint + schema location |
| Schema evolution | `mergeSchema` copy option | Five evolution modes |
| Best when | A known, bounded set of files; SQL-only teams; reloading a subset of re-uploaded files | Continuous arrival, unbounded growth |

Both are **incremental and exactly-once**. Choosing between them is about scale and shape,
never about correctness — an answer claiming one of them can duplicate data on retry is
wrong.

COPY INTO's status, stated plainly: it isn't deprecated, and the guide names it. But the
docs call a streaming table, `CREATE STREAMING TABLE … FROM STREAM read_files(…)`, "the
recommended alternative to `COPY INTO`" for scalable SQL ingestion, and Section 7 builds one.
And one thing COPY INTO does better: "Loading a subset of re-uploaded files can be a bit
easier to manage with `COPY INTO`." You can even run it for that while an Auto Loader
stream runs on the same data.

---

## Exam traps

- **`FORMAT_OPTIONS` vs `COPY_OPTIONS`** — reader options vs command options.
- **Editing an already-loaded file changes nothing.** Idempotency is by file identity, not
  by content.
- **`force` is not "reload the new stuff"** — it reloads everything matched.
- **`FILES` caps at 1000** and cannot be combined with `PATTERN`.
- **`VALIDATE` loads nothing.** If a question asks how to check a load before committing
  to it, this is the answer — not a `LIMIT`.
- **`s3://` path denied?** `READ FILES` on the external location. `/Volumes` path? `READ VOLUME`.

## Resources

- COPY INTO: https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into
- COPY INTO with Unity Catalog: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/unity-catalog
- Choosing between COPY INTO and Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/
- Auto Loader options: https://docs.databricks.com/aws/en/spark/api-options
