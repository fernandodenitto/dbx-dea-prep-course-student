---
title: "Reference card: Auto Loader options"
section: S5 — Data Ingestion & Loading
duration: ~6 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D2.3
verified: docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. This is the page to come back to the
> night before the exam. Every option below was checked against the docs on the date in
> the header — if Databricks changes a default, the doc link at the bottom wins over this
> page.

You configured Auto Loader in lesson 5.5 and evolved its schema in 5.6. Those lessons
used the handful of options the job needed. The exam asks about the rest — usually as
*"which option do you set to…"*. Here are the ones to know.

---

## The three options you always set

```python
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path)
  .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("lakeshore.bronze.orders"))
```

| Option | Default | What it does |
|---|---|---|
| `cloudFiles.format` | *(required)* | Source file format: `avro`, `binaryFile`, `csv`, `json`, `orc`, `parquet`, `text`, `xml` |
| `cloudFiles.schemaLocation` | none | Where the inferred schema and its later changes are stored. Required for inference |
| `checkpointLocation` | *(required)* | Streaming bookkeeping — which files are already ingested. **Not** an Auto Loader option: it belongs to the writer |

**Trap:** the schema location and the checkpoint are two different things. The exam likes
answers that conflate them. Checkpoint = what has been read. Schema location = what the
data looks like.

---

## Schema inference and evolution

| Option | Default | What it does |
|---|---|---|
| `cloudFiles.schemaHints` | none | Override inferred types, or declare a column before it appears. **Used only when you do not supply a schema** |
| `cloudFiles.inferColumnTypes` | `false` | Infer exact types instead of reading everything as string |
| `cloudFiles.schemaEvolutionMode` | see below | What happens when a new column shows up |
| `rescuedDataColumn` | none | Name of the column that collects everything that didn't fit. Conventionally `_rescued_data` |

`inferColumnTypes` defaulting to `false` is why `items` landed in lesson 5.5 as a **JSON
string**, not an array: for JSON, CSV and XML, inference reads columns as strings, nested
fields included, unless you ask otherwise. Lesson 6.4 will parse it with `from_json`.
(Beyond the guide's wording: to keep a whole record semi-structured, the docs offer
`singleVariantColumn`, or a `VARIANT` type in `schemaHints`.)

### The five evolution modes

| Mode | New column appears → | Schema location updated? |
|---|---|---|
| `addNewColumns` | Stream **fails** with `UnknownFieldException`, having first written the new schema. Restart succeeds | Yes, before failing |
| `addNewColumnsWithTypeWidening` | Same, plus safe widening (e.g. `int` → `long`). Incompatible changes go to the rescued column | Yes |
| `rescue` | Never fails. New columns are **not** added — their data lands in the rescued column | No |
| `failOnNewColumns` | Stream fails, and does **not** update the schema. You fix it by hand | No |
| `none` | New columns are silently ignored, stream continues | No |

**The default is conditional, and the exam knows it:**
`addNewColumns` when you do **not** supply a schema · `none` when you **do**.

**Trap:** "the stream failed with `UnknownFieldException`, what do you do?" — the answer is
usually *restart it*, because the schema was already updated on the way down. That is why
Databricks recommends running Auto Loader inside a job with retries: the first run fails
by design, the retry succeeds.

### The rescued data column

Collects rows that didn't match the schema: type mismatches, missing columns, case
mismatches. It is how you avoid silently dropping data — and it is Auto Loader's *native*
mechanism for bad records. Reach for it before reaching for a generic quarantine path.

---

## Throughput

| Option | Default | What it does |
|---|---|---|
| `cloudFiles.maxFilesPerTrigger` | `1000` | Max new files per micro-batch |
| `cloudFiles.maxBytesPerTrigger` | none | Max new bytes per micro-batch, e.g. `"1g"` |

Set these when large files make micro-batches slow or memory-hungry — they trade latency
for predictable batch duration. Note the asymmetry the exam can test: **files** has a
default of 1000, **bytes** has no default at all.

---

## Which files get picked up

| Option | Default | What it does |
|---|---|---|
| `cloudFiles.includeExistingFiles` | `true` | Process files already in the directory, or only ones arriving after the stream starts |
| `cloudFiles.pathGlobFilter` | none | Glob pattern, e.g. `"*.json"`. Note the **capital F** |
| `modifiedAfter` / `modifiedBefore` | none | Filter by file modification timestamp |
| `cloudFiles.allowOverwrites` | `false` | Re-process a file whose contents changed at the same path |
| `cloudFiles.backfillInterval` | none | Asynchronous backfill sweep, to catch files a notification may have missed. Don't use it with `useManagedFileEvents` |
| `cloudFiles.cleanSource` | `OFF` | `OFF`, `DELETE` or `MOVE` — housekeeping of already-processed source files |

`allowOverwrites = false` is why lesson 5.5 is safely re-runnable: our feed is
deterministic, so re-emitting a batch rewrites an identical file at the same path, and Auto
Loader ignores it. We measured it: 0 new rows.

---

## File detection: the fork the exam names

The guide says "directory listing or file notification". The docs split the second in two.

| | Directory listing | File notification with file events | Classic file notification |
|---|---|---|---|
| Option | none, the default | `cloudFiles.useManagedFileEvents = true` | `cloudFiles.useNotifications = true` |
| How | Lists the input directory | Events through one Databricks-managed queue per external location | Events through a queue per stream, in your cloud account |
| Setup | Access to the data only | An admin enables file events on the external location; no extra permissions on the stream | Elevated cloud permissions |
| Docs' position | Quick starts | **Recommended for most workloads** | Older path |
| Guarantees | Exactly-once | Exactly-once | Exactly-once |

Same correctness everywhere. `cloudFiles.useIncrementalListing` is **deprecated**: if you
see it in an answer, the docs point you to file events instead. File events also needs a
run at least every seven days, or its file cache can expire.

---

## Exam traps

- **Schema location ≠ checkpoint.** One remembers the shape, the other remembers the files.
- **`UnknownFieldException` is not a bug.** With the default mode the schema is already
  updated; the fix is a restart, which is why jobs with retries are the recommended home.
- **The default evolution mode flips** depending on whether you supplied a schema.
  `addNewColumns` is not allowed at all when you provide one.
- **`schemaHints` are ignored if you supply a full schema.** Hints are for guiding
  inference, not for overriding an explicit schema.
- **Auto Loader vs COPY INTO** is a scale question, not a correctness one — see the next
  card.
- **Discovery:** the default is directory listing; the recommendation is file events. Don't
  confuse them. And permissions for queues only apply to classic notification.

## Resources

- Auto Loader options (on the general options page, a long one): https://docs.databricks.com/aws/en/spark/api-options
- Schema inference and evolution: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema
- File detection modes: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-detection-modes
- File notification mode: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode
