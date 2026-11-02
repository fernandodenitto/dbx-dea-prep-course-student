---
title: "Reference card: Structured Streaming options"
section: S5 — Data Ingestion & Loading
duration: ~6 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D2.3
verified: docs.databricks.com + spark.apache.org, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every value below was checked against
> the docs on the date in the header.

Lesson 5.4 built your first stream and 5.5 pointed Auto Loader at it. Those lessons ran one
trigger and one output mode. The exam asks about the others — and about two things that
are commonly taught wrong.

---

## The shape of every streaming query

```python
(spark.readStream                        # source
    .format("cloudFiles")
    .option(…)
    .load(path)
  .writeStream                           # sink
    .option("checkpointLocation", ckpt)  # required
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("catalog.schema.table"))
```

---

## Triggers

| Trigger | Syntax | Behaviour |
|---|---|---|
| *(unspecified)* | — | The default. Micro-batches as fast as they can run, ~3-5 s latency. Equivalent to `processingTime` with a 0 ms interval |
| Fixed interval | `.trigger(processingTime="10 seconds")` | Checks for new data every interval. The cost/latency dial. Stream stays alive |
| Incremental batch | `.trigger(availableNow=True)` | Processes everything available **now**, then stops. The one you schedule in a job |
| Real-time | `.trigger(realTime="5 minutes")` | Sub-second latency for operational workloads |
| Once | `.trigger(once=True)` | **DEPRECATED** since DBR 11.3 LTS — use `availableNow` |
| Continuous | `.trigger(continuous="1 second")` | **Not supported on Databricks.** Experimental in open-source Spark only |

**On serverless, most of this table is theory.** Serverless notebooks and jobs accept only
`availableNow` and the deprecated `once`. `processingTime`, and leaving the trigger out,
raise `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`: lesson 5.4 shows the error. So:

| You need | Serverless | Classic compute |
|---|---|---|
| Scheduled catch-up | `availableNow` | `availableNow` |
| A stream that stays alive | A Lakeflow pipeline in continuous mode (Section 7) | `processingTime` |

**Two traps here, and both are widespread misinformation:**

- **`Trigger.Once` is deprecated.** Plenty of study material still presents it as the way
  to run a stream as a batch. `availableNow` replaced it — and unlike `once`, it can
  process the backlog in *several* micro-batches instead of forcing one giant one.
- **`continuous` is not a Databricks feature.** If an answer offers continuous processing
  as the way to get low latency on Databricks, it is wrong. Real-time mode is the answer.

A stream with `availableNow` is not "not a stream": it keeps the checkpoint, the
exactly-once guarantees and the schema handling. It simply stops when it runs out of work.
That is the whole trick behind incremental batch ingestion.

---

## Output modes

| Mode | What reaches the sink | Supported for |
|---|---|---|
| `append` *(default)* | Only rows new since the last trigger | Queries whose emitted rows never change: `select`, `where`, `join`, … |
| `complete` | The entire result table, every trigger | Aggregations only |
| `update` | Only rows that changed since the last trigger | Aggregations, and most non-aggregating queries |

Support by query type:

| Query | Allowed modes |
|---|---|
| Aggregation on event time **with a watermark** | append, update, complete |
| Aggregation **without** a watermark | complete, update — **not append** |
| Queries with joins | append |
| Everything else | append, update |

**Trap:** *"aggregate a stream and write it with `append`"* is a wrong answer unless there
is a watermark. Without one, the engine can never declare a group final, so it has nothing
it is allowed to append.

> **🎓 Professional preview:** watermarks, late data and the state store are a whole
> section of the Professional exam. Here you only need the rule above.

---

## The checkpoint

`checkpointLocation` is required, and it stores progress and state so the query can
recover and still deliver exactly-once results.

Three things to know:

1. **One checkpoint per query.** Two streams sharing a directory corrupt each other.
2. **Deleting the checkpoint means reprocessing from scratch** — which, into an append
   sink, means duplicates.
3. **The checkpoint is not the schema location.** Auto Loader has both, and they answer
   different questions: *what have I read* versus *what does it look like*.

---

## foreachBatch

> **🎓 Professional preview:** the Associate guide doesn't name `foreachBatch` or
> watermarks. Read this block as a head start, not as exam material for this certification.

Hands you each micro-batch as a normal DataFrame:

```python
def upsert(microbatch_df, batch_id):
    if not microbatch_df.isEmpty():
        microbatch_df.createOrReplaceTempView("updates")
        microbatch_df.sparkSession.sql("MERGE INTO … USING updates …")

stream.writeStream.foreachBatch(upsert).option("checkpointLocation", ckpt).start()
```

Two arguments: **the micro-batch DataFrame and the batch id.**

Use it for:

- **`MERGE INTO` from a stream** — you *must* use `foreachBatch`; there is no streaming
  merge writer.
- **Writing to a sink with no streaming support**, using its batch writer.
- **Fanning one stream out to several sinks.**

Caveats worth remembering:

- It guarantees **at-least-once**, not exactly-once. Deduplicate using the `batchId` to
  get back to exactly-once.
- **Handle the empty DataFrame**, or the query can fail.
- It does not work with continuous processing — which on Databricks is moot anyway.

---

## Exam traps

- **`Trigger.Once` is deprecated; `continuous` is not supported on Databricks.** Both still
  appear in older study material.
- **`processingTime` and the default trigger fail on serverless.** `availableNow` is the
  answer there; a continuous Lakeflow pipeline if it must stay alive.
- **Aggregation + `append` needs a watermark.** Otherwise the mode is illegal.
- **`availableNow` is still a stream** — checkpoint, exactly-once, schema tracking, all of
  it. It just terminates.
- **One checkpoint per query**, and deleting it means reprocessing everything.
- **A streaming `MERGE` means `foreachBatch`.** There is no other route.

## Resources

- Triggers: https://docs.databricks.com/aws/en/structured-streaming/triggers
- Serverless limitations: https://docs.databricks.com/aws/en/compute/serverless/limitations
- foreachBatch: https://docs.databricks.com/aws/en/structured-streaming/foreach
- Structured Streaming concepts: https://docs.databricks.com/aws/en/structured-streaming/concepts
- Output modes (Apache Spark): https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html
