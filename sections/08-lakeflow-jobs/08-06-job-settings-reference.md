---
title: "Reference card: Lakeflow Jobs settings"
section: S8 — Lakeflow Jobs
duration: ~7 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D4.1, D4.2, D4.3, D6.2
verified: docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every value below was checked against
> the docs on the date in the header.

**Lakeflow Jobs (formerly Databricks Workflows, formerly Jobs)** is **16% of the exam** —
the third-heaviest domain. Lesson 8.2 built `lakeshore-daily` with five tasks, and the same
job is in your Git folder as `sections/08-lakeflow-jobs/job/lakeshore-daily.json`. This
card covers the settings the exam reaches for.

---

## Triggers

| Trigger | Fires when |
|---|---|
| **None (manual)** *(default)* | Only on **Run now**, or from the API/CLI/another orchestrator |
| **Scheduled** | On a time-based (cron) schedule |
| **File arrival** | New files land in a monitored **Unity Catalog** storage location |
| **Table update** | Monitored tables are updated: any of them, or all of them |
| **Continuous** | A new run starts whenever the previous one finishes **or fails** — the job is always running |
| **Model update** | A UC model or version appears (Beta) |
| **Job completion** | Upstream jobs finish with a matching result (Beta) |

The exam guide names three: scheduled, file arrival, table update. A job can have up to five
triggers under the **Multiple triggers** Beta, and Continuous can't be combined with any other.

**Trap:** *continuous* is a real trigger, but it is not "streaming". It restarts the job whenever
a run ends, including after a failure, and you pay to stay running through idle hours. If a
question wants low-latency processing of arriving data with no idle cost, **file arrival** is the
answer.

**Note:** file-arrival triggers monitor a **Unity Catalog** location — a volume or external
location — not an arbitrary path. Only new files fire them, and wildcards aren't allowed.

| Setting | On | What it does |
|---|---|---|
| **Minimum time between triggers** | File arrival, table update | A cooldown: at most one run per interval |
| **Wait after last change** | File arrival, table update | Waits for quiet; each new change resets the timer |
| **Trigger when** | Table update | **Any table is updated** or **All tables are updated**, up to 25 tables |

---

## Concurrency and queueing

- **Maximum concurrent runs defaults to 1.** By default a second trigger while a run is
  active does not start a parallel run.
- With **queueing** enabled, that run waits — for up to **48 hours** — instead of being
  dropped.

**Trap:** "the schedule fired but no run appeared" is usually the concurrency limit, not a
broken trigger.

---

## Task dependencies and `run_if`

Tasks form a DAG. Each dependency edge carries a **Run if** condition:

| Condition | Runs when | Status if not met |
|---|---|---|
| **All succeeded** *(default)* | Every dependency ran and succeeded | `Upstream failed` |
| **At least one succeeded** | At least one dependency succeeded | `Upstream failed` |
| **None failed** | No dependency failed, and at least one ran | `Upstream failed` |
| **All done** | Every dependency finished, whatever the outcome | — |
| **At least one failed** | At least one dependency failed | `Excluded` |
| **All failed** | Every dependency failed | `Excluded` |

**All done** is the cleanup pattern: a task that must run whether or not the pipeline
worked. **At least one failed** is the alerting pattern.

Cascading rule worth knowing: **if all of a task's dependencies are excluded, that task is
excluded too**, regardless of its own `run_if`.

**Excluded is not failed.** An excluded task is skipped, and it counts as **successful** when
`run_if` is evaluated further down. An `Upstream failed` task counts as **failed**.

The separate **If/else condition task** branches the DAG on a boolean expression — that is
the task lesson 8.3 used to switch on `run_summary`. Its untaken branch shows **Excluded**
(measured on our job). `==` and `!=` compare strings; `>`, `>=`, `<`, `<=` compare numbers. The
JSON spells the operator `EQUAL_TO`.

---

## For each

| Setting | Value |
|---|---|
| **Inputs** | A JSON array (up to 5,000 characters), a task value, or a job parameter |
| **Concurrency** | Iterations in parallel. **Defaults to 1** |
| Nested task | Any standard task type, except another For each |
| Reading the value | `{{input}}`, or `{{input.<key>}}` for an array of objects, in the nested task's parameters |
| Downstream tasks | Depend on the For each task, never on the nested one |

---

## Statuses

| Run status | Means |
|---|---|
| **Queued, Pending, Running** | Waiting, starting, in progress |
| **Skipped** | The run didn't execute, for example because of the concurrency limit |
| **Succeeded** | All tasks succeeded |
| **Succeeded with failures** | Some tasks failed, all **leaf** tasks succeeded |
| **Failed** | One or more **leaf** tasks failed |
| **Timed Out** | The run exceeded its time limit |
| **Canceling, Canceled** | Stopped |

A leaf task has nothing downstream, and the leaves decide the run's result. Colors: green
successful, red unsuccessful, pink skipped, yellow waiting for retry, grey pending, canceled or
timed out. History is kept for up to 60 days.

---

## Retries, timeouts, notifications

| Setting | Scope | Notes |
|---|---|---|
| Retries | Per task | Count plus a retry interval. `ingest_fx` in our job uses two. Most configurations don't retry by default; serverless jobs auto-optimize retries |
| Run duration **Warning** | Per task and per job | Exceeding it fires an event you can notify on. **The run keeps going** |
| Run duration **Timeout** | Per task and per job | Exceeding it sets the status to **Timed Out** and stops it. With retries, it applies to **each retry** |
| Notifications | Job and task | On Start, Success, Failure, Duration warning; email or a system destination. Success also covers Succeeded with failures |

**Fixing a Timed Out task:** raise its Timeout above its real duration, with headroom. Concurrency
and cluster auto-termination have nothing to do with it.

**Retries are per task, not per job.** A question about "retry just the flaky ingestion
step" is answered at the task level.

---

## Parameters

| Kind | Where set | Reaches |
|---|---|---|
| **Job parameters** | On the job | Every task that accepts key-value parameters |
| **Task parameters** | On one task | That task only |

If both use the same key, the **job parameter wins**. You can override job parameters when you run
the job, or when you repair a run.

In a notebook you read them with **widgets** — which is why lesson 3.3 flagged
`dbutils.widgets` as the job-parameter mechanism long before you needed it. Dynamic value
references (run id, start time, and similar) can be passed in as parameter values.

---

## Repair runs

A repair re-runs **only the unsuccessful tasks and their dependents**. Successful tasks
are **not** re-executed. That is the whole point: recovering from a five-task failure without
paying for the four that worked. It works only for jobs with two or more tasks, and it uses the
job's current settings, so you can fix a path or a parameter first.

**The caveat the exam likes:** *"a repair re-runs each unsuccessful task from the
beginning. Lakeflow Jobs doesn't make tasks idempotent, so if a task wrote part of its
output before it failed, re-running it can duplicate that data."*

So repair is safe only if your tasks are idempotent — `MERGE` or overwrite rather than a
blind append. This is the same property that made every notebook in this course re-runnable.

---

## Compute

| | Job compute | All-purpose compute |
|---|---|---|
| Lifetime | Created for the run, terminated after | Long-lived, shared |
| Cost | Lower DBU rate | Higher DBU rate |
| Use for | Scheduled production work | Interactive development |

Running a scheduled job on all-purpose compute is the classic wrong answer: it works, and
it costs more. On Free Edition everything is serverless, so this fork is theory here — and
still on the exam.

---

## Exam traps

- **Continuous ≠ streaming.** A real trigger that restarts on completion *or failure*, always on.
- **Excluded ≠ Upstream failed ≠ Skipped.** Task condition not met; dependency failed; run never
  executed.
- **The leaves decide the run's status.** Succeeded with failures means a middle task failed.
- **Warning keeps running, Timeout stops it.** Both exist at task and job level.
- **Max concurrent runs defaults to 1**; queueing holds a run for up to 48 hours.
- **`run_if` defaults to All succeeded**, and *All done* is how cleanup tasks survive
  failure.
- **Retries are per task.** Timeouts are per task and per job, and apply to each retry.
- **Repair skips successful tasks** — but a partially-written failed task can duplicate
  data on repair.
- **Job compute for scheduled work**, all-purpose for interactive.

## Resources

- Configure a job: https://docs.databricks.com/aws/en/jobs/configure-job
- Triggers: https://docs.databricks.com/aws/en/jobs/triggers
- Configure a task: https://docs.databricks.com/aws/en/jobs/configure-task
- Run if: https://docs.databricks.com/aws/en/jobs/run-if
- If/else task: https://docs.databricks.com/aws/en/jobs/tasks/if-else
- For each task: https://docs.databricks.com/aws/en/jobs/tasks/for-each
- Monitor jobs: https://docs.databricks.com/aws/en/jobs/monitor
- Table update trigger: https://docs.databricks.com/aws/en/jobs/trigger-table-update
- Repair runs: https://docs.databricks.com/aws/en/jobs/repair-job-failures
- Task parameters: https://docs.databricks.com/aws/en/jobs/task-parameters
