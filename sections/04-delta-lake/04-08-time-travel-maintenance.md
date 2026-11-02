---
title: "Reference card: time travel and table maintenance"
section: S4 — Delta Lake
duration: ~6 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D3.1, D6.4
verified: docs.databricks.com, 2026-08-29
---

> **📄 Reference card.** No video, nothing to run. Every default below was checked against
> the docs on the date in the header.

Lesson 4.3 travelled back in time and 4.4 ran `OPTIMIZE` and `VACUUM`. This card holds the
syntax, and — more importantly — the **retention numbers**, the place where questions on
this topic get specific.

---

## Reading the past

```sql
SELECT * FROM t VERSION   AS OF 123;
SELECT * FROM t TIMESTAMP AS OF '2026-08-18T22:15:12.013Z';
SELECT * FROM t@v123;                     -- version shorthand
SELECT * FROM t@20260818000000000;        -- timestamp shorthand
```

```python
spark.read.option("versionAsOf", 123).table("t")
spark.read.option("timestampAsOf", "2026-08-18").table("t")
```

## Going back for real

```sql
RESTORE TABLE t TO VERSION   AS OF 123;
RESTORE TABLE t TO TIMESTAMP AS OF '2026-08-18';
```

`RESTORE` requires `MODIFY` on the table. It is **not** a rewind: it writes a *new version*
whose contents match the old one, so the history is preserved and the restore itself is
auditable. That is why lesson 4.5's demo `MERGE` could be undone without the catalog
losing its 20 rows.

## Inspecting

```sql
DESCRIBE HISTORY t;
```

Returns one row per commit, newest first: the version, timestamp, user, the operation, and
two columns worth knowing — **`operationParameters`** (what was asked for) and
**`operationMetrics`** (what it did: rows added, files removed, and so on).

**Note from our own validation:** `DESCRIBE HISTORY` is not valid inside a `FROM`
subquery. To get the latest version programmatically, read it into a DataFrame and take
`max(version)` — the pattern lesson 4.3 uses.

---

## How far back can you actually go?

The two numbers are different, and that difference is the trap.

| Property | Default | Governs |
|---|---|---|
| `delta.logRetentionDuration` | **`interval 30 days`** | How long the *transaction log* is kept — i.e. how much history exists |
| `delta.deletedFileRetentionDuration` | **`interval 7 days`** | How long *unreferenced data files* survive before `VACUUM` may delete them |

**In practice you can time travel about 7 days by default**, not 30 — because the log may
still name a version whose data files have already been vacuumed away. History without
files is not a readable version.

To keep more, raise **both**. (On DBR 18.0 and above, `logRetentionDuration` must be
greater than or equal to `deletedFileRetentionDuration`.)

---

## VACUUM

```sql
VACUUM t;            -- FULL is the default
VACUUM t DRY RUN;    -- list what would be deleted, delete nothing
VACUUM t LITE;
VACUUM t FULL;
```

| Mode | What it does |
|---|---|
| `FULL` *(default)* | Deletes files outside the retention window **and** any file in the table directory the table does not reference |
| `LITE` | Uses the transaction log to find unreferenced files, without scanning the whole directory. Faster, but **raises an exception if the log has been pruned** |
| `DRY RUN` | Lists the candidates. Deletes nothing |

Three things to hold on to:

1. **`VACUUM` destroys time travel** beyond the retention threshold. The docs are explicit:
   the ability to query older versions is lost after running it.
2. **There is a safety check** that refuses to vacuum below the retention interval. It is
   disabled with `spark.databricks.delta.retentionDurationCheck.enabled = false` — and the
   correct exam instinct is that you should not.
3. **It breaks shallow clones of this table.** See the previous card.

**Trap:** "we need to reclaim storage immediately" is bait. Vacuuming with a tiny retention
can delete files that a *running* reader or an in-flight transaction still needs.

---

## OPTIMIZE

```sql
OPTIMIZE t;
OPTIMIZE t WHERE order_date >= '2026-01-01';
OPTIMIZE t ZORDER BY (customer_id);
OPTIMIZE t FULL;
```

Without a collocation clause and without liquid clustering, `OPTIMIZE` does **bin-packing**:
it rewrites many small files into fewer, evenly sized ones. That is the small-file problem,
and it is why the command exists.

| Clause | Effect |
|---|---|
| `WHERE` | Restrict compaction to a slice — cheaper on big tables |
| `ZORDER BY` | Collocate related data in the same files, so data skipping (per-file min/max statistics, lesson 4.4) rules out more files |
| `FULL` | Rewrite **all** files, including already-clustered ones (DBR 16.0+) |

**The current recommendation:** *"Databricks recommends liquid clustering for all new
tables instead of Z-ordering."* `ZORDER BY` is not allowed on a table that has liquid
clustering enabled. Section 12 goes into the layout eras properly — for now, know that
Z-ordering is the previous generation and still very much on the exam.

---

## Exam traps

- **30 days of log, 7 days of files.** History is bounded by the *files*, so the practical
  time-travel window is the 7-day one. Answers that say "30 days" are usually the distractor.
- **`RESTORE` adds a version**, it does not erase history.
- **`VACUUM` is irreversible** and ends time travel past the threshold.
- **`DRY RUN` deletes nothing** — the answer whenever a question asks how to check first.
- **Liquid clustering over Z-ordering for new tables**, and the two are mutually exclusive.
- **`DESCRIBE HISTORY` is not a subquery source.**

## Resources

- Time travel and history: https://docs.databricks.com/aws/en/delta/history
- VACUUM: https://docs.databricks.com/aws/en/delta/vacuum
- VACUUM (SQL manual): https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum
- OPTIMIZE: https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize
- Data skipping: https://docs.databricks.com/aws/en/delta/data-skipping
