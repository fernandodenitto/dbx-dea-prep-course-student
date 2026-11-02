---
title: "Reference card: the renaming map"
section: S2 — Data Engineering Foundations
duration: ~5 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D1.1 (and every other domain, indirectly)
verified: docs.databricks.com + Databricks CLI v1.19.0, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. **Bookmark this one.** It is the page
> that stops you losing marks to vocabulary rather than to knowledge.

Lesson 2.1 explained *why* Databricks renames things so often. This is the map. Keep it
open while you study, because the exam, the documentation, the product UI and every
tutorial you find online are frequently on **different sides of a rename**.

---

## The map

| Current name | Former name(s) |
|---|---|
| **Lakeflow Jobs** | Databricks Workflows · Jobs |
| **Lakeflow pipelines** (Spark Declarative Pipelines; **SDP** for short in this course) | Lakeflow Spark Declarative Pipelines · Lakeflow Declarative Pipelines (what the exam guide still says) · **Delta Live Tables (DLT)** |
| **Lakeflow Connect** | *(new — no former name)* |
| **Declarative Automation Bundles (DABs)** | Databricks Asset Bundles |
| **AI/BI Dashboards** | Lakeview dashboards · DBSQL dashboards |
| **Genie** | AI/BI Genie |
| **Genie Agent** | Genie space |
| **Genie Code** | Databricks Assistant |
| **Git folders** | Repos |
| **Catalog Explorer** | Data Explorer |
| **Unity Catalog volumes** | *(replaces the DBFS / mounts pattern)* |
| **Medallion architecture** | Multi-hop architecture *(not a rename: both names are in use, and they mean the same thing)* |
| **OpenSharing** | Delta Sharing *(beyond the exam guide: its outline doesn't list sharing)* |
| **Unity Gateway** | Unity AI Gateway · AI Gateway *(beyond the exam, not used in this course)* |

**The pipelines row is the messy one, so here it is in full.** The docs title the product
area "Spark Declarative Pipelines on Lakeflow" and call the product "Lakeflow pipelines",
built on the open-source Apache Spark Declarative Pipelines. The exam guide writes
"Lakeflow Declarative Pipelines". The CLI help still says "Lakeflow Spark Declarative
Pipelines". I found no announcement of a formal rename, so Databricks' own usage is simply
inconsistent. All of these are the same product, and in this course it's `SDP` for short.

---

## Where the old names still live

Renaming a product does not rename everything it touches. Expect the former name in:

- **Documentation URLs and old links.** Pipeline docs now live under
  `docs.databricks.com/…/ldp/`, and materialized views and streaming tables under
  `…/sql/mv-st/`. The old `…/dlt/` links in blog posts and tutorials still work: they redirect
  to `/ldp`. The Unity Gateway docs still sit under `…/ai-gateway/`.
- **API and CLI surfaces.** The CLI still has a `repos` command group, and its help text
  already says *"Declarative Automation Bundles"*, while `databricks pipelines --help` says
  *"Lakeflow Spark Declarative Pipelines API"*. The renames landed at different times.
- **The exam and its practice material.** Question banks lag product marketing by months. The
  current guide says "Lakeflow Declarative Pipelines", and **"Delta Live Tables" is the old
  name you are most likely to meet** in older practice questions and courses.
- **Table and job properties.** A pipeline is still marked `development: true`.

---

## How to answer a question that uses an old name

1. **Translate first, then answer.** "Delta Live Tables" or "Lakeflow Declarative Pipelines"
   → Lakeflow pipelines. "Databricks Workflows" → Lakeflow Jobs. "Multi-hop" → medallion. The
   underlying question is unchanged.
2. **Never pick an answer because it uses the newest word.** Distractors are sometimes
   built out of current-sounding vocabulary attached to wrong behaviour.
3. **Watch for genuinely retired concepts.** This is the important distinction: *Repos →
   Git folders* is a rename, and both mean the same thing. *DBFS mounts → Unity Catalog
   volumes* is **not** a rename — it is a replacement, and the old pattern is legacy. Same
   for `hive_metastore` versus Unity Catalog, and for the old `USAGE` / `READ_METADATA`
   privileges versus `USE CATALOG` / `USE SCHEMA` and `BROWSE` / `READ METADATA`.

---

## Keeping up after the exam

**[rebricked.org/databricks](https://rebricked.org/databricks/)** tracks Databricks renames
over time — genuinely useful when you meet a name you don't recognise.

One honest warning, from building this course: **treat it as a lead, not as a source.**
When we checked four renames against it, one was wrong. The authority is
`docs.databricks.com`; rebricked tells you *where to look*.

---

## Exam traps

- **"Delta Live Tables" and "Lakeflow Declarative Pipelines" are both in circulation.** Read
  them as Lakeflow pipelines and move on.
- **A rename is not a replacement.** Repos → Git folders is a rename. DBFS mounts → volumes
  is a replacement, and the old one is a wrong answer in any new design.
- **New vocabulary is not evidence of a right answer.**
- **Doc URLs keep old names.** A `/dlt/` link in an old tutorial redirects to the current
  page under `/ldp/`; it isn't a sign the feature is gone.

## Resources

- Rename tracker (a lead, not a source): https://rebricked.org/databricks/
- Lakeflow Jobs: https://docs.databricks.com/aws/en/jobs/
- Lakeflow pipelines (Spark Declarative Pipelines on Lakeflow): https://docs.databricks.com/aws/en/ldp/
- Git folders: https://docs.databricks.com/aws/en/repos/
- Unity Catalog volumes: https://docs.databricks.com/aws/en/volumes/
- Medallion architecture (a.k.a. multi-hop): https://docs.databricks.com/aws/en/lakehouse/medallion
- OpenSharing: https://docs.databricks.com/aws/en/opensharing/
- Unity Gateway: https://docs.databricks.com/aws/en/ai-gateway/
