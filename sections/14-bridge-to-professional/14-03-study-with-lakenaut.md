---
title: "Reference card: studying with Lakenaut and the official docs"
section: S14 — Bridge to Professional
duration: ~4 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: all (support material)
verified: lakenaut.dev links checked 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. It's the map of where to read more after
> a lesson, and what to trust when two sources disagree.

## What Lakenaut is

[Lakenaut](https://lakenaut.dev/) is a free study site about Databricks, one page per concept,
organized by learning path and by certification. **I built it**, so weigh this recommendation
accordingly. It's an independent project, not affiliated with Databricks, and you don't need it
to pass: everything the exam asks is in this course and in the docs.

## Which source for what

| You want | Go to |
|---|---|
| What a feature actually does, its limits, its exact syntax | [docs.databricks.com](https://docs.databricks.com/). **This is the authority** |
| What's on the exam, and the official sample questions | the [exam page](https://www.databricks.com/learn/certification/data-engineer-associate) and its guide |
| A second explanation of a concept, in plain words | a Lakenaut concept page |
| To see how concepts connect | the [concept graph](https://lakenaut.dev/graph/) and [learning paths](https://lakenaut.dev/paths/) |
| To follow the exam domain by domain | the [Associate roadmap](https://lakenaut.dev/roadmaps/de-associate/) |
| "What is this thing called now?" | [renames](https://lakenaut.dev/naming/) |
| "Is this safe to build on, or still in preview?" | [previews](https://lakenaut.dev/preview/) |
| "When does this stop being supported?" | [end of support](https://lakenaut.dev/lifecycle/) |
| What changed recently | [updates](https://lakenaut.dev/updates/) |

**When sources disagree, the docs and the current exam guide win.** That includes this course and
Lakenaut. Databricks changes fast: check the guide two weeks before your exam, as it tells you to,
and check that any roadmap you follow names the same guide version.

## How to use it with the course

1. Watch the lesson and run the notebook. Hands-on first.
2. If something didn't land, read its concept page below for a second angle.
3. Open the doc link in that lesson's Resources. Get used to the docs: they're how you'll work.
4. Before the practice exam, walk the roadmap and read the concepts you can't explain out loud.

## Concept pages by section

| Section | Topic | Concept pages |
|---|---|---|
| 2 | Foundations | [medallion-architecture](https://lakenaut.dev/concepts/medallion-architecture/) · [delta-lake-overview](https://lakenaut.dev/concepts/delta-lake-overview/) · [ingestion-patterns](https://lakenaut.dev/concepts/ingestion-patterns/) |
| 3 | Platform and compute | [platform-architecture](https://lakenaut.dev/concepts/platform-architecture/) · [compute-options](https://lakenaut.dev/concepts/compute-options/) · [serverless-compute](https://lakenaut.dev/concepts/serverless-compute/) · [compute-access-modes](https://lakenaut.dev/concepts/compute-access-modes/) · [cluster-policies](https://lakenaut.dev/concepts/cluster-policies/) · [instance-pools](https://lakenaut.dev/concepts/instance-pools/) · [runtime-and-photon](https://lakenaut.dev/concepts/runtime-and-photon/) · [notebooks-basics](https://lakenaut.dev/concepts/notebooks-basics/) · [unity-catalog-overview](https://lakenaut.dev/concepts/unity-catalog-overview/) |
| 4 | Delta Lake | [delta-lake-overview](https://lakenaut.dev/concepts/delta-lake-overview/) · [table-history-and-checkpoints](https://lakenaut.dev/concepts/table-history-and-checkpoints/) · [delta-time-travel](https://lakenaut.dev/concepts/delta-time-travel/) · [delta-optimize-vacuum](https://lakenaut.dev/concepts/delta-optimize-vacuum/) · [data-layout-partitioning-zorder](https://lakenaut.dev/concepts/data-layout-partitioning-zorder/) · [merge-upsert](https://lakenaut.dev/concepts/merge-upsert/) |
| 5 | Ingestion | [ingestion-patterns](https://lakenaut.dev/concepts/ingestion-patterns/) · [copy-into](https://lakenaut.dev/concepts/copy-into/) · [structured-streaming-basics](https://lakenaut.dev/concepts/structured-streaming-basics/) · [streaming-triggers](https://lakenaut.dev/concepts/streaming-triggers/) · [auto-loader](https://lakenaut.dev/concepts/auto-loader/) · [lakeflow-connect](https://lakenaut.dev/concepts/lakeflow-connect/) · [ingestion-jdbc-rest](https://lakenaut.dev/concepts/ingestion-jdbc-rest/) · [kafka-streaming](https://lakenaut.dev/concepts/kafka-streaming/) · [semi-structured-data](https://lakenaut.dev/concepts/semi-structured-data/) |
| 6 | Transformation | [dataframe-columns-rows](https://lakenaut.dev/concepts/dataframe-columns-rows/) · [dataframe-joins-unions](https://lakenaut.dev/concepts/dataframe-joins-unions/) · [dataframe-dedup-aggregations](https://lakenaut.dev/concepts/dataframe-dedup-aggregations/) · [sql-window-functions](https://lakenaut.dev/concepts/sql-window-functions/) · [gold-layer-objects](https://lakenaut.dev/concepts/gold-layer-objects/) · [spark-tuning-basics](https://lakenaut.dev/concepts/spark-tuning-basics/) · [udfs-and-alternatives](https://lakenaut.dev/concepts/udfs-and-alternatives/) |
| 7 | Pipelines | [pipelines-overview](https://lakenaut.dev/concepts/pipelines-overview/) · [streaming-tables-sql](https://lakenaut.dev/concepts/streaming-tables-sql/) · [pipelines-expectations](https://lakenaut.dev/concepts/pipelines-expectations/) · [pipelines-event-log](https://lakenaut.dev/concepts/pipelines-event-log/) |
| 8 | Lakeflow Jobs | [jobs-overview](https://lakenaut.dev/concepts/jobs-overview/) · [jobs-task-dependencies](https://lakenaut.dev/concepts/jobs-task-dependencies/) · [jobs-control-flow](https://lakenaut.dev/concepts/jobs-control-flow/) · [jobs-parameters](https://lakenaut.dev/concepts/jobs-parameters/) · [jobs-triggers](https://lakenaut.dev/concepts/jobs-triggers/) · [runs-monitoring](https://lakenaut.dev/concepts/runs-monitoring/) · [jobs-repair-runs](https://lakenaut.dev/concepts/jobs-repair-runs/) · [jobs-serverless](https://lakenaut.dev/concepts/jobs-serverless/) · [jobs-queue-and-concurrency](https://lakenaut.dev/concepts/jobs-queue-and-concurrency/) |
| 9 | CI/CD | [git-folders](https://lakenaut.dev/concepts/git-folders/) · [bundles-overview](https://lakenaut.dev/concepts/bundles-overview/) · [bundles-variables-targets](https://lakenaut.dev/concepts/bundles-variables-targets/) · [bundles-ci-cd](https://lakenaut.dev/concepts/bundles-ci-cd/) · [cli-and-sdk](https://lakenaut.dev/concepts/cli-and-sdk/) |
| 10 | SQL, dashboards, Genie | [sql-editor-basics](https://lakenaut.dev/concepts/sql-editor-basics/) · [sql-warehouse-sizing](https://lakenaut.dev/concepts/sql-warehouse-sizing/) · [dashboards-overview](https://lakenaut.dev/concepts/dashboards-overview/) · [genie-agents](https://lakenaut.dev/concepts/genie-agents/) · [alerts-overview](https://lakenaut.dev/concepts/alerts-overview/) |
| 11 | Governance | [unity-catalog-overview](https://lakenaut.dev/concepts/unity-catalog-overview/) · [managed-vs-external-tables](https://lakenaut.dev/concepts/managed-vs-external-tables/) · [external-locations-and-storage-credentials](https://lakenaut.dev/concepts/external-locations-and-storage-credentials/) · [privileges-grant-revoke](https://lakenaut.dev/concepts/privileges-grant-revoke/) · [row-filters-column-masks](https://lakenaut.dev/concepts/row-filters-column-masks/) · [abac-policies](https://lakenaut.dev/concepts/abac-policies/) · [governed-tags](https://lakenaut.dev/concepts/governed-tags/) · [opensharing-overview](https://lakenaut.dev/concepts/opensharing-overview/) · [lakehouse-federation](https://lakenaut.dev/concepts/lakehouse-federation/) |
| 12 | Troubleshooting | [spark-ui-bottlenecks](https://lakenaut.dev/concepts/spark-ui-bottlenecks/) · [query-profile](https://lakenaut.dev/concepts/query-profile/) · [data-layout-partitioning-zorder](https://lakenaut.dev/concepts/data-layout-partitioning-zorder/) · [liquid-clustering](https://lakenaut.dev/concepts/liquid-clustering/) · [predictive-optimization](https://lakenaut.dev/concepts/predictive-optimization/) · [system-tables](https://lakenaut.dev/concepts/system-tables/) · [cluster-troubleshooting](https://lakenaut.dev/concepts/cluster-troubleshooting/) |

## Going further

Heading for the Professional exam? The
[Professional roadmap](https://lakenaut.dev/roadmaps/de-professional/) is the same idea for that
guide. Lakenaut's writing is CC BY-NC-SA 4.0, so link to it rather than copying it.
