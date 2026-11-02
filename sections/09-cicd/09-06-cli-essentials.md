---
title: "Reference card: Databricks CLI essentials"
section: S9 — CI/CD
duration: ~6 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D5.4
verified: Databricks CLI v1.19.0 (`--help` output) + docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every command below was checked against
> **the installed CLI, v1.19.0**, not from memory. Run `databricks <group> --help` — the
> binary on your machine is always more authoritative than any page, including this one.

Lesson 9.3 used the CLI to deploy a bundle. That is one corner of it. The CLI is the same
REST API surface with a friendlier face, and it is how anything automated talks to
Databricks.

---

## Authentication and profiles

```bash
databricks auth login --host https://<workspace-host> --profile DEFAULT
databricks auth profiles          # list what is in ~/.databrickscfg
databricks auth describe          # which credentials am I actually using, and from where
databricks auth token             # print a token
databricks auth switch            # set the default profile
databricks auth logout
```

Credentials live in `~/.databrickscfg`, one block per profile. **Every command takes
`-p/--profile`**, and on a machine with more than one workspace you should always pass it
explicitly rather than trusting the default.

```bash
databricks current-user me --profile DEFAULT     # the "am I connected?" check
```

**Trap:** `auth describe` is the debugging command. When a call hits the wrong workspace,
it tells you which credential was picked and *where it came from* — env var, profile, or
config file. That resolution order is the usual culprit.

---

## The command groups

`databricks --help` prints them grouped. The ones that matter for this exam:

| Group | For |
|---|---|
| `workspace` | List, import, export, delete notebooks and folders |
| `fs` | Filesystem operations, including `dbfs:` and volume paths |
| `secrets` | Secret scopes and secrets |
| `repos` / `git-credentials` | Git folders and the token behind them |
| `jobs` | Create, edit, run, delete jobs |
| `pipelines` | Lakeflow Spark Declarative Pipelines |
| `clusters`, `cluster-policies`, `instance-pools` | Classic compute |
| `warehouses` | SQL warehouses |
| `catalogs`, `schemas`, `tables`, `volumes`, `grants` | Unity Catalog |
| `bundle` | DABs — see the previous card |

---

## Moving notebooks in and out

```bash
databricks workspace import /Workspace/Users/me/demo \
  --file ./notebook.py --format SOURCE --language PYTHON --overwrite --profile DEFAULT

databricks workspace export /Workspace/Users/me/demo --format SOURCE --profile DEFAULT
databricks workspace list /Workspace/Users/me --profile DEFAULT
```

`--format SOURCE` is the format this whole repository uses: `# Databricks notebook source`
with `# COMMAND ----------` separators. It round-trips cleanly and it diffs in git, which
is the entire reason the course ships notebooks that way.

---

## Running jobs

```bash
databricks jobs list --profile DEFAULT
databricks jobs get 123 --profile DEFAULT
databricks jobs run-now 123 --profile DEFAULT
databricks jobs submit --json @run.json --profile DEFAULT   # one-time run, no job created
```

**`run-now` vs `submit`:** `run-now` triggers a job that already exists. `submit` runs a
workload **without creating a job** — a one-time run. Every notebook in this course was
validated with `submit`, which is why the workspace has no leftover validation jobs.

---

## Unity Catalog from the shell

⚠️ These take **positional** arguments, not flags, and so do `jobs get` and `jobs run-now`
above. It's a common and frustrating mistake:

```bash
databricks catalogs list --profile DEFAULT
databricks schemas list <CATALOG> --profile DEFAULT
databricks tables list <CATALOG> <SCHEMA> --profile DEFAULT
databricks tables get <CATALOG>.<SCHEMA>.<TABLE> --profile DEFAULT
```

`databricks schemas list --catalog-name foo` and `databricks jobs get --job-id 123` do **not**
exist: both fail with `unknown flag`.

---

## Output as JSON

```bash
databricks jobs list -o json --profile DEFAULT | jq '.[].settings.name'
```

`-o json` on any command makes the CLI scriptable. This is what turns it from a
convenience into automation.

---

## Exam traps

- **The CLI and the REST API are the same surface.** A question asking "how do you automate
  X without the UI" accepts either; there is rarely a UI-only answer.
- **`submit` creates no job**, `run-now` needs one to exist.
- **Unity Catalog and `jobs get`/`run-now` take positional arguments.**
- **Settings resolve in an order:** environment variables come before `.databrickscfg`
  profiles, and the CLI stops at the first match. When a command hits the wrong workspace,
  `auth describe` explains why.
- **`--format SOURCE`** is what makes notebooks reviewable in git.

## Resources

- CLI overview: https://docs.databricks.com/aws/en/dev-tools/cli/
- CLI installation: https://docs.databricks.com/aws/en/dev-tools/cli/install
- CLI authentication: https://docs.databricks.com/aws/en/dev-tools/cli/authentication
