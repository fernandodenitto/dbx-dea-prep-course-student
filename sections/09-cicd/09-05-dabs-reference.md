---
title: "Reference card: Declarative Automation Bundles"
section: S9 — CI/CD
duration: ~7 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D5.2, D5.3
verified: docs.databricks.com + Databricks CLI v1.19.0, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Commands were checked against the docs
> **and against the installed CLI (v1.19.0)** on the date in the header. Newer CLIs exist;
> `databricks --version` tells you yours, and `--help` is authoritative.

**Declarative Automation Bundles (DABs), formerly Databricks Asset Bundles**, are how you
express a project as code: jobs, pipelines, dashboards and apps in YAML, deployed per
environment. Lesson 9.2 built one and 9.3 deployed it. This card is the full surface.

---

## The commands

```bash
databricks bundle init default-python          # scaffold from a template
databricks bundle validate                     # parse + check, deploys nothing
databricks bundle validate --strict            # same, and warnings become errors
databricks bundle plan                         # show the deployment plan
databricks bundle deploy --target dev          # upload + create/update resources
databricks bundle run my_job --target dev      # run a job, pipeline update or app
databricks bundle summary                      # what is deployed, with URLs
databricks bundle destroy --target dev         # remove the deployed resources
databricks bundle sync                         # sync the tree to the workspace only
databricks bundle open my_job                  # open the resource in a browser
databricks bundle schema                       # JSON Schema for the config format
databricks bundle generate job --existing-job-id 123 --key my_job         # YAML from a live job
databricks bundle generate pipeline --existing-pipeline-id abc --key my_pipeline
databricks bundle deployment bind my_job 123   # adopt an existing resource
databricks bundle generate job --existing-job-id 123 --key my_job --bind  # both steps at once
```

Flags worth knowing: `-t/--target`, `-p/--profile`, `--var="key=value"`, `-o json`. On
`deploy`: `--auto-approve`, `--fail-on-active-runs`, and `--select` to deploy only named
resources.

**Trap:** `validate` needs credentials, because it talks to your workspace. It creates no
resources (on v1.19.0 it did leave an empty `.bundle/<name>/<target>/files` folder), and it
does **not** prove the deployment works. A bundle can validate perfectly and
deploy nothing at all, which is exactly the bug this course hit: a missing `include:`
produced a valid, empty bundle. Read what `validate` prints, don't just check that it
exited 0. `bundle summary` after deploy lists what really exists.

**Trap:** `bundle generate` alone connects nothing. Deploy the generated YAML without
binding and you get a second copy of the job. Check generated YAML too: a pipeline task
keeps its hardcoded `pipeline_id`.

---

## The file: top-level mappings of `databricks.yml`

| Mapping | What it configures |
|---|---|
| `bundle` | **Required.** `name`, plus `databricks_cli_version`, `git`, `cluster_id` |
| `include` | Extra config files to pull in, by path or glob — **the one everybody forgets** |
| `resources` | The actual jobs, pipelines, dashboards, apps, volumes… |
| `targets` | The environments. Exactly one may carry `default: true` |
| `variables` | Custom variables: description, default, type (see below) |
| `workspace` | `host`, `profile`, `root_path`, `artifact_path`, `file_path` |
| `sync` | Which files are included in / excluded from the upload |
| `artifacts` | Build settings for wheels and other artifacts |
| `permissions` | Access control applied to the deployed resources |
| `run_as` | The identity the resources run as: `user_name` or `service_principal_name` |
| `scripts` | Scripts the bundle can run |

**Do not pin `workspace.host`.** This course had it pinned and it did two bad things:
exposed the instance, and pointed every student's deploy at it. Leave it out and the CLI
resolves the workspace from the caller's profile.

---

## Variables and overrides

Declare a variable at the top level and read it as `${var.name}`. The CLI takes the first
value it finds, in this order:

1. `--var="name=value"` on the command
2. An environment variable, `BUNDLE_VAR_name`
3. `.databricks/bundle/<target>/variable-overrides.json`
4. The target's `variables` mapping
5. The variable's `default`

We confirmed the order live on v1.19.0 with `bundle validate -o json`: the flag beat the
environment, the environment beat the overrides file, the file beat the target, and the
target beat the default.

```yaml
variables:
  output_schema:
    default: bundle_output
targets:
  dev:
    variables:
      output_schema: bundle_dev      # a value per environment
  prod:
    variables:
      output_schema: bundle_prod
    resources:
      jobs:
        lakeshore_health_check:
          schedule:
            pause_status: PAUSED     # change one field, inherit the rest
```

**Secrets in CI:** declare a variable with no default and supply it as
`BUNDLE_VAR_db_password` from the CI system's secret store. It never touches Git. A variable
is a string unless you set `type: complex`.

---

## Target modes — where the exam lives

```yaml
targets:
  dev:
    mode: development
    default: true
  test:
    presets:
      name_prefix: "[test] "
      trigger_pause_status: PAUSED
  prod:
    mode: production
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
```

### `mode: development`

- **Prefixes every resource** with `[dev ${workspace.current_user.short_name}]`
- Tags deployed jobs and pipelines with a `dev` tag. Unity Catalog schemas get a `dev_<you>_`
  name prefix (seen live)
- Marks deployed pipelines as `development: true`
- **Pauses all schedules and triggers** — override per job with `pause_status: UNPAUSED`
- Enables concurrent runs, and **disables the deployment lock**, for faster iteration
- Lets you override the cluster with `--cluster-id`

### `mode: production`

- Validates that deployed pipelines are marked **`development: false`**. `development: true`
  in a prod deploy is an error, and leaving the key out passes
- Validates that the **current Git branch matches** the branch declared in the target
  (override with `--force`)
- **Docs:** if you are not deploying as a service principal, it validates that `artifact_path`,
  `file_path`, `root_path` and `state_path` are not overridden to a specific user, and that
  `run_as` and `permissions` are specified. The docs recommend service principals for prod
- **Refuses** cluster overrides from the command line
- **Does not touch schedules.** Nothing is paused, nothing is unpaused: you get what the YAML
  declares

**What CLI v1.19.0 did on our bundle:** a missing `workspace.root_path` is a hard error ("must
set 'workspace.root_path' to make sure only one copy is deployed"), and the error itself
suggests a path under your username. With that set, a user-specific path and no `run_as` or
`permissions` validated and deployed with no warning, even under `--strict`. So our prod target
is a single-user teaching simplification, and a company's looks more like this:

```yaml
  prod:
    mode: production
    workspace:
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}   # or a restricted folder
    run_as:
      service_principal_name: <application-id>
    permissions:
      - group_name: data-engineers
        level: CAN_MANAGE
```

📖 We have not deployed that form; the mappings are from the docs. The CLI warns that
`/Workspace/Shared` is writable by all workspace users, so a real prod picks a restricted folder.

**Trap, learned the hard way in this course:** ours was missing `root_path`, so the prod target
had never validated, and the error only appears the day you try to ship. Validate **every**
target in CI, not just dev.

### No mode, `presets` instead

A target needs no `mode`. `presets` sets the behaviour by hand: `name_prefix`,
`trigger_pause_status`, `pipelines_development`, `jobs_max_concurrent_runs`, `tags`. If both
are set, presets override the mode, and a resource's own setting overrides the preset.

---

## The dev prefix is a feature, not noise

In development mode your job is genuinely named `[dev yourname] lakeshore-health-check`.
That is deliberate: it makes dev deployments obvious in a shared workspace and stops two
engineers colliding on one job name. Do not "fix" it.

---

## Exam traps

- **`validate` proves the config parses, not that the bundle does anything.**
- **A missing `include:` gives you a valid, empty bundle** — no error, no resources.
- **Development mode pauses schedules.** "My scheduled job never ran after deploying" is
  usually this, and the fix is `pause_status: UNPAUSED` or deploying to prod. Production mode
  doesn't unpause an explicit `PAUSED`.
- **Without a service principal, the docs say production mode wants non-user paths, `run_as`
  and `permissions`.** Learn that for the exam, even though the CLI we tested only enforced
  `root_path`.
- **`BUNDLE_VAR_<name>`** is how CI passes a secret. `--var` beats it; both beat the target's
  `variables`, which beat `default`.
- **DABs is the current name; Databricks Asset Bundles is the former one.** The exam may
  use either.

## Resources

- Bundle configuration: https://docs.databricks.com/aws/en/dev-tools/bundles/settings
- Deployment modes: https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes
- Variables: https://docs.databricks.com/aws/en/dev-tools/bundles/variables
- Bundle commands: https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands
- Bundles overview: https://docs.databricks.com/aws/en/dev-tools/bundles/
