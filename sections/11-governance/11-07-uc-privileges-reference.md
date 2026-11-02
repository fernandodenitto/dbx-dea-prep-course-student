---
title: "Reference card: Unity Catalog privileges"
section: S11 — Governance & Security
duration: ~7 min read
type: 📄 reference card (Udemy article — no video)
exam-domains: D7.1, D7.2
verified: docs.databricks.com, 2026-11-01
---

> **📄 Reference card.** No video, nothing to run. Every privilege below was checked
> against the docs on the date in the header.

Governance is **15% of the exam** — the fourth-heaviest domain. Lesson 11.3 granted the
trio that makes a table readable. This card is the whole model.

---

## The hierarchy

```
Metastore
└── Catalog
    └── Schema
        └── Table · View · Volume · Function · Model
```

Privileges granted higher up **inherit downward**, to current *and future* objects.
`GRANT SELECT ON SCHEMA lakeshore.silver` covers every table in `silver`, including ones
created tomorrow.

---

## The traversal rule — the thing that trips everyone

A data privilege is not enough on its own. To read `lakeshore.silver.customers` a user
needs **all three**:

```sql
GRANT USE CATALOG ON CATALOG lakeshore              TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA  lakeshore.silver       TO `analysts`;
GRANT SELECT      ON TABLE   lakeshore.silver.customers TO `analysts`;
```

`USE CATALOG` and `USE SCHEMA` are **access-control boundaries**: an object owner cannot
bypass them, which is exactly what stops a table owner from exposing data through a
catalog they don't control.

**Trap:** "the user has `SELECT` but still gets a permission error." The answer is almost
always a missing `USE CATALOG` or `USE SCHEMA`.

---

## The privileges worth memorising

| Privilege | Grants |
|---|---|
| `SELECT` | Read a table, view or materialized view |
| `MODIFY` | Insert, update and delete — the full DML set |
| `USE CATALOG` / `USE SCHEMA` | Traverse into the container. Required, never sufficient |
| `CREATE TABLE` | Create tables or views in a schema |
| `CREATE SCHEMA` | Create schemas in a catalog |
| `CREATE VOLUME` | Create volumes in a schema |
| `READ VOLUME` / `WRITE VOLUME` | Read / write files inside a volume |
| `READ FILES` / `WRITE FILES` | Read / write files directly under an external location, for example `COPY INTO` from `s3://…` |
| `CREATE EXTERNAL TABLE` | On an external location: create external tables there |
| `CREATE MANAGED STORAGE` | On an external location: use it as a `MANAGED LOCATION` |
| `EXECUTE` | Invoke a function, or load a model for inference |
| `REFRESH` | Manually refresh a materialized view |
| `APPLY TAG` | Add and edit tags |
| `BROWSE` | Discover an object and see its metadata **without** being able to use it |
| `READ METADATA` | See all owner-visible metadata, change nothing |
| `MANAGE` | Manage privileges, transfer ownership, rename, drop |
| `ALL PRIVILEGES` | Everything on the object and its children — with exclusions, below |

Separate `INSERT`, `UPDATE` and `DELETE` privileges exist but are in **Beta**; `MODIFY` is
the one to know for the exam.

`BROWSE` is the discoverability privilege: users can find the table in Catalog Explorer and
know it exists without reading a single row. It is the answer to "let people request access
to data they can't see yet".

---

## Ownership vs MANAGE

They are **not** the same thing, and the docs are explicit about it.

| | Owner | `MANAGE` holder |
|---|---|---|
| Grant and revoke on the object | Yes | Yes |
| Transfer ownership, rename, drop | Yes | Yes |
| Automatically has `SELECT`, `MODIFY`, … | Yes | **No** |
| Can grant those to themselves | — | Yes |

So a `MANAGE` holder is an administrator of the object who is not automatically a *reader*
of it — though nothing stops them granting themselves `SELECT`. Granting `MANAGE` on a
container cascades `MANAGE` to every child. To use `MANAGE`, they still need usage privileges
on the parent containers, though not at the level where `MANAGE` itself was granted.

---

## DENY, stated precisely

The exam guide lists "GRANT, REVOKE, and DENY". Here is what each thing called DENY actually is.

| Thing | Where it applies | Status |
|---|---|---|
| SQL `DENY` statement | `hive_metastore` only. On Unity Catalog it fails with `UC_COMMAND_NOT_SUPPORTED` | Legacy |
| ABAC DENY policy | Unity Catalog, and it denies only `MANAGE ACCESS CONTROL` | Beta |
| Denying `SELECT` or `MODIFY` in Unity Catalog | Nowhere. You revoke, or you use a filter or mask | Not possible |

So for data access, what someone can do is the union of what has been granted, and you remove it by
revoking. A deny **policy** is a different tool: it stops principals, owners included, from changing
permissions on matching objects.

The same legacy caution applies to `USAGE` and `READ_METADATA` as privilege names — those are
Hive-metastore vocabulary; the Unity Catalog equivalents are `USE CATALOG` / `USE SCHEMA` and
`BROWSE` / `READ METADATA`.

---

## Syntax

```sql
GRANT  <privilege> ON <object_type> <name> TO   `principal`;
REVOKE <privilege> ON <object_type> <name> FROM `principal`;

SHOW GRANTS ON TABLE lakeshore.silver.customers;
SHOW GRANTS `analysts` ON SCHEMA lakeshore.silver;   -- for one principal
```

Object types: `CATALOG`, `SCHEMA`, `TABLE`, `VIEW`, `VOLUME`, `FUNCTION`, `MODEL`,
`CONNECTION`, `EXTERNAL LOCATION`, `METASTORE`.

Principals are users, groups or service principals — backtick-quote them. A service principal
is named by its **applicationId**, not its display name. **Grant to groups, not to users**: it
is the practice the exam expects and the only one that scales.

The same operations live in Catalog Explorer: pick the object, **Permissions** tab, **Grant** or
select a row and **Revoke**. To move ownership, use the pencil next to the owner on the Overview
tab, or SQL:

```sql
ALTER TABLE lakeshore.silver.customers OWNER TO `data-engineering`;
```

**`ALL PRIVILEGES` quirks:** once granted, `SHOW GRANTS` displays only `ALL PRIVILEGES`,
not the individual privileges it implies. Revoking it removes the implied set too — except
`MANAGE`, `READ METADATA`, `EXTERNAL USE SCHEMA` and `EXTERNAL USE LOCATION`, which are not
included in it.

---

## Exam traps

- **`SELECT` alone never works.** `USE CATALOG` + `USE SCHEMA` + `SELECT` is the trio.
- **`DENY SELECT` on a Unity Catalog table is not a thing.** The SQL statement is
  `hive_metastore` only. ABAC DENY policies (Beta) cover only `MANAGE ACCESS CONTROL`.
- **`USAGE` and `READ_METADATA` are legacy names.** The current ones are `USE CATALOG` /
  `USE SCHEMA` and `BROWSE` / `READ METADATA`.
- **`MANAGE` is not ownership** and does not carry `SELECT`.
- **Inheritance covers future objects**, which is why granting at schema level is the
  common right answer — and the common over-grant.
- **`BROWSE` is discovery without access**, not a weaker `SELECT`.

## Resources

- Unity Catalog privileges: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
- GRANT: https://docs.databricks.com/aws/en/sql/language-manual/security-grant
- Manage privileges: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/
- DENY statement: https://docs.databricks.com/aws/en/sql/language-manual/security-deny
- ABAC DENY policies (Beta): https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/deny-policies
