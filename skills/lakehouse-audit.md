---
name: lakehouse-audit
description: Audit the watsonx.data lakehouse for broken, wasteful, or risky configuration — stale/unreachable catalogs, orphaned storage, idle engines, plaintext secrets, cross-region storage. Read-only investigation that produces a ranked report and proposes fixes for human approval. Use when the user asks to audit, health-check, clean up, or review cost/security of their lakehouse.
allowed-tools: mcp__watsonx_data-mcp-server__watsonxdata-get-instance-details, mcp__watsonx_data-mcp-server__watsonxdata-list-engines, mcp__watsonx_data-mcp-server__watsonxdata-list-schemas, mcp__watsonx_data-mcp-server__watsonxdata-list-tables, mcp__watsonx_data-mcp-server__watsonxdata-list-storage-registrations, mcp__watsonx_data-mcp-server__watsonxdata-list-database-registrations, mcp__watsonx_data-mcp-server__watsonxdata-get-database-connection-status, mcp__watsonx_data-mcp-server__watsonxdata-list-spark-applications, mcp__watsonx_data-mcp-server__watsonxdata-get-wxd-catalog-access-policies, mcp__watsonx_data-mcp-server__watsonxdata-get-associated-engines, Write
---

# Lakehouse Health Audit

You are auditing a watsonx.data instance. Your job is to **investigate, not to fix**.
Follow the leads: each finding may tell you where to look next. That adaptive
investigation is the point — do not just run every tool once and dump output.

## Ground rules (non-negotiable)

1. **Read-only.** Never call pause/resume/create/update/rename/insert/delete/scale
   tools during the audit. Remediation happens only in Phase 5, one action at a
   time, after the user explicitly approves that specific action.
2. **Never echo secrets.** If a tool returns credentials (access keys, secret
   keys, passwords, tokens, connection-user values for secret-bearing configs),
   report *that* a secret is exposed and *where* — never the value. Redact as
   `AKIA****` / `[REDACTED]`. This applies to the chat AND the report file.
3. **No heavy queries.** Do not run `execute-select` or `explain-analyze` on data
   tables. Metadata calls only.
4. **Run independent calls in parallel** (e.g. `list-schemas` for every catalog
   at once).
5. **Be honest about coverage.** If a tool fails or something can't be checked,
   say so in the report — don't silently skip it.

## Phase 1 — Inventory

Run in parallel:
- `get-instance-details` → note instance **region** (needed for cross-region check)
- `list-engines`
- `list-storage-registrations`
- `list-database-registrations`

Build a **catalog master list** = union of:
- every `associated_catalogs` entry across engines
- every catalog in storage registrations
- every catalog in database registrations

For each catalog record: which engines it's attached to, its backing storage/DB
registration, `created_by`, type.

## Phase 2 — Probe

For every catalog attached to at least one **Presto/Prestissimo** engine, call
`list-schemas` using a Presto engine it's attached to (prefer the engine with the
most catalogs, to reuse one engine). Run them all in parallel.

Classify each result:
- error 404 / "does not exist" → **STALE**
- error 503 / connection failed → **UNREACHABLE** → follow up: find its DB
  registration, call `get-database-connection-status` if available
- 0 schemas, or only `information_schema` → **EMPTY**
- otherwise → **OK** (record schema count)

Catalogs attached only to Spark engines can't be probed via Presto — mark as
**NOT PROBED** (coverage gap, not a finding).

For every **Spark** engine, call `list-spark-applications` with
state `["running","accepted","submitted","waiting"]`, limit 20.

## Phase 3 — Analyze

Apply these checks. Each produces findings with a severity.

| Check | Condition | Severity |
|---|---|---|
| Plaintext secrets | DB/storage registration property whose key contains `secret`, `password`, `access-key`, `token`, `key` and `encrypt` is not `true` | **CRITICAL** |
| Shared credentials | Same (redacted) secret reused across multiple registrations | **CRITICAL** (note blast radius) |
| Stale catalog | Catalog attached to an engine but probe returns 404 | **HIGH** |
| Unreachable source | Probe returns 503 / connection failure | **HIGH** |
| Inactive storage | Storage registration `state` ≠ `active` | **HIGH** if a catalog depends on it, else MEDIUM |
| Orphaned catalog | Catalog has a registration but no engine attached | **MEDIUM** |
| Idle Spark engine | Spark engine `running` with 0 active applications | **MEDIUM** (cost) |
| Duplicate Presto coverage | One Presto engine's catalogs are a subset of another's, both running | **MEDIUM** (cost — candidate to pause/consolidate) |
| Cross-region storage | Storage region ≠ instance region | **LOW** (egress cost + latency) |
| Empty catalog | 0 schemas or only `information_schema` | **LOW** |
| `donotdelete` naming | Engine/bucket named "donotdelete"/"do-not-delete" | INFO — flag so remediation skips it unless the user insists |

**Follow the thread.** Examples of the adaptive behaviour expected:
- A stale catalog → check whether its DB registration still exists (leftover registration to remove).
- An unreachable catalog → check the connection status, report hostname (not credentials).
- Secrets found in one registration → scan *all* registrations for the same value.
- An orphaned catalog on an inactive bucket → combine into one finding.

Optionally (if the user asked for access review), call
`get-wxd-catalog-access-policies` for HIGH/CRITICAL catalogs only.

## Phase 4 — Report

Write the report to `audits/lakehouse-audit-YYYY-MM-DD.md` (use today's date)
AND show a condensed version in chat. Structure:

```markdown
# Lakehouse Audit — <date>
Instance: <id> (<region>) · Engines: N · Catalogs: N · Registrations: N

## Summary
CRITICAL: n · HIGH: n · MEDIUM: n · LOW: n

## Findings
### [CRITICAL] <title>
- **What:** ...
- **Where:** catalog/registration/engine ids
- **Owner:** created_by
- **Why it matters:** ...
- **Proposed fix:** ... (tool + args if a tool can do it, else manual step)

(... ranked most severe first ...)

## Coverage
- Probed: n catalogs · Not probed (Spark-only): list · Tool errors: list

## Investigation trace
Short numbered list of what you checked and why each step led to the next.
```

The **investigation trace** matters — it's what makes this an agent demo rather
than a script. Keep it to the decisions, not every call.

## Phase 5 — Remediate (only with approval)

After the report, list proposed actions that the available tools can perform
(e.g. pause an idle Spark engine with no running apps). For each:
- state the exact tool and arguments,
- state the impact and who owns the resource,
- **ask the user to approve it individually.**

Never use `force: true`. Never act on `donotdelete` resources without explicit
confirmation naming that resource. Items without a tool (rotating credentials,
removing registrations) go in the report as manual steps.
