# Agentic Workflow (`lake-brief`)

A lakehouse brief built as an **agentic workflow**: the steps are fixed in code, and models do the
work only inside the steps that need judgment. Compare with `../agentic-orchestrator`, where the model
decides the steps.

```bash
./lake-brief            # reuse the latest audit if it's < 24h old, otherwise run one
./lake-brief --fresh    # always run a new audit first (5-25 min)
```

## Diagram

Annotated with the first live run (2026-11-16, ref `f5940b`):

```
                         lake-brief  (one run = one brief)
 ════════════════════════════════════════════════════════════════════════════════
  ░ CODE (fixed, testable)      ▓ AGENTS (explore)        █ LLM (one call)
 ════════════════════════════════════════════════════════════════════════════════

 ┌──────────────────────────────┐
 │ ░ 1. AUDIT                   │   latest report ≤ 24h old?
 │   audits/agent-audit-*.md    │──── yes ──► reuse it              (today: 3.2h → reused)
 │                              │──── no  ──► run `audit-lake run`  (5-25 min)
 └──────────────┬───────────────┘
                │ report.md
                ▼
 ┌──────────────────────────────┐
 │ ░ 2. SELECT                  │   read lakehouse-auditor-agent/state/proposals.json
 │   pending + not expired      │   keep ≤ 5           (today: f880f4→presto4,
 │   (the model never picks)    │                              b9078e→spark16)
 └──────────────┬───────────────┘
                │ N proposals
                ▼
        ┌───────┴────────┐  fan out: one per engine, 2 at a time
        ▼                ▼
 ┌─────────────┐  ┌─────────────┐
 │ ▓ 3. ASSESS │  │ ▓ 3. ASSESS │   each = a full `ask-lake` agent run
 │  presto4    │  │  spark16    │   fixed question template:
 │  ref a562a3 │  │  ref d04359 │   "what would pausing <engine> affect?"
 └──────┬──────┘  └──────┬──────┘   (SELECT-only tools, own guardrails, own logs)
        └───────┬────────┘
                │ only the final answers come back  (~98k tokens stayed inside the analysts)
                ▼
 ┌──────────────────────────────┐
 │ █ 4. SUMMARIZE               │   ONE call, output forced into a JSON schema:
 │   report + proposals +       │   { headline, risks[], proposals[], caveats[] }
 │   impact assessments         │   inputs wrapped as DATA, not instructions
 └──────────────┬───────────────┘
                │ brief (JSON)
                ▼
 ┌──────────────────────────────┐
 │ ░ 5. VALIDATE                │   ✗ invented proposal id   → removed
 │   check the model's output   │   ✗ skipped proposal       → added as "investigate"
 │   against the facts          │   ✗ "approve" w/o evidence → changed to "investigate"
 └──────────────┬───────────────┘   (today: 0 corrections)
                │
                ▼
 ┌──────────────────────────────┐
 │ ░ 6. PUBLISH                 │──► briefs/lake-brief-<date>.md
 │                              │──► notification (text built from counts only)
 │                              │──► exit 0 clean · 2 needs you · 1 failed   (today: 2)
 └──────────────┬───────────────┘
                │
 ═══════════════╪════════════════ the workflow stops here; it changes nothing ═══
                ▼
          ┌───────────┐
          │  👤 YOU   │   read the brief → `audit-lake review` → approve / reject
          └─────┬─────┘
                ▼
     auditor re-checks the facts NOW, then pauses (or refuses if things changed)
```

## The six steps

| # | Step | Who | What |
|---|---|---|---|
| 1 | audit | code | reuse `audits/agent-audit-*.md` if fresh, otherwise run `audit-lake run` |
| 2 | select | code | pending, unexpired proposals from `lakehouse-auditor-agent/state/proposals.json` (max 5) |
| 3 | assess | **agents** | one `ask-lake` run per proposed engine, 2 in parallel, from a fixed question template |
| 4 | summarize | **LLM** | one call with a JSON schema → headline, risks, recommendation per proposal |
| 5 | validate | code | removes proposal ids the summary invented, adds ones it skipped, blocks "approve" without an assessment |
| 6 | publish | code | writes `briefs/lake-brief-<date>.md`, optional notification, exit 0 / 2 / 1 |

The brief only **recommends**. Pausing still goes through `audit-lake review`.

## Workflow vs orchestrator

| | `lake-brief` (workflow) | `ask-team` (orchestrator) |
|---|---|---|
| Who picks the steps | code | the model |
| Runs per invocation | fixed: 1 + N proposals | varies with the question |
| Good for | scheduled, repeatable reports | open, one-off questions |
| Testable offline | steps 1, 2, 5, 6 | only the tools |
| Handles surprises | reports them, doesn't investigate | can follow up on them |

## Tests

```bash
uv run python -m pytest -q    # offline: no network, no model
```

Tune with `BRIEF_MAX_REPORT_AGE_HOURS` (24), `BRIEF_MAX_ENGINES` (5), `BRIEF_MAX_PARALLEL` (2),
`BRIEF_ANALYST_TIMEOUT_S` (900), `BRIEF_AUDIT_TIMEOUT_S` (1800), `BRIEF_NOTIFY=macos`, `BRIEF_DIR`.
