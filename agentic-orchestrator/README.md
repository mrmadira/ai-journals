# Agentic Orchestrator (`ask-team`)

A multi-agent coordinator. It answers questions that need both the **data** (analyst agent) and
the **infrastructure** (audit agent), by delegating to them. It never touches watsonx.data itself.

```bash
./ask-team "Which engines are idle per the latest audit, and which catalog has the most tables?"
./ask-team            # interactive; follow-ups remember earlier answers
```

## Diagram

```
                       ask-team  (one question → one answer)
 ════════════════════════════════════════════════════════════════════════════════
  ░ CODE (enforces limits)      ▓ AGENTS (explore)       █ LLM (decides the steps)
 ════════════════════════════════════════════════════════════════════════════════

        ❓ "Which engines are idle per the audit, and which catalog has the most tables?"
                                          │
                                          ▼
          ┌──────────────────────────────────────────────────────────────┐
          │ █ ORCHESTRATOR LOOP                                           │
          │                                                               │
          │      ┌──────────► THINK  "two parts: infra + data,            │
          │      │              │     independent → do both at once"      │
          │      │              ▼                                         │
          │   OBSERVE        ACT    picks tools, writes the analyst's     │
          │  (reads only        │   question itself, or answers directly  │
          │   final answers)    │                                         │
          │      └──────────────┘   repeat until the model says "done"    │
          │                                                               │
          │  tools it sees: ask_analyst · read_latest_audit ·             │
          │                 audit_status · run_audit                      │
          │  tools it does NOT have: SQL · MCP · approve · pause          │
          └───────────────────────────┬───────────────────────────────────┘
                                      │ tool calls (any order, any number, chosen by the model)
 ┌────────────────────────────────────┴─────────────────────────────────────────┐
 │ ░ Team: the code gate between the model and the specialists                   │
 │   ≤ 6 runs per question · ≤ 2 analysts at once · timeouts · errors → is_error │
 └───┬──────────────────────┬──────────────────────┬───────────────────┬────────┘
     │                      │                      │                   │
     ▼                      ▼                      ▼                   ▼
 ┌─────────────┐   ┌─────────────────┐   ┌──────────────────┐  ┌──────────────────┐
 │ ask_analyst │   │read_latest_audit│   │  audit_status    │  │   run_audit      │
 └──────┬──────┘   └────────┬────────┘   └────────┬─────────┘  └────────┬─────────┘
        │ new process       │ read file           │ no model            │
        ▼                   ▼                     ▼                     ▼
 ┌─────────────┐   audits/agent-audit-*.md   audit-lake status     ┌──────────────┐
 │ ▓ ask-lake  │      (free, instant)          (free, instant)     │ 👤 "Start a  │
 │ own loop,   │                                                   │ fresh audit? │
 │ SELECT-only │                                                   │   [y/N]"     │
 │ guardrails, │                                                   └──┬────────┬──┘
 │ own logs    │                                                 y  │        │ N
 └──────┬──────┘                                                    ▼        ▼
        │                                                   ┌─────────────┐  "declined:
        ▼                                                   │▓ audit-lake │   use the
   watsonx.data                                             │     run     │   latest
                                                            └──────┬──────┘   report"
                                                                   ▼
                                                              watsonx.data
     │                      │                      │                   │
     └──────────────────────┴───────┬──────────────┴───────────────────┘
                                    │ back to the loop: answers only, clipped to 12k chars,
                                    │ treated as DATA (never as instructions)
                                    ▼
                    🧭 Answer: facts with sources, caveats kept, analyst refs
                    — orchestrator 9.8k in · specialists 217k in · refs 8cdec1

 ═══════════════════ the orchestrator stops here; it can't approve anything ═══════
                                    ▼
                    👤 YOU → `audit-lake review` → approve / reject
```

What the first live run did (2026-11-16):

```
 turn 1  █ THINK  "infra part → audit report; data part → analyst; independent"
         █ ACT    read_latest_audit  ║  ask_analyst("count tables per catalog…")   ← parallel
                        │            ║         │
                        │ instant    ║         ▓ 12 steps, ~11 min, 217k tokens
                        ▼            ║         ▼
 turn 2  █ OBSERVE report (1.3h old) + analyst answer (~1k tokens)
         █ THINK  "presto4 at 21.8h is probably past 24h by now; flag it"
         █ DONE   → briefing                 (chose NOT to call run_audit: report fresh enough)
```

Compared with the agentic workflow (`../agentic-workflow`):

```
   WORKFLOW (lake-brief)                     ORCHESTRATOR (ask-team)

   ░ → ░ → ▓▓ → █ → ░ → ░                         ┌──► ▓ analyst
   a straight line; code sets the order       █ ◄─┼──► ░ audit report
                                              loop├──► ░ audit status
                                                  └──► ▓ auditor (👤 y/N)
                                              the model picks the order,
                                              how many calls, and when to stop
```

## How it works

| Tool the coordinator sees | What actually happens | Cost / gate |
|---|---|---|
| `ask_analyst(question)` | starts `ask-lake "<question>"` as a separate process; only the final answer comes back | full agent run; max 2 in parallel, 6 per question |
| `read_latest_audit` | reads the newest `audits/agent-audit-*.md` | free |
| `audit_status` | runs `audit-lake status` (no model) | free |
| `run_audit(why)` | runs `audit-lake run` | 5-25 min, **you must type `y`** |

There is no approve tool. Pausing anything still goes through `audit-lake review`.

## Ideas it demonstrates
- **Delegation**: the coordinator splits the question and sends independent parts in parallel.
- **Context isolation**: a specialist may read 200k tokens; the coordinator sees ~1k of answer.
- **Least privilege**: each specialist is its own process with its own allowlist and guardrails.
- **Trust boundaries**: specialist output is treated as data; costly actions need a human, in code.

## Tracing
The footer shows the coordinator's ref, the token split (coordinator vs specialists) and the analyst
refs. `grep <ref> ../analyst/logs/*.jsonl` shows every SQL statement behind an answer.

Tune with `ORCH_MAX_STEPS` (15), `ORCH_MAX_DELEGATIONS` (6), `ORCH_MAX_PARALLEL` (2),
`ORCH_ANALYST_TIMEOUT_S` (900), `ORCH_AUDIT_TIMEOUT_S` (1800).
