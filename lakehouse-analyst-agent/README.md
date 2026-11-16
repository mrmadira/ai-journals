# Lakehouse Analyst Agent

Answers plain-English questions about data in watsonx.data. It finds the right catalog,
schema and tables by itself, writes SQL, recovers from errors, and sanity-checks its results.

> **Using the analyst, not developing it?** Read [USER_GUIDE.md](USER_GUIDE.md) instead.
> This README is for whoever runs and maintains the agent.

## Setup (one time)

**1. Prerequisites:** [uv](https://docs.astral.sh/uv/), an AWS account with Claude enabled on
Amazon Bedrock, and an IBM watsonx.data instance.

**2. Create `.mcp.json` at the repo root** (copy `.mcp.json.example`, one folder above `agent.py`). It tells the agent
how to reach the watsonx.data MCP server. Keep secrets as `${VAR}` placeholders. They're filled
in from environment variables when the agent runs, so the file never holds a real token.
**Don't commit this file.**

```json
{
  "mcpServers": {
    "watsonx.data-mcp-server": {
      "type": "http",
      "url": "https://<region>.lakehouse.cloud.ibm.com/api/v1/watsonxdata/mcp",
      "headers": {
        "authorization": "Basic ${WXD_AUTH}",
        "authinstanceid": "${WXD_INSTANCE_ID}"
      }
    }
  }
}
```

**3. Install the `ask-lake` command** (optional). This puts it on your PATH so it runs from
any directory:

```bash
cd lakehouse-analyst-agent
chmod +x ask-lake
ln -s "$PWD/ask-lake" ~/.local/bin/ask-lake   # ~/.local/bin must be on your PATH
```

**4. Set environment variables:**

| Variable | Used for |
|---|---|
| `AWS_BEARER_TOKEN_BEDROCK`, `AWS_REGION` | Calling Claude on Amazon Bedrock. `ask-lake` reads these from `~/.claude/settings.json` (the `env` block) if they aren't exported. |
| `WXD_AUTH`, `WXD_INSTANCE_ID` | Substituted into the `${...}` placeholders in `.mcp.json` |
| `WXD_MCP_CONFIG` *(optional)* | Path to an `.mcp.json` somewhere other than the repo root |

## Run it

```bash
ask-lake "What tables are in the tpchdbiceberg schema?"   # one question
ask-lake                                                  # interactive (follow-ups remember context)
```

Without the wrapper: `cd lakehouse-analyst-agent && uv run python agent.py "..."`.

Inside Claude Code, prefix with `!` (`! ask-lake "..."`), or define a small subagent (for example
`.claude/agents/ask-lake.md`) that relays your question to this agent.

**Output:** in a terminal, colors are shown and the Markdown answer is rendered. When the output
is captured (a pipe, a log, or Claude Code's `!`), it's plain text. Set `NO_COLOR=1` to force plain.

## How it works: the concepts

| Concept | Where in `agent.py` | What it means |
|---|---|---|
| **Agent loop** | `answer()` | Call Claude → if it asks for tools, run them → send the results back → repeat until `end_turn`. The model plans; the code executes. |
| **Tool use** | `tool_use` / `tool_result` blocks | Claude never runs anything itself. It *asks* for a call, and our code runs it. |
| **MCP client** | `main()`, `run_tool()` | The tool list is fetched live from the MCP server, so new server tools need no code change. |
| **Generic by design** | `BASE_PROMPT` | It describes *how* to explore and names no tables, so the same agent handles TPC-H, NBA, or anything else. |
| **Guardrails in code** | `ALLOWED_TOOLS`, `Tools.check_guardrails()` | Read-only allowlist, SELECT-only, scan budget, timeouts, row cap, `MAX_STEPS`. These are enforced, not just requested in the prompt. |
| **Local tools** | `record_finding` | A tool implemented in this file, not on the MCP server. This is how you give an agent abilities the server lacks. |
| **Errors as observations** | `is_error: True` results | A failed tool call goes back to the model, which adapts instead of crashing. |
| **Parallel tool calls** | `asyncio.gather` | Independent calls run concurrently, and their results return in one message. |
| **Visible reasoning** | `thinking.display = "summarized"` | The 💭 lines summarize the model's reasoning at each step. |
| **Short-term memory** | `messages` list | Interactive mode keeps the conversation, so "now break that down by nation" works. |
| **Long-term memory** | `context.md` + `notes.md` | Curated knowledge and the agent's own findings, loaded into every run. |
| **Refusal fallback** | `BetaRefusalFallbackMiddleware` | If Opus 5.5 declines a request, it retries on Opus 4.8 (Bedrock has no server-side fallback). |

## Provider-side features (what users get without asking)

| Feature | How it works | What you, the provider, do |
|---|---|---|
| **Context file** | `context.md` is loaded into the system prompt on every run: canonical datasets, metric definitions, known data issues, conventions, physical layout. | Keep it current. It's the biggest lever on answer quality. See **[CONTEXT_GUIDE.md](CONTEXT_GUIDE.md)**. |
| **Bootstrap profiling** | `ask-lake --profile <catalog>.<schema>` profiles a schema from metadata only (no scans) and writes a **draft** context section to `drafts/`. | Review the draft and copy verified lines into `context.md`. |
| **Agent memory** | The agent calls `record_finding` when it *confirms* a data issue, which appends to `notes.md`. Future runs see those notes, marked **unreviewed**. | Review `notes.md` regularly: move confirmed facts into `context.md` and delete the rest. (Notes are unreviewed on purpose: injected text could try to plant false "facts".) |
| **Asks when ambiguous** | If a question could mean several datasets or metrics, the agent asks instead of guessing. | Add canonical choices to `context.md` so it needs to ask less. |
| **Scan budget** | Every query is `EXPLAIN`ed first and refused if the plan's largest row estimate exceeds `ANALYST_MAX_SCAN_ROWS`. The agent gets told how to narrow it. If the EXPLAIN itself fails, the query is refused (fail closed). | Tune the budget per engine. Note: tables or filters without statistics estimate as unknown and are allowed, so the timeout is the backstop. |
| **No table dumps** | "Show me all rows" gets an immediate reply offering a count, a sample, summary stats or a filtered subset, with no exploring and no queries. In code, any query without a filter or aggregation is capped to a `PREVIEW_ROWS` (20) row preview, whatever the model asks for. | — |
| **Transient-error retry** | A server error on a (read-only) tool call is retried once after 2s. Timeouts aren't retried (a slow query is slow again). Error messages say which one happened. | If retries show up often in the logs (`"event": "retry"`), raise it with the MCP server's owners. |
| **Timeouts + concurrency cap** | Each tool call times out after `ANALYST_QUERY_TIMEOUT_S`, and at most `ANALYST_MAX_PARALLEL_QUERIES` queries run at once. | For hard limits, also set query limits on the engine itself (a client timeout doesn't always cancel the server-side query). |
| **Performance rules** | The prompt asks for one-pass checks, `approx_distinct` on big tables, partition filters, window functions instead of self-joins, and lighter follow-ups after slow queries. | Put **physical layout** (partition columns, metadata tables like `"table$partitions"`) in `context.md`. On this lakehouse that turned a ~60s count into a 3s metadata lookup. |
| **Query timing** | Each query's `execution_time_ms` is logged (`"event": "query_time"`), shown on its result line, and totalled in the answer footer. | Find slow queries: `jq 'select(.event=="query_time" and .ms > 30000)' logs/*.jsonl`. Evals can fail on `max_engine_seconds` / `max_queries`. |
| **Fixed answer format** | Answer → How I got it (with the SQL) → Checks → Caveats. | — |
| **Audit log + ref IDs** | Every question, tool call, SQL statement, scan estimate and answer goes to `logs/YYYY-MM-DD.jsonl`. Each answer ends with `ref abc123` and token usage. | Find any answer's trail: `jq 'select(.ref=="abc123")' logs/*.jsonl`. Logs are git-ignored. |
| **Prompt-injection rules** | Tool output is declared to be data, never instructions. The `@ask-lake` relay is told the same. | The real backstop is a **read-only** watsonx.data credential: use a service ID that can't write. |
| **Friendly errors** | Expired or missing tokens produce one actionable line instead of a stack trace (`ANALYST_DEBUG=1` for the full traceback). | — |

Tuning knobs (environment variables): `ANALYST_MAX_SCAN_ROWS` (default 1e9),
`ANALYST_QUERY_TIMEOUT_S` (120), `ANALYST_MAX_PARALLEL_QUERIES` (2), `ANALYST_MAX_STEPS` (25),
`ANALYST_RETRY_BACKOFF_S` (5).

## Testing

```bash
uv run python -m pytest -q tests          # offline guardrail unit tests: no network, free
uv run python evals/check_guardrails.py   # live guardrail check against the MCP server: no model calls, free
uv run python evals/run_evals.py          # eval set: real questions with known answers (uses the model, costs tokens)
```

Run the evals after every change to `BASE_PROMPT`, `context.md`, or the model, and compare the
pass rate (history is in `evals/results.jsonl`). Add a question whenever a user reports a bad
answer.

## Known limits

- **Unknown estimates pass the scan budget.** Without table statistics, `EXPLAIN` can't estimate rows. Running `ANALYZE` on big tables makes the budget effective.
- **Tokens.** Every step resends the full history. Prompt caching makes repeats much cheaper (see the cache-hit numbers in the footer), but it isn't free.
- **Answers are still model output.** The checks reduce mistakes; they don't eliminate them. Anything that drives a decision still gets a human look.
