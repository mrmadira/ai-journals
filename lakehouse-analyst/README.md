# Lakehouse Analyst Agent (step 1)

Answers plain-English questions about data in watsonx.data. It finds the right catalog,
schema and tables by itself, writes SQL, recovers from errors, and sanity-checks its results.

## Run it

```bash
cd agents/analyst
uv run python agent.py "What tables are in the tpchdbiceberg schema?"   # one question
uv run python agent.py                                                   # interactive (follow-ups remember context)
```

Required environment variables (outside Claude Code, export them in your shell):

| Variable | Used for |
|---|---|
| `AWS_BEARER_TOKEN_BEDROCK`, `AWS_REGION` | Calling Claude on Amazon Bedrock. Claude Code sets these from `~/.claude/settings.json`; your own terminal doesn't. |
| `WXD_AUTH` | Referenced as `${WXD_AUTH}` in `../../.mcp.json` for the watsonx.data MCP server |
| `WXD_MCP_CONFIG` *(optional)* | Path to a different `.mcp.json` |

## How it works: the concepts

| Concept | Where in `agent.py` | What it means |
|---|---|---|
| **Agent loop** | `answer()` | Call Claude → if it asks for tools, run them → send the results back → repeat until `end_turn`. The model plans; the code executes. |
| **Tool use** | `tool_use` / `tool_result` blocks | Claude never runs anything itself. It *asks* for a call, and our code runs it. |
| **MCP client** | `main()`, `run_tool()` | The tool list is fetched live from the MCP server, so new server tools need no code change. |
| **Generic by design** | `SYSTEM_PROMPT` | It describes *how* to explore and names no tables, so the same agent handles TPC-H, NBA, or anything else. |
| **Guardrails in code** | `ALLOWED_TOOLS`, `check_guardrails()` | Read-only allowlist, SELECT-only, row cap, `MAX_STEPS`. These are enforced, not just requested in the prompt. |
| **Errors as observations** | `is_error: True` results | A failed tool call goes back to the model, which adapts instead of crashing. |
| **Parallel tool calls** | `asyncio.gather` | Independent calls run concurrently, and their results return in one message. |
| **Visible reasoning** | `thinking.display = "summarized"` | The 💭 lines summarize the model's reasoning at each step. |
| **Memory** | `messages` list | Interactive mode keeps the conversation, so "now break that down by nation" works. |
| **Refusal fallback** | `BetaRefusalFallbackMiddleware` | If Opus 5.5 declines a request, it retries on Opus 4.8 (Bedrock has no server-side fallback). |

## Known limits (good next exercises)

- **Query cost.** It can still write an expensive join over the 6B-row `lineitem` table. A next guardrail would be to run `explain-query` first and refuse plans above a size budget.
- **Tokens.** Every step resends the full history (prompt caching makes repeats cheaper, but it isn't free).
