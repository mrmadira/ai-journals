# Lakehouse Analyst Agent

Answers plain-English questions about data in watsonx.data. It finds the right catalog,
schema and tables by itself, writes SQL, recovers from errors, and sanity-checks its results.

## Setup (one time)

**1. Prerequisites:** [uv](https://docs.astral.sh/uv/), an AWS account with Claude enabled on
Amazon Bedrock, and an IBM watsonx.data instance.

**2. Create `.mcp.json` at the repo root** (two folders above `agent.py`). It tells the agent
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
cd agents/analyst
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

Without the wrapper: `cd agents/analyst && uv run python agent.py "..."`.

Inside Claude Code, prefix with `!` (`! ask-lake "..."`), or use the `@ask-lake` subagent
(`.claude/agents/ask-lake.md`), which relays your question to this agent.

**Output:** in a terminal, colors are shown and the Markdown answer is rendered. When the output
is captured (a pipe, a log, or Claude Code's `!`), it's plain text. Set `NO_COLOR=1` to force plain.

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
