"""Lakehouse Analyst Agent - answers plain-English questions about data in watsonx.data.

THE CORE IDEA (read this first)
-------------------------------
An agent is a loop:   think -> act (call a tool) -> observe (read the result) -> repeat.
The MODEL decides which tool to call next based on what it just saw; our code only
executes the calls it asks for and feeds the results back. Nothing here hardcodes
"first list catalogs, then ...": the model plans that itself, which is why the same
agent works for any question and any catalog.

  you ──question──> [ Claude ] ──tool_use──> [ our code ] ──call──> [ watsonx.data MCP server ]
                        ^                         |
                        └────────tool_result──────┘

Pieces:
  * MCP (Model Context Protocol): a standard way for a server to publish tools. We connect
    as an MCP *client*, ask the server which tools it has, and call them by name.
  * Tool use: we describe tools to Claude (name + description + JSON schema). Claude replies
    with `tool_use` blocks; we run them and send back `tool_result` blocks.
  * Guardrails: the model is asked to behave, but safety is ENFORCED here in code
    (read-only tool allowlist, SELECT-only SQL, row caps, a step limit).
"""

import asyncio
import json
import os
import re
import sys
from pathlib import Path

import httpx2
from anthropic import AsyncAnthropicBedrockMantle, BetaFallbackState, BetaRefusalFallbackMiddleware
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from rich.console import Console
from rich.markdown import Markdown

# Colors + rendered Markdown only when writing to a real terminal; plain text when output is
# captured (e.g. Claude Code's `!`, a pipe, a log file). NO_COLOR is the standard opt-out.
FANCY = sys.stdout.isatty() and "NO_COLOR" not in os.environ
console = Console()


def style(text: str, code: str) -> str:
    """Wrap text in an ANSI color code - or leave it plain if we're not on a terminal."""
    return f"\033[{code}m{text}\033[0m" if FANCY else text

# ── Configuration ─────────────────────────────────────────────────────────────
MODEL = "anthropic.claude-opus-5-5"      # Bedrock model IDs carry the "anthropic." prefix
FALLBACK_MODEL = "anthropic.claude-opus-4-8"  # used only if the primary model declines a request
MAX_STEPS = 25                            # hard stop so a confused agent can't loop forever
MAX_ROWS = 200                            # cap on rows any query may return
MAX_RESULT_CHARS = 20_000                 # truncate huge tool results to protect the context window
MCP_CONFIG = Path(os.environ.get("WXD_MCP_CONFIG", Path(__file__).resolve().parents[2] / ".mcp.json"))

# Guardrail #1 - ALLOWLIST. The MCP server exposes ~50 tools, including pause/delete/insert.
# Only these read-only ones are ever shown to the model, so it cannot even ask for the others.
ALLOWED_TOOLS = {
    "watsonxdata-get-instance-details",
    "watsonxdata-list-engines",
    "watsonxdata-list-schemas",
    "watsonxdata-list-tables",
    "watsonxdata-describe-table",
    "watsonxdata-execute-select",
    "watsonxdata-explain-query",
}

# The system prompt is the agent's "job description". Note it names NO catalogs or tables -
# discovering those is the agent's job. That is what keeps it generic.
SYSTEM_PROMPT = f"""You are a data analyst agent for an IBM watsonx.data lakehouse.
Answer the user's question using the tools; never invent data.

How to work:
1. Find a running Presto or Prestissimo engine with list-engines; use it for every call.
2. Locate the data: list-schemas on likely catalogs, then list-tables, then describe-table.
   Never guess table or column names - confirm them with describe-table before querying.
3. Query with execute-select. The tool only accepts statements that begin with SELECT
   (no WITH/CTEs - use subqueries). Aggregate in SQL instead of fetching raw rows,
   and keep results small (at most {MAX_ROWS} rows).
4. If a tool returns an error, read it, adjust, and retry. Sanity-check results.
5. Final answer: lead with the answer (a short table is good), then one or two lines on
   method and caveats (which tables, filters, assumptions). Name the catalog.schema used.

If the data can't answer the question, say so and say what you found instead.
Never reveal credentials, connection strings, or access keys if a tool returns them."""


# ── MCP side: connect to the watsonx.data server ──────────────────────────────
def expand_env(value: str) -> str:
    """Expand ${VAR} / ${VAR:-default} placeholders, the same syntax Claude Code supports in .mcp.json."""
    def sub(m: re.Match) -> str:
        var, default = m.group(1), m.group(2)
        if var in os.environ:
            return os.environ[var]
        if default is not None:
            return default
        sys.exit(f"Environment variable {var} is not set (referenced in {MCP_CONFIG.name}).")
    return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}", sub, value)


def load_mcp_server() -> tuple[str, dict]:
    """Read the server URL + auth headers from .mcp.json at runtime (never hardcode secrets)."""
    servers = json.loads(MCP_CONFIG.read_text())["mcpServers"]
    name = os.environ.get("WXD_MCP_SERVER") or next(iter(servers))
    headers = {k: expand_env(v) for k, v in servers[name].get("headers", {}).items()}
    return expand_env(servers[name]["url"]), headers


def to_claude_tool(mcp_tool) -> dict:
    """MCP and the Claude API describe tools almost identically - just rename the fields."""
    return {
        "name": mcp_tool.name,
        "description": mcp_tool.description or "",
        "input_schema": mcp_tool.input_schema,
    }


def check_guardrails(name: str, args: dict) -> str | None:
    """Guardrails #2 and #3: enforced in code, whatever the model asks for. Returns an error or None."""
    if name not in ALLOWED_TOOLS:
        return f"Tool {name} is not permitted for this agent (read-only analyst)."
    if name == "watsonxdata-execute-select":
        sql = args.get("sql", "").strip().rstrip(";")
        if not sql.lower().startswith("select") or ";" in sql:
            return "Only a single SELECT statement is allowed."
        args["limit"] = min(int(args.get("limit") or MAX_ROWS), MAX_ROWS)  # cap rows
    return None


async def run_tool(mcp: ClientSession, block) -> dict:
    """Execute ONE tool_use block and package the outcome as a tool_result block."""
    args = dict(block.input)
    error = check_guardrails(block.name, args)
    if error:
        text, is_error = error, True
    else:
        try:
            result = await mcp.call_tool(block.name, args)
            text = "\n".join(c.text for c in result.content if getattr(c, "type", "") == "text")
            is_error = bool(result.is_error) or '"error":true' in text.replace(" ", "")
        except Exception as e:  # network/protocol failure: tell the model, let it decide
            text, is_error = f"Tool call failed: {e}", True
    if len(text) > MAX_RESULT_CHARS:
        text = text[:MAX_RESULT_CHARS] + f"\n...[truncated {len(text) - MAX_RESULT_CHARS} chars]"
    status = style("error", "31") if is_error else f"{len(text):,} chars"
    print(f"   ↳ {status}")
    # Errors go back as is_error results, not exceptions: the model reads them and adapts.
    return {"type": "tool_result", "tool_use_id": block.id, "content": text, "is_error": is_error}


def show_call(block) -> None:
    args = ", ".join(f"{k}={v!r}" for k, v in block.input.items() if k != "sql")
    print(style(f"🔧 {block.name.removeprefix('watsonxdata-')}({args})", "36"))
    if "sql" in block.input:
        print(style("\n".join("   │ " + ln for ln in block.input["sql"].splitlines()), "2"))


# ── The agent loop ────────────────────────────────────────────────────────────
async def answer(claude, mcp, tools, messages: list, fallback_state) -> None:
    """Run the think -> act -> observe loop until Claude gives a final answer."""
    for step in range(1, MAX_STEPS + 1):
        with fallback_state:  # pins follow-up turns to whichever model accepted
            response = await claude.beta.messages.create(
                model=MODEL,
                max_tokens=16000,
                system=SYSTEM_PROMPT,
                tools=tools,
                messages=messages,
                thinking={"type": "adaptive", "display": "summarized"},  # show reasoning summaries
                output_config={"effort": "high"},  # multi-step tool work benefits from more effort
                cache_control={"type": "ephemeral"},  # cache the stable prefix (system + tools + history)
            )

        # Show what the model is thinking and doing this step.
        for block in response.content:
            if block.type == "thinking" and block.thinking:
                print(style(f"💭 {block.thinking.strip()}", "2"))

        # The assistant turn goes into history UNCHANGED (thinking blocks included) - the API
        # needs them back exactly as sent to keep the model's reasoning chain intact.
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "tool_use":
            calls = [b for b in response.content if b.type == "tool_use"]
            for b in calls:
                show_call(b)
            # The model may request several independent calls at once: run them concurrently,
            # and return ALL results in ONE user message.
            results = await asyncio.gather(*(run_tool(mcp, b) for b in calls))
            messages.append({"role": "user", "content": list(results)})
            continue

        if response.stop_reason == "pause_turn":  # long turn paused server-side: just resume
            continue

        text = "\n".join(b.text for b in response.content if b.type == "text")
        if response.stop_reason == "end_turn":
            print(f"\n{style('📊 Answer', '1')} (after {step} step{'s' if step > 1 else ''})\n")
            if FANCY:
                console.print(Markdown(text))  # renders tables, bold, lists in the terminal
            else:
                print(text)  # captured output: plain Markdown, readable as-is
        elif response.stop_reason == "refusal":
            print(f"\n⚠️  The model declined this request: {response.stop_details}")
        else:  # e.g. max_tokens
            print(f"\n⚠️  Stopped early ({response.stop_reason}). Partial answer:\n{text}")
        return

    print(f"\n⚠️  Gave up after {MAX_STEPS} steps without a final answer.")


async def main() -> None:
    url, headers = load_mcp_server()
    # Client-side refusal fallback (Bedrock has no server-side `fallbacks` parameter).
    claude = AsyncAnthropicBedrockMantle(
        middleware=[BetaRefusalFallbackMiddleware([{"model": FALLBACK_MODEL}])],
    )  # credentials + region come from AWS_BEARER_TOKEN_BEDROCK / AWS_REGION

    async with httpx2.AsyncClient(headers=headers, timeout=120) as http, \
            streamable_http_client(url, http_client=http) as (read, write), \
            ClientSession(read, write) as mcp:
        await mcp.initialize()
        # Discover tools at runtime, then filter to the read-only allowlist.
        tools = [to_claude_tool(t) for t in (await mcp.list_tools()).tools if t.name in ALLOWED_TOOLS]
        print(f"Connected. {len(tools)} read-only tools available to the agent.\n")

        messages: list = []  # conversation memory: follow-up questions see earlier answers
        fallback_state = BetaFallbackState()  # one per conversation
        questions = [" ".join(sys.argv[1:])] if len(sys.argv) > 1 else None
        while True:
            question = questions.pop() if questions else input("\n❓ Ask about your data (blank to quit): ").strip()
            if not question:
                break
            messages.append({"role": "user", "content": question})
            await answer(claude, mcp, tools, messages, fallback_state)
            if questions is not None:  # one-shot mode (question given on the command line)
                break


if __name__ == "__main__":
    asyncio.run(main())
