# ai-journals: agentic AI on IBM watsonx.data

Code from my blog series *Exploring IBM's watsonx.data MCP Server*. It goes from one agent to a
small team, all working against a watsonx.data lakehouse through its MCP server, with Claude on
Amazon Bedrock as the model.

| Folder | Command | Pattern |
|---|---|---|
| [`lakehouse-analyst-agent/`](lakehouse-analyst-agent/) | `ask-lake` | Agent (interactive): answers plain-English questions about the data with read-only SQL |
| [`lakehouse-auditor-agent/`](lakehouse-auditor-agent/) | `audit-lake` | Autonomous agent + human approval: audits the lakehouse, proposes fixes, you approve |
| [`agentic-orchestrator/`](agentic-orchestrator/) | `ask-team` | Multi-agent: a coordinator **model** decides which agent to call, and when |
| [`agentic-workflow/`](agentic-workflow/) | `lake-brief` | Agentic workflow: **code** fixes the steps, agents work inside them |
| [`skills/`](skills/) | | The `lakehouse-audit` skill for Claude Code that the auditor grew out of |

The orchestrator and the workflow don't talk to watsonx.data themselves. They start the analyst
and auditor as separate processes, so those agents' guardrails always apply.

## Setup

1. Install [uv](https://docs.astral.sh/uv/). You need Claude enabled on Amazon Bedrock and an
   IBM watsonx.data instance.
2. Copy the MCP config and fill in your region (secrets stay as `${VAR}` placeholders):
   ```bash
   cp .mcp.json.example .mcp.json      # .mcp.json is gitignored
   ```
3. Export the credentials:
   ```bash
   export AWS_BEARER_TOKEN_BEDROCK=...  AWS_REGION=...       # Claude on Bedrock
   export WXD_AUTH=...  WXD_INSTANCE_ID=...                  # watsonx.data (fills .mcp.json)
   ```
4. Run any agent from its folder, e.g. `./lakehouse-analyst-agent/ask-lake "What tables are in tpch?"`.
   Each folder's README has the details.

## Layout

All four expect to sit side by side in this repo. They share the repo root for:

```
.mcp.json        your MCP config (from .mcp.json.example)
audits/          reports written by the auditor, read by the orchestrator and the workflow
briefs/          briefs written by the workflow
```

These, and every agent's `logs/` and `state/`, describe **your** lakehouse and are gitignored.
Keep them out of any fork you publish.

## Safety, briefly

- Only the analyst and the auditor touch the lakehouse, and each sees only read-only tools. The
  analyst's SQL is SELECT-only, with a scan budget checked by `EXPLAIN`; the auditor's model gets no
  SQL tool at all.
- Nothing pauses, deletes or changes anything without a human approving it (`audit-lake review`).
- Guardrails are enforced in code, not just asked for in prompts. Each agent's README lists them.
