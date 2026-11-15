"""Live guardrail check against the real MCP server - no model calls, so it's free to run.

    uv run python evals/check_guardrails.py [engine_id]

Run it after changing guardrail code or upgrading the MCP server/library.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx2  # noqa: E402
from mcp import ClientSession  # noqa: E402
from mcp.client.streamable_http import streamable_http_client  # noqa: E402

import agent  # noqa: E402

ENGINE = sys.argv[1] if len(sys.argv) > 1 else "presto58"
CASES = [  # (label, sql, should_run)
    ("full 6B-row lineitem scan", "SELECT l_returnflag, COUNT(*) FROM icedemocatalog.tpchdbiceberg.lineitem GROUP BY l_returnflag", False),
    ("10M-row supplier aggregate", "SELECT s_nationkey, COUNT(*) FROM icedemocatalog.tpchdbiceberg.supplier GROUP BY s_nationkey", True),
    ("unqualified table name", "SELECT COUNT(*) FROM supplier", False),
    ("non-SELECT statement", "DELETE FROM icedemocatalog.tpchdbiceberg.nation", False),
    ("stacked statements", "SELECT 1; DROP TABLE icedemocatalog.tpchdbiceberg.nation", False),
]


async def main() -> int:
    url, headers = agent.load_mcp_server()
    async with httpx2.AsyncClient(headers=headers, timeout=150) as http, \
            streamable_http_client(url, http_client=http) as (r, w), ClientSession(r, w) as mcp:
        await mcp.initialize()
        tools = agent.Tools(mcp, agent.AuditLog())
        tools.ref = "guardrail-check"
        failures = 0
        for label, sql, should_run in CASES:
            verdict = await tools.check_guardrails("watsonxdata-execute-select", {"sql": sql, "engine_id": ENGINE})
            ran = verdict is None
            ok = ran == should_run
            failures += not ok
            print(f"{'PASS' if ok else 'FAIL'}  {label:28} -> {'allowed' if ran else verdict[:90]}")
        return failures


sys.exit(asyncio.run(main()))
