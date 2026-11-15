"""Offline tests for the guardrails - no network, no model. Run: uv run python -m pytest -q"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import agent  # noqa: E402

PLAN = '{"2": {"plan": {"name": "ScanProject", "estimates": [{"outputRowCount" : 5.999989709E9}]}}}'


def test_estimate_from_mcp_wrapped_plan():
    # What the MCP server really returns: the plan is a JSON *string* inside JSON (escaped quotes).
    wrapped = json.dumps({"engine_id": "x", "plan": PLAN})
    assert '\\"outputRowCount\\"' in wrapped
    assert agent.max_row_estimate(wrapped) == 5.999989709e9


def test_estimate_from_raw_plan_and_missing_stats():
    assert agent.max_row_estimate(PLAN) == 5.999989709e9
    assert agent.max_row_estimate('{"plan": "{\\"outputRowCount\\" : \\"NaN\\"}"}') is None


class FakeMCP:
    """Stands in for the MCP server: returns a canned EXPLAIN plan."""
    def __init__(self, rows):
        self.rows = rows

    async def call_tool(self, name, args, read_timeout_seconds=None):
        text = json.dumps({"plan": json.dumps({"estimates": [{"outputRowCount": self.rows}]})})
        content = [type("C", (), {"type": "text", "text": text})()]
        return type("R", (), {"content": content, "is_error": False})()


class NullLog:
    def write(self, *a, **k):
        pass


def check(sql, rows=1e6):
    tools = agent.Tools(FakeMCP(rows), NullLog())
    return asyncio.run(tools.check_guardrails("watsonxdata-execute-select", {"sql": sql, "engine_id": "e"}))


def test_scan_budget_refuses_huge_and_allows_small():
    assert "scan budget" in check("SELECT 1 FROM c.s.big", rows=6e9)
    assert check("SELECT 1 FROM c.s.small", rows=1e6) is None


def test_select_only_and_allowlist():
    assert "single SELECT" in check("DELETE FROM c.s.t")
    assert "single SELECT" in check("SELECT 1; DROP TABLE c.s.t")
    tools = agent.Tools(FakeMCP(1), NullLog())
    assert "not permitted" in asyncio.run(tools.check_guardrails("watsonxdata-pause-presto-engine", {}))
