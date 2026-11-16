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


def test_raw_row_dumps_get_preview_cap():
    assert agent.is_raw_row_dump("SELECT * FROM c.s.customer")
    assert agent.is_raw_row_dump("SELECT c_name, c_acctbal FROM c.s.customer ORDER BY c_acctbal DESC")
    assert agent.is_raw_row_dump("SELECT * FROM c.s.t WHERE_ISH")  # no real WHERE keyword
    assert not agent.is_raw_row_dump("SELECT * FROM c.s.customer WHERE c_nationkey = 3")
    assert not agent.is_raw_row_dump("SELECT c_nationkey, COUNT(*) FROM c.s.customer GROUP BY 1")
    assert agent.is_raw_row_dump("SELECT 'where' AS x FROM c.s.t")  # keyword only inside a string
    args = {"sql": "SELECT * FROM c.s.customer", "engine_id": "e", "limit": 200}
    tools = agent.Tools(FakeMCP(1e6), NullLog())
    assert asyncio.run(tools.check_guardrails("watsonxdata-execute-select", args)) is None
    assert args["limit"] == agent.PREVIEW_ROWS and args["_preview"]


class FlakyMCP(FakeMCP):
    """Fails once with a server error, then succeeds - like the real transient errors we saw."""
    def __init__(self):
        super().__init__(1e6)
        self.calls = 0

    async def call_tool(self, name, args, read_timeout_seconds=None):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("Server returned an error response")
        assert not any(k.startswith("_") for k in args)  # internal flags never reach the server
        return await super().call_tool(name, args, read_timeout_seconds)


def test_transient_server_error_is_retried_once(monkeypatch):
    async def no_sleep(_):
        return None
    monkeypatch.setattr(agent.asyncio, "sleep", no_sleep)
    mcp = FlakyMCP()
    text, is_error = asyncio.run(agent.Tools(mcp, NullLog()).call_mcp("watsonxdata-list-engines", {"_preview": True}))
    assert not is_error and mcp.calls == 2


def test_persistent_server_error_reports_clearly(monkeypatch):
    async def no_sleep(_):
        return None
    monkeypatch.setattr(agent.asyncio, "sleep", no_sleep)

    class BrokenMCP(FakeMCP):
        async def call_tool(self, *a, **k):
            raise RuntimeError("Server returned an error response")
    text, is_error = asyncio.run(agent.Tools(BrokenMCP(1), NullLog()).call_mcp("watsonxdata-list-engines", {}))
    assert is_error and "not a timeout" in text and "failed twice" in text


def test_engine_time_parsed_from_query_result():
    assert agent.engine_time_ms('{"rows": [[1]], "execution_time_ms": 3444}') == 3444
    assert agent.engine_time_ms("not json") is None
