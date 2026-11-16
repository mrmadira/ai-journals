"""Offline tests for the CODE steps of the workflow (no network, no model).
This is what fixing the steps in code buys you: most of the pipeline is testable like any program."""

import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import workflow as w  # noqa: E402

NOW = dt.datetime(2026, 11, 16, 12, 0, tzinfo=dt.timezone.utc)


def prop(pid, status="pending", expires="2026-11-17T00:00:00+00:00", protected=False):
    return {"id": pid, "status": status, "expires_at": expires, "action": "pause_presto_engine",
            "engine_id": f"eng-{pid}", "engine_name": "x", "engine_type": "presto", "protected": protected}


def test_select_keeps_only_pending_unexpired(tmp_path):
    f = tmp_path / "proposals.json"
    f.write_text(json.dumps([prop("a"), prop("b", status="rejected"),
                             prop("c", expires="2026-11-15T00:00:00+00:00"), prop("d")]))
    assert [p["id"] for p in w.step_select(NOW, f)] == ["a", "d"]


def test_select_caps_engine_count(tmp_path, monkeypatch):
    monkeypatch.setattr(w, "MAX_ENGINES", 2)
    f = tmp_path / "proposals.json"
    f.write_text(json.dumps([prop(str(i)) for i in range(5)]))
    assert len(w.step_select(NOW, f)) == 2


def test_validate_removes_invented_and_adds_missing():
    proposals = [prop("a"), prop("b")]
    impact = {"a": {"answer": "ok"}, "b": {"answer": "ok"}}
    brief = {"proposals": [{"proposal_id": "a", "recommendation": "approve", "why": ""},
                           {"proposal_id": "zzz", "recommendation": "approve", "why": "made up"}]}
    issues = w.step_validate(brief, proposals, impact)
    assert {r["proposal_id"] for r in brief["proposals"]} == {"a", "b"}
    assert any("zzz" in i for i in issues) and any("skipped proposal b" in i for i in issues)


def test_validate_blocks_approve_without_assessment():
    brief = {"proposals": [{"proposal_id": "a", "recommendation": "approve", "why": ""}]}
    issues = w.step_validate(brief, [prop("a")], {"a": {"answer": None, "error": "timeout"}})
    assert brief["proposals"][0]["recommendation"] == "investigate" and issues


def test_parse_analyst_extracts_answer_and_ref():
    out = "Connected.\n💭 hmm\n\n📊 Answer\n\nAll fine.\n\n— ref ab12cd · 3 steps · tokens: 1,000 in (x) · 200 out · SQL: 1 query"
    info = w.parse_analyst(out)
    assert info == {"ref": "ab12cd", "tokens_in": 1000, "tokens_out": 200, "answer": "All fine."}
    assert w.parse_analyst("⚠️ Gave up")["answer"] is None


def test_render_sorts_risks_and_marks_protected(tmp_path):
    rep = tmp_path / "agent-audit-x.md"
    rep.write_text("r")
    brief = {"headline": "H", "caveats": [], "risks": [
        {"severity": "low", "title": "L", "detail": "", "source": "audit"},
        {"severity": "high", "title": "Hi", "detail": "", "source": "analyst"}],
        "proposals": [{"proposal_id": "a", "recommendation": "investigate", "why": "w"}]}
    md = w.render(brief, [], rep, [prop("a", protected=True)], {}, "ref1",
                  {"summary_in": 1, "summary_out": 1, "analyst_in": 0, "analyst_out": 0})
    assert md.index("**HIGH**") < md.index("**LOW**") and "🔒" in md
