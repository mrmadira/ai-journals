"""Offline tests: no network, no model. Run: uv run python -m pytest -q"""
import asyncio
import datetime as dt
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import auditor  # noqa: E402
import guardrails as g  # noqa: E402
from state import AlreadyRunning, ProposalStore, run_lock  # noqa: E402

PRESTO = {"engine_id": "presto4", "display_name": "starter", "type": "presto", "status": "running", "created_by": "a@x"}
SPARK = {"engine_id": "spark9", "display_name": "donotdelete-demo", "type": "spark", "status": "running", "created_by": "b@x"}


def idle_row(n_work=5, idle_s=7200 * 3, n_active=0, n=50):
    return {"n_queries": n, "n_active": n_active, "n_work": n_work,
            "last_work_end": "2026-11-15 09:41:16" if n_work else None,
            "last_any_end": "2026-11-15 13:02:40", "oldest_in_history": "2026-11-15 07:00:00",
            "now": "2026-11-15 13:03:11", "tz": "UTC", "idle_work_s": idle_s if n_work else None}


# ── Tool surface ──
def test_model_never_sees_write_or_sql_tools():
    forbidden = {"watsonxdata-execute-select", "watsonxdata-execute-insert", "watsonxdata-execute-update",
                 "watsonxdata-pause-presto-engine", "watsonxdata-pause-spark-engine", "watsonxdata-create-schema",
                 "watsonxdata-rename-table", "watsonxdata-scale-presto-engine"}
    assert not (g.READ_ONLY_TOOLS & forbidden)
    assert all(not t.startswith(("watsonxdata-pause", "watsonxdata-resume", "watsonxdata-create",
                                 "watsonxdata-update", "watsonxdata-execute")) for t in g.READ_ONLY_TOOLS)


def test_unlisted_tool_is_refused_by_executor():
    tools = auditor.Tools(lake=None, log=NullLog(), store=None, snapshot={})
    block = type("B", (), {"name": "watsonxdata-pause-presto-engine", "input": {"engine_id": "presto4"}, "id": "t1"})()
    out = asyncio.run(tools.run(block))
    assert out["is_error"] and "not permitted" in out["content"]


# ── Redaction ──
def test_redacts_secret_keys_with_stable_fingerprint_and_keeps_flags():
    reg = {"registrations": [
        {"id": "a", "properties": {"secret_key": "s3cr3t-value", "access_key": "AKIAABCDEFGHIJKLMNOP", "encrypt": True,
                                   "auth_mode": "hmac", "endpoint": "https://s3.example.com"}},
        {"id": "b", "connection": {"password": "s3cr3t-value", "user": "bob"}},
    ]}
    out = json.loads(g.redact(json.dumps(reg)))
    a, b = out["registrations"][0]["properties"], out["registrations"][1]["connection"]
    assert "s3cr3t" not in json.dumps(out) and "AKIAABCD" not in json.dumps(out)
    assert a["secret_key"] == b["password"]           # same value -> same fp: shared-credential check still works
    assert a["encrypt"] is True and a["auth_mode"] == "hmac" and a["endpoint"].startswith("https://")
    assert b["user"] == "bob"


def test_redacts_secrets_in_free_text():
    txt = 'error: auth "Basic dXNlcjpwYXNzd29yZDEyMw==" failed for AKIAABCDEFGHIJKLMNOP; jdbc:pg://h/db?password=hunter2'
    out = g.redact_text(txt)
    assert "dXNlcjpw" not in out and "AKIAABCD" not in out and "hunter2" not in out
    assert out.count("[REDACTED fp=") == 3


NOW = dt.datetime(2026, 11, 15, 13, 0, tzinfo=dt.timezone.utc)


def spark_idle(active=0, ended="2026-11-09T10:35:23.363Z"):
    apps = [{"state": "finished", "end_time": ended, "finish_time": ended},
            {"state": "failed", "failed_time": "2026-10-28T11:40:39.350Z"}] if ended else []
    return g.interpret_spark_idle(active, apps, NOW)


# ── Idle interpretation ──
def test_spark_idle_from_last_app_end():
    m = spark_idle()
    assert m["idle_hours"] == 146.41 and m["confidence"] == "high" and m["last_app_end"].startswith("2026-11-09T10:35")


def test_spark_failed_app_counts_as_last_activity_and_no_history_is_unknown():
    m = g.interpret_spark_idle(0, [{"state": "failed", "failed_time": "2026-11-15T12:00:00Z"}], NOW)
    assert m["idle_hours"] == 1.0
    assert spark_idle(ended=None)["idle_hours"] is None and spark_idle(ended=None)["confidence"] == "low"
    assert spark_idle(active=2)["idle_hours"] == 0.0



def test_idle_high_confidence():
    m = g.interpret_idle(idle_row(idle_s=12114))
    assert m["idle_hours"] == 3.37 and m["confidence"] == "high"


def test_empty_history_is_unknown_not_forever():
    m = g.interpret_idle(idle_row(n_work=0))
    assert m["idle_hours"] is None and m["confidence"] == "low" and "UNKNOWN" in m["note"]


def test_running_queries_mean_not_idle():
    assert g.interpret_idle(idle_row(n_active=2))["idle_hours"] == 0.0


def test_idle_sql_excludes_itself_and_metadata():
    assert "NOT LIKE '%system.runtime.queries%'" in g.IDLE_SQL
    assert "show|describe|explain" in g.IDLE_SQL
    assert g.IDLE_SQL.lstrip().upper().startswith("SELECT")


# ── Proposal validation ──
def test_check_proposal_rules():
    hi = g.interpret_idle(idle_row(idle_s=5 * 3600))
    assert g.check_proposal("pause_presto_engine", PRESTO, hi, 2) == (None, "high")
    assert "below" in g.check_proposal("pause_presto_engine", PRESTO, g.interpret_idle(idle_row(idle_s=600)), 2)[0]
    assert "needs 'spark'" in g.check_proposal("pause_spark_engine", PRESTO, hi, 2)[0]
    assert "not running" in g.check_proposal("pause_presto_engine", {**PRESTO, "status": "paused"}, hi, 2)[0]
    assert "Unknown action" in g.check_proposal("delete_engine", PRESTO, hi, 2)[0]
    assert "No such engine" in g.check_proposal("pause_presto_engine", None, hi, 2)[0]
    assert g.check_proposal("pause_presto_engine", PRESTO, g.interpret_idle(idle_row(n_work=0)), 2) == (None, "low")
    assert "active application" in g.check_proposal("pause_spark_engine", SPARK, spark_idle(active=1), 2)[0]
    assert "No idle measurement" in g.check_proposal("pause_spark_engine", SPARK, None, 2)[0]
    assert g.check_proposal("pause_spark_engine", SPARK, spark_idle(), 2) == (None, "high")
    assert "below" in g.check_proposal("pause_spark_engine", SPARK, spark_idle(ended="2026-11-15T12:30:00Z"), 2)[0]


def test_protected_names():
    assert g.is_protected(SPARK) and not g.is_protected(PRESTO)
    assert g.is_protected({"engine_id": "x", "display_name": "do-not-delete bucket"})


# ── State ──
def make_store(tmp_path, ttl=24, snooze=7):
    return ProposalStore(tmp_path / "proposals.json", ttl, snooze)


def test_upsert_dedupes_and_persists_atomically(tmp_path):
    s = make_store(tmp_path)
    kw = dict(action="pause_presto_engine", engine=PRESTO, reason="r", evidence="e", measured={}, confidence="high",
              protected=False, run_ref="x")
    p1, new1 = s.upsert(**kw)
    p2, new2 = s.upsert(**kw)
    s.save()
    assert new1 and not new2 and p1["id"] == p2["id"] and p2["times_seen"] == 2
    assert len(make_store(tmp_path).items) == 1 and not (tmp_path / "proposals.tmp").exists()


def test_rejection_snoozes_and_pending_expires(tmp_path):
    s = make_store(tmp_path, ttl=24)
    p, _ = s.upsert(action="pause_presto_engine", engine=PRESTO, reason="r", evidence="e", measured={},
                    confidence="high", protected=False, run_ref="x")
    s.decide(p, "rejected", "me", "still used by BI")
    assert s.snoozed("pause_presto_engine", "presto4")["decision"]["note"] == "still used by BI"
    q, _ = s.upsert(action="pause_spark_engine", engine=SPARK, reason="r", evidence="e", measured={},
                    confidence="high", protected=True, run_ref="x")
    q["expires_at"] = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat()
    assert [x["id"] for x in s.expire_old()] == [q["id"]] and q["status"] == "expired"


def test_run_lock_is_exclusive(tmp_path):
    with run_lock(tmp_path / ".lock"):
        with pytest.raises(AlreadyRunning):
            with run_lock(tmp_path / ".lock"):
                pass


# ── propose_action + execute, against a fake MCP ──
class NullLog:
    ref = "test"

    def write(self, *a, **k):
        pass


class FakeLake:
    def __init__(self, engines, idle_rows=None, spark_apps=0):
        self.engines, self.idle_rows, self.spark_apps, self.calls = engines, idle_rows or {}, spark_apps, []

    async def call(self, name, args, retry=True):
        self.calls.append((name, args))
        if name == "watsonxdata-list-engines":
            return json.dumps({"engines": self.engines}), False
        if name == "watsonxdata-execute-select":
            row = self.idle_rows[args["engine_id"]]
            return json.dumps({"columns": [{"name": k} for k in row], "rows": [list(row.values())]}), False
        if name == "watsonxdata-list-spark-applications":
            if args.get("state"):
                return json.dumps({"applications": [{"id": i, "state": "running"} for i in range(self.spark_apps)],
                                   "total_count": self.spark_apps}), False
            return json.dumps({"applications": [{"state": "finished", "end_time": "2026-11-09T10:35:23Z"}],
                               "total_count": 1}), False
        if name.startswith("watsonxdata-pause"):
            return json.dumps({"status": "pausing"}), False
        raise AssertionError(name)


def test_propose_refuses_snoozed_and_queues_valid(tmp_path):
    s = make_store(tmp_path)
    snap = {"engines": {"presto4": PRESTO}, "idle": {"presto4": g.interpret_idle(idle_row(idle_s=5 * 3600))}}
    tools = auditor.Tools(FakeLake([PRESTO]), NullLog(), s, snap)
    text, err = asyncio.run(tools.propose({"action": "pause_presto_engine", "engine_id": "presto4", "reason": "r", "evidence": "e"}))
    assert not err and "Queued as proposal" in text and len(s.pending()) == 1
    s.decide(s.pending()[0], "rejected", "me", "no")
    text, err = asyncio.run(tools.propose({"action": "pause_presto_engine", "engine_id": "presto4", "reason": "r", "evidence": "e"}))
    assert err and "rejected" in text


def test_preflight_measures_presto_and_spark(tmp_path):
    lake = FakeLake([PRESTO, SPARK, {**SPARK, "engine_id": "spark1", "status": "paused"}],
                    {"presto4": idle_row(idle_s=5 * 3600)})
    snap = asyncio.run(auditor.preflight(lake))
    assert set(snap["idle"]) == {"presto4", "spark9"}           # paused engines aren't measured
    assert snap["idle"]["spark9"]["last_app_end"].startswith("2026-11-09") and snap["idle"]["presto4"]["idle_hours"] == 5.0


def test_propose_spark_uses_measured_facts_not_model_claims(tmp_path):
    snap = {"engines": {"spark9": SPARK}, "idle": {"spark9": spark_idle(active=2)}}
    tools = auditor.Tools(FakeLake([SPARK]), NullLog(), make_store(tmp_path), snap)
    text, err = asyncio.run(tools.propose({"action": "pause_spark_engine", "engine_id": "spark9", "reason": "r", "evidence": "model says idle"}))
    assert err and "2 active application" in text


def run_execute(monkeypatch, lake, proposal, dry_run=False):
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def fake_connect(log):
        yield None, lake
    monkeypatch.setattr(auditor, "connect", fake_connect)
    tmp = Path(proposal.pop("_tmp"))
    monkeypatch.setattr(auditor, "LOG_DIR", tmp)
    monkeypatch.setattr(auditor, "STATE_DIR", tmp)   # never touch the real state/
    return asyncio.run(auditor.execute(proposal, dry_run))


def proposal(tmp_path, **kw):
    return {"id": "abc123", "action": "pause_presto_engine", "engine_id": "presto4", "engine_type": "presto",
            "confidence": "high", "_tmp": str(tmp_path), **kw}


def test_execute_aborts_if_engine_was_used_since(monkeypatch, tmp_path):
    lake = FakeLake([PRESTO], {"presto4": idle_row(idle_s=300)})  # someone ran a query 5 min ago
    out = run_execute(monkeypatch, lake, proposal(tmp_path))
    assert out.startswith("superseded") and not any(c[0].startswith("watsonxdata-pause") for c in lake.calls)


def test_execute_dry_run_never_pauses(monkeypatch, tmp_path):
    lake = FakeLake([PRESTO], {"presto4": idle_row(idle_s=5 * 3600)})
    out = run_execute(monkeypatch, lake, proposal(tmp_path), dry_run=True)
    assert out.startswith("dry run") and not any(c[0].startswith("watsonxdata-pause") for c in lake.calls)


def test_execute_aborts_if_spark_app_started_since(monkeypatch, tmp_path):
    lake = FakeLake([SPARK], spark_apps=1)
    out = run_execute(monkeypatch, lake, proposal(tmp_path, action="pause_spark_engine", engine_id="spark9", engine_type="spark"))
    assert out.startswith("superseded") and not any(c[0].startswith("watsonxdata-pause") for c in lake.calls)


def test_execute_pauses_after_recheck_without_force(monkeypatch, tmp_path):
    lake = FakeLake([SPARK], spark_apps=0)
    out = run_execute(monkeypatch, lake, proposal(tmp_path, action="pause_spark_engine", engine_id="spark9", engine_type="spark"))
    assert out.startswith("executed")
    assert ("watsonxdata-pause-spark-engine", {"engine_id": "spark9", "force": False}) in lake.calls


# ── Severity rule + report tables ──
def test_idle_severity_rule():
    sev = lambda h: g.idle_severity({"n_active_now": 0, "idle_hours": h}, 2, 24)  # noqa: E731
    assert sev(24) == "HIGH" and sev(159.85) == "HIGH" and sev(23.9) == "LOW" and sev(1) == "-"
    assert sev(None).startswith("LOW") and g.idle_severity(None, 2, 24) == "not measured"
    assert g.idle_severity({"n_active_now": 1, "idle_hours": 0.0}, 2, 24) == "-"


def test_engine_table_replaces_placeholder_with_measured_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(auditor, "REPORT_DIR", tmp_path)
    snap = {"taken_at": "2026-11-16T02:30:00+00:00", "engines": {"presto4": PRESTO, "spark9": SPARK},
            "idle": {"presto4": g.interpret_idle(idle_row(idle_s=5 * 3600)), "spark9": spark_idle()}}
    tools = auditor.Tools(None, NullLog(), make_store(tmp_path), snap)
    tools.report = {"report_markdown": "# Audit\n## Engine activity\n{{ENGINE_ACTIVITY}}\n## Findings\n| # |", "counts": {}}
    usage = {"input": 1, "output": 1, "cache_read": 0, "cache_write": 0}
    text = auditor.write_report(tools, {"stop": "submitted", "steps": 1, "usage": usage, "final_text": ""},
                                tools.store, snap).read_text()
    assert "{{ENGINE_ACTIVITY}}" not in text
    assert "| spark9 |" in text and "146.41h (6.1d)" in text and "**HIGH**" in text
    assert "| presto4 |" in text and "| 5h (0.2d)" in text and "| LOW |" in text


# ── Remembered activity ──
def test_saved_activity_rescues_wrapped_presto_history():
    now = dt.datetime(2026, 11, 16, 12, 0, tzinfo=dt.timezone.utc)
    first, rec = g.merge_activity(g.interpret_idle(idle_row(idle_s=3600)), None, now)
    assert first["source"] == "live history" and rec["last_activity_utc"].startswith("2026-11-15T09:41:16")
    wrapped = g.interpret_idle(idle_row(n_work=0))           # later: history no longer has the work query
    later, rec2 = g.merge_activity(wrapped, rec, now + dt.timedelta(days=1))
    assert later["confidence"] == "medium" and later["source"] == "saved state"
    assert later["idle_hours"] == round((now + dt.timedelta(days=1) - dt.datetime(2026, 11, 15, 9, 41, 16, tzinfo=dt.timezone.utc)).total_seconds() / 3600, 2)
    assert rec2 == {**rec, "updated_at": rec2["updated_at"]}  # newest activity unchanged


def test_newer_live_activity_wins_and_active_now_records_now():
    now = dt.datetime(2026, 11, 16, 12, 0, tzinfo=dt.timezone.utc)
    old = {"last_activity_utc": "2026-11-01T00:00:00+00:00"}
    m, rec = g.merge_activity(g.interpret_idle(idle_row()), old, now)
    assert m["source"] == "live history" and m["confidence"] == "high" and rec["last_activity_utc"].startswith("2026-11-15")
    m, rec = g.merge_activity(g.interpret_idle(idle_row(n_active=1)), old, now)
    assert m["idle_hours"] == 0.0 and rec["last_activity_utc"] == now.isoformat()


def test_no_history_and_no_saved_state_stays_unknown():
    m, rec = g.merge_activity(g.interpret_idle(idle_row(n_work=0)), None, dt.datetime.now(dt.timezone.utc))
    assert m["idle_hours"] is None and m["confidence"] == "low" and rec is None


def test_preflight_persists_and_reuses_activity(tmp_path):
    from state import ActivityStore
    store = ActivityStore(tmp_path / "activity.json")
    asyncio.run(auditor.preflight(FakeLake([PRESTO], {"presto4": idle_row(idle_s=3600)}), store))
    assert "presto4" in ActivityStore(tmp_path / "activity.json").items
    snap = asyncio.run(auditor.preflight(FakeLake([PRESTO], {"presto4": idle_row(n_work=0)}),
                                         ActivityStore(tmp_path / "activity.json")))
    assert snap["idle"]["presto4"]["source"] == "saved state" and snap["idle"]["presto4"]["idle_hours"] > 0
    assert g.check_proposal("pause_presto_engine", PRESTO, snap["idle"]["presto4"], 2) == (None, "medium")
