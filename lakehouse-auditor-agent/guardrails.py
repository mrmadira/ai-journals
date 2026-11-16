"""Guardrails for the audit agent: pure functions, no network, no model. All unit-tested.

The rule this file exists to enforce: the MODEL may look and may PROPOSE; only CODE decides
what is allowed, and only a HUMAN can turn a proposal into an action.
"""

import datetime as dt
import hashlib
import json
import re

# ── Tool surface ──────────────────────────────────────────────────────────────
# The MCP server exposes ~50 tools. The model is shown ONLY these read-only ones. Note what's
# missing: execute-select (the idle check runs in code with a fixed query) and every pause /
# resume / create / update / delete tool (actions go through propose_action + human review).
READ_ONLY_TOOLS = frozenset({
    "watsonxdata-get-instance-details",
    "watsonxdata-list-engines",
    "watsonxdata-list-schemas",
    "watsonxdata-list-tables",
    "watsonxdata-list-storage-registrations",
    "watsonxdata-list-database-registrations",
    "watsonxdata-get-database-connection-status",
    "watsonxdata-list-spark-applications",
    "watsonxdata-get-associated-engines",
    "watsonxdata-get-qhmm-config",
})

# The ONLY actions that can ever be proposed, and the MCP tool + engine type each maps to.
# Executed exclusively by review code after human approval - never by the model.
ACTIONS = {
    "pause_presto_engine": ("watsonxdata-pause-presto-engine", "presto"),
    "pause_prestissimo_engine": ("watsonxdata-pause-prestissimo-engine", "prestissimo"),
    "pause_spark_engine": ("watsonxdata-pause-spark-engine", "spark"),
}

ACTIVE_SPARK_STATES = ["running", "accepted", "submitted", "waiting"]

# ── Secret redaction ──────────────────────────────────────────────────────────
# Applied to EVERY tool result before the model or the log sees it. Values become a short
# fingerprint, so the model can still spot one credential reused across registrations
# (same fp = same value) without ever seeing the credential.
SECRET_KEY_RE = re.compile(
    r"(secret|passw|pwd|token|credential|api_?key|access_?key|private_?key|account_?key|"
    r"client_?secret|sas_?token|connection_?string)", re.I)
NOT_SECRET_KEY_RE = re.compile(r"(encrypt|_type$|_mode$|^type$|^mode$|_present$|_set$|_id_type$)", re.I)
SECRET_VALUE_PATTERNS = [
    re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b"),                       # AWS access key ids
    re.compile(r"(?i)\b(basic|bearer)\s+[A-Za-z0-9+/=._-]{12,}"),      # auth headers
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"(?i)(password|pwd|secret)=([^;&\s\"']+)"),           # inline k=v in URLs/conn strings
]


def fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:8]


def redact_text(text: str) -> str:
    """Scrub secret-looking substrings from free text (errors, non-JSON output, the report)."""
    for pat in SECRET_VALUE_PATTERNS:
        text = pat.sub(lambda m: f"[REDACTED fp={fingerprint(m.group(0))}]", text)
    return text


def redact_obj(obj, _key: str = ""):
    """Recursively replace values under secret-named keys; scrub strings everywhere else."""
    if isinstance(obj, dict):
        return {k: redact_obj(v, str(k)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact_obj(v, _key) for v in obj]
    if isinstance(obj, str):
        if obj and SECRET_KEY_RE.search(_key) and not NOT_SECRET_KEY_RE.search(_key):
            return f"[REDACTED fp={fingerprint(obj)}]"
        return redact_text(obj)
    return obj  # numbers, booleans (e.g. "encrypt": true), None stay as they are


def redact(text: str) -> str:
    """Redact a tool result: structurally if it's JSON, by pattern otherwise."""
    try:
        return json.dumps(redact_obj(json.loads(text)), ensure_ascii=False)
    except (ValueError, TypeError):
        return redact_text(text)


# ── Idle detection (Presto/Prestissimo) ───────────────────────────────────────
# system.runtime.queries is Presto's in-memory history (~last 100 queries, wiped on restart).
# Two exclusions make it trustworthy:
#  * this probe itself (else every engine looks busy the moment we check it);
#  * metadata statements: list-schemas / describe-table run as SHOW / DESCRIBE queries, so an
#    audit that probes catalogs every morning would otherwise reset every engine's idle clock.
_META = r"'^(show|describe|explain|use)\s'"
_WORK = f"NOT regexp_like(lower(trim(query)), {_META})"
IDLE_SQL = (
    "SELECT count(*) AS n_queries, "
    "count_if(state NOT IN ('FINISHED', 'FAILED')) AS n_active, "
    f"count_if({_WORK}) AS n_work, "
    f"CAST(max(CASE WHEN {_WORK} THEN coalesce(\"end\", localtimestamp) END) AS varchar) AS last_work_end, "
    "CAST(max(coalesce(\"end\", localtimestamp)) AS varchar) AS last_any_end, "
    "CAST(min(\"created\") AS varchar) AS oldest_in_history, "
    "CAST(localtimestamp AS varchar) AS now, current_timezone() AS tz, "
    f"date_diff('second', max(CASE WHEN {_WORK} THEN coalesce(\"end\", localtimestamp) END), localtimestamp) AS idle_work_s "
    "FROM system.runtime.queries WHERE query NOT LIKE '%system.runtime.queries%'"
)
HISTORY_CAP_HINT = 90  # history near this size means the oldest entry is the cap edge, not a start


def parse_select(text: str) -> list[dict]:
    """execute-select result -> list of {column: value} rows."""
    data = json.loads(text)
    cols = [c["name"] for c in data.get("columns", [])]
    return [dict(zip(cols, row)) for row in data.get("rows", [])]


def interpret_idle(row: dict) -> dict:
    """Turn the probe row into facts + a confidence the model and the reviewer can rely on."""
    n_work = int(row.get("n_work") or 0)
    idle_s = row.get("idle_work_s")
    out = {
        "n_queries_in_history": int(row.get("n_queries") or 0),
        "n_active_now": int(row.get("n_active") or 0),
        "n_work_queries": n_work,
        "last_work_query_end": row.get("last_work_end"),
        "last_any_query_end": row.get("last_any_end"),
        "oldest_in_history": row.get("oldest_in_history"),
        "measured_at": row.get("now"),
        "timezone": row.get("tz"),
    }
    if out["n_active_now"] > 0:
        out.update(idle_hours=0.0, confidence="high", note="queries are running right now")
    elif n_work == 0 or idle_s is None:
        out.update(idle_hours=None, confidence="low",
                   note="no work queries in in-memory history: either idle since the last restart, "
                        "or restarted recently. Idle time is UNKNOWN, not 'forever'.")
    else:
        out.update(idle_hours=round(int(idle_s) / 3600, 2), confidence="high",
                   note="idle since the last non-metadata query")
        if out["n_queries_in_history"] >= HISTORY_CAP_HINT:
            out["note"] += "; history is near its ~100-query cap, so oldest_in_history is just the cap edge"
    return out


# ── Idle detection (Spark) ────────────────────────────────────────────────────
# Spark keeps application history server-side (it survives engine restarts, unlike Presto's
# in-memory query list), so "last application ended" is a durable idle signal.
SPARK_END_FIELDS = ("end_time", "finish_time", "failed_time", "stop_time", "stopped_time", "killed_time")


def _ts(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        t = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def interpret_spark_idle(active_total: int, apps: list[dict], now: dt.datetime) -> dict:
    """active_total: authoritative count of apps in ACTIVE_SPARK_STATES; apps: recent history."""
    ends = [t for a in apps for t in [max((x for x in map(_ts, (a.get(f) for f in SPARK_END_FIELDS)) if x),
                                           default=None)] if t]
    last = max(ends, default=None)
    out = {"n_active_now": active_total, "n_apps_in_history": len(apps),
           "last_app_end": last.isoformat(timespec="seconds") if last else None,
           "measured_at": now.isoformat(timespec="seconds"), "timezone": "UTC"}
    if active_total > 0:
        out.update(idle_hours=0.0, confidence="high", note="applications are active right now")
    elif last is None:
        out.update(idle_hours=None, confidence="low",
                   note="no finished applications in history: idle since creation, or history was purged. "
                        "Idle time is UNKNOWN.")
    else:
        out.update(idle_hours=round((now - last).total_seconds() / 3600, 2), confidence="high",
                   note="idle since the last application ended (server-side history)")
    return out


# ── Remembered activity (survives Presto's history wrapping or an engine restart) ──
def _engine_ts(value: str | None, tz: str | None) -> dt.datetime | None:
    """Presto returns naive timestamps in the session time zone; make them UTC-aware."""
    t = _ts(value)
    if t is None or not value or "+" in str(value) or str(value).endswith("Z") or tz in (None, "UTC", "Z"):
        return t
    try:
        from zoneinfo import ZoneInfo
        return t.replace(tzinfo=ZoneInfo(tz)).astimezone(dt.timezone.utc)
    except Exception:  # noqa: BLE001 - unknown zone name: assume UTC rather than crash
        return t


def measured_last_activity(m: dict) -> dt.datetime | None:
    if "last_app_end" in m:  # Spark
        return _ts(m.get("last_app_end"))
    return _engine_ts(m.get("last_work_query_end"), m.get("timezone"))


def merge_activity(m: dict | None, saved: dict | None, now: dt.datetime) -> tuple[dict | None, dict | None]:
    """Combine a live measurement with what earlier runs saw. Returns (idle, record_to_save).

    Presto's in-memory history keeps ~100 queries; the audit's own SHOW probes push old work
    queries out, and a restart wipes it. Without memory, a well-measured engine would decay to
    'idle unknown'. With it, we keep the newest real activity ever observed.
    """
    if not m or "error" in m:
        return m, saved
    if m["n_active_now"] > 0:
        live = now
    else:  # a timestamp only counts if the measurement itself trusted it
        live = measured_last_activity(m) if m.get("idle_hours") is not None else None
    kept = _ts(saved.get("last_activity_utc")) if saved else None
    best = max((t for t in (live, kept) if t), default=None)
    out = {**m, "last_activity_utc": best.isoformat(timespec="seconds") if best else None, "source": "live history"}
    if best is not None and m["n_active_now"] == 0 and (live is None or kept and kept > live):
        out.update(idle_hours=round((now - best).total_seconds() / 3600, 2), confidence="medium",
                   source="saved state",
                   note=f"last real activity {best:%Y-%m-%d %H:%M} UTC was seen by an earlier audit run and "
                        "is no longer in the engine's history (it wrapped or the engine restarted). "
                        "Nothing newer has been seen since.")
    record = {"last_activity_utc": best.isoformat(timespec="seconds"), "updated_at": now.isoformat(timespec="seconds")} \
        if best else saved
    return out, record


def idle_severity(idle: dict | None, min_hours: float, high_hours: float) -> str:
    """The severity rule for idle engines, decided in code so the report can't drift from it."""
    if not idle or "error" in idle:
        return "not measured"
    if idle["n_active_now"] > 0:
        return "-"                      # in use right now: not a finding
    if idle["idle_hours"] is None:
        return "LOW (idle unknown)"
    if idle["idle_hours"] >= high_hours:
        return "HIGH"
    if idle["idle_hours"] >= min_hours:
        return "LOW"
    return "-"


# ── Proposal validation ───────────────────────────────────────────────────────
def is_protected(engine: dict) -> bool:
    """donotdelete-style names: proposals allowed, but review demands typing the engine id."""
    name = f"{engine.get('engine_id', '')} {engine.get('display_name', '')}".lower()
    return bool(re.search(r"do[-_ ]?not[-_ ]?delete|donotdelete|do[-_ ]?not[-_ ]?pause|protected", name))


def check_proposal(action: str, engine: dict | None, idle: dict | None,
                   min_idle_hours: float) -> tuple[str | None, str]:
    """Decide in code whether a proposal is admissible. Returns (error, confidence)."""
    if action not in ACTIONS:
        return f"Unknown action {action!r}. Allowed: {', '.join(ACTIONS)}.", ""
    if engine is None:
        return "No such engine in the inventory taken at the start of this run.", ""
    _, want_type = ACTIONS[action]
    if engine.get("type") != want_type:
        return f"Engine {engine.get('engine_id')} is type {engine.get('type')!r}; {action} needs {want_type!r}.", ""
    if str(engine.get("status", "")).lower() != "running":
        return f"Engine {engine.get('engine_id')} is {engine.get('status')!r}, not running - nothing to pause.", ""
    if idle is None:
        return "No idle measurement for this engine (the pre-flight probe failed); can't propose.", ""
    if idle["n_active_now"] > 0:
        what = "active application(s)" if want_type == "spark" else "running queries"
        return f"Engine has {idle['n_active_now']} {what} right now; it is not idle.", ""
    if idle["idle_hours"] is None:
        return None, "low"  # allowed, but flagged: the reviewer sees that idle time is unknown
    if idle["idle_hours"] < min_idle_hours:
        return (f"Engine idle for {idle['idle_hours']}h, below the {min_idle_hours}h threshold. "
                "Report it if relevant, but don't propose a pause."), ""
    return None, idle.get("confidence", "high")  # "medium" when it came from saved state


def clip(text: str, n: int) -> str:
    return " ".join(str(text).split())[:n]
