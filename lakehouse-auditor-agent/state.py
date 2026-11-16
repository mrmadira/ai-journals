"""Durable state between runs: the proposal queue and a run lock.

An unattended agent needs memory a chat session doesn't: what it proposed yesterday, what you
rejected (so it stops nagging), and whether another run is already in progress.
"""

import datetime as dt
import fcntl
import json
import os
import uuid
from contextlib import contextmanager
from pathlib import Path


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def iso(t: dt.datetime) -> str:
    return t.isoformat()


def parse(ts: str) -> dt.datetime:
    return dt.datetime.fromisoformat(ts)


class ProposalStore:
    """proposals.json - a list of proposal records. Every write is atomic (tmp file + rename),
    so a crash or a kill mid-write can never leave a half-written queue."""

    def __init__(self, path: Path, ttl_hours: float, snooze_days: float) -> None:
        self.path = path
        self.ttl = dt.timedelta(hours=ttl_hours)
        self.snooze = dt.timedelta(days=snooze_days)
        self.items: list[dict] = json.loads(path.read_text()) if path.exists() else []

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.items, indent=2, default=str))
        os.replace(tmp, self.path)

    def get(self, pid: str) -> dict | None:
        return next((p for p in self.items if p["id"] == pid), None)

    def expire_old(self) -> list[dict]:
        """Pending proposals older than the TTL expire: their evidence is too old to act on."""
        expired = []
        for p in self.items:
            if p["status"] == "pending" and now() >= parse(p["expires_at"]):
                p["status"] = "expired"
                expired.append(p)
        return expired

    def pending(self) -> list[dict]:
        return [p for p in self.items if p["status"] == "pending"]

    def snoozed(self, action: str, engine_id: str) -> dict | None:
        """A rejection from the last `snooze_days` - the agent must not re-propose it."""
        for p in reversed(self.items):
            if (p["action"], p["engine_id"]) == (action, engine_id) and p["status"] == "rejected":
                if now() - parse(p["decision"]["at"]) < self.snooze:
                    return p
        return None

    def upsert(self, *, action: str, engine: dict, reason: str, evidence: str, measured: dict,
               confidence: str, protected: bool, run_ref: str) -> tuple[dict, bool]:
        """Add a proposal, or refresh the identical pending one. Returns (proposal, is_new)."""
        t = now()
        for p in self.items:
            if p["status"] == "pending" and (p["action"], p["engine_id"]) == (action, engine["engine_id"]):
                p.update(reason=reason, evidence=evidence, measured=measured, confidence=confidence,
                         last_seen_at=iso(t), expires_at=iso(t + self.ttl), run_ref=run_ref)
                p["times_seen"] = p.get("times_seen", 1) + 1
                return p, False
        p = {
            "id": uuid.uuid4().hex[:6],
            "status": "pending",
            "action": action,
            "engine_id": engine["engine_id"],
            "engine_name": engine.get("display_name"),
            "engine_type": engine.get("type"),
            "owner": engine.get("created_by"),
            "reason": reason,
            "evidence": evidence,
            "measured": measured,
            "confidence": confidence,
            "protected": protected,
            "created_at": iso(t),
            "last_seen_at": iso(t),
            "expires_at": iso(t + self.ttl),
            "times_seen": 1,
            "run_ref": run_ref,
        }
        self.items.append(p)
        return p, True

    def decide(self, p: dict, status: str, by: str, note: str = "", result: str | None = None) -> None:
        p["status"] = status
        p["decision"] = {"at": iso(now()), "by": by, "note": note}
        if result is not None:
            p["result"] = result


class AlreadyRunning(Exception):
    pass


@contextmanager
def run_lock(path: Path):
    """Exclusive, non-blocking lock: a slow run and the next scheduled run can't overlap,
    and `review` can't execute a pause while an audit is mid-flight."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise AlreadyRunning(f"another audit-lake process holds {path}") from None
        f.write(str(os.getpid()))
        f.flush()
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


class ActivityStore:
    """activity.json - per engine, the newest real activity any run has observed."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.items: dict[str, dict] = json.loads(path.read_text()) if path.exists() else {}

    def get(self, engine_id: str) -> dict | None:
        return self.items.get(engine_id)

    def put(self, engine_id: str, record: dict | None) -> None:
        if record:
            self.items[engine_id] = record

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.items, indent=2, sort_keys=True))
        os.replace(tmp, self.path)
