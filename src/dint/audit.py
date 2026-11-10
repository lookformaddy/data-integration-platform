"""Hash-chained, append-only audit log (tamper-evident)."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class AuditEntry:
    seq: int
    timestamp: str
    actor: str
    action: str
    run_id: str | None
    details: dict
    prev_hash: str
    entry_hash: str = field(default="")


class AuditLog:
    def __init__(self):
        self.entries: list[AuditEntry] = []

    def append(self, action: str, actor: str, run_id: str | None,
               details: dict | None = None, timestamp: str | None = None) -> AuditEntry:
        prev = self.entries[-1].entry_hash if self.entries else "GENESIS"
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        entry = AuditEntry(seq=len(self.entries) + 1, timestamp=ts, actor=actor,
                           action=action, run_id=run_id,
                           details=details or {}, prev_hash=prev)
        payload = json.dumps(
            {"seq": entry.seq, "timestamp": ts, "actor": actor, "action": action,
             "run_id": run_id, "details": entry.details, "prev_hash": prev},
            sort_keys=True, default=str)
        entry.entry_hash = hashlib.sha256(payload.encode()).hexdigest()
        self.entries.append(entry)
        return entry

    def verify(self) -> bool:
        prev = "GENESIS"
        for entry in self.entries:
            if entry.prev_hash != prev:
                return False
            payload = json.dumps(
                {"seq": entry.seq, "timestamp": entry.timestamp, "actor": entry.actor,
                 "action": entry.action, "run_id": entry.run_id,
                 "details": entry.details, "prev_hash": entry.prev_hash},
                sort_keys=True, default=str)
            if hashlib.sha256(payload.encode()).hexdigest() != entry.entry_hash:
                return False
            prev = entry.entry_hash
        return True

    def for_run(self, run_id: str) -> list[AuditEntry]:
        return [e for e in self.entries if e.run_id == run_id]
