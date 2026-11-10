"""Checkpointed incremental state and the versioned target store.

Checkpoints hold the last watermark processed per connector, so a run
only reads rows newer than the last successful run. The target store is a
versioned table: upserts are keyed on the natural key, each write bumps
the version, and a row whose content hash is unchanged is skipped, which
makes re-runs idempotent.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from dint.mapping import row_hash
from dint.models import Checkpoint, TargetRow


class CheckpointStore:
    def __init__(self):
        self._checkpoints: dict[str, Checkpoint] = {}

    def get(self, connector_id: str) -> Checkpoint:
        return self._checkpoints.setdefault(
            connector_id, Checkpoint(connector_id=connector_id))

    def save(self, connector_id: str, watermark: Any, rows_processed: int,
             run_id: str, now: datetime) -> Checkpoint:
        checkpoint = Checkpoint(connector_id=connector_id, watermark=watermark,
                                rows_processed=rows_processed, last_run_id=run_id,
                                updated_at=now)
        self._checkpoints[connector_id] = checkpoint
        return checkpoint


class TargetStore:
    """Versioned table store with upsert/merge semantics and history."""

    def __init__(self):
        self._tables: dict[str, dict[Any, TargetRow]] = {}
        self._history: dict[str, dict[Any, list[TargetRow]]] = {}

    def upsert(self, table: str, key: Any, row: dict[str, Any],
               now: datetime) -> tuple[TargetRow, bool]:
        """Upsert one row. Returns (target_row, changed). Unchanged rows
        (same content hash) are skipped, so re-runs are idempotent."""
        digest = row_hash(row)
        table_rows = self._tables.setdefault(table, {})
        existing = table_rows.get(key)
        if existing is not None and existing.row_hash == digest:
            return existing, False
        version = existing.version + 1 if existing is not None else 1
        target = TargetRow(key=key, row=dict(row), version=version,
                           row_hash=digest, updated_at=now)
        table_rows[key] = target
        self._history.setdefault(table, {}).setdefault(key, []).append(target)
        return target, True

    def get(self, table: str, key: Any) -> TargetRow | None:
        return self._tables.get(table, {}).get(key)

    def rows(self, table: str) -> list[TargetRow]:
        return list(self._tables.get(table, {}).values())

    def history(self, table: str, key: Any) -> list[TargetRow]:
        return list(self._history.get(table, {}).get(key, []))

    def tables(self) -> list[str]:
        return sorted(self._tables)
