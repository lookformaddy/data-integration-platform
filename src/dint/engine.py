"""Job orchestration: scheduled/manual runs, retries, dry-run vs apply.

One run of one connector is: resolve credentials (fail-closed) → read
rows newer than the checkpoint watermark with retries and backoff →
validate the mapping spec → map rows (failures quarantined with reasons)
→ quality gates (blocking violations quarantine, warnings recorded) →
dedup by content hash → dry-run reports what would happen, apply writes
to the versioned target store and advances the checkpoint.

A run is recorded in history whether it succeeds or fails, and every
state change is appended to the hash-chained audit log.
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any

from dint.audit import AuditLog
from dint.connectors import SourceConnector, build_connector
from dint.lineage import LineageGraph
from dint.mapping import apply_mapping, row_hash, validate_mapping
from dint.models import (
    ConnectorConfig,
    ConnectorError,
    ConnectorMetrics,
    CredentialError,
    IntegrationError,
    MappingError,
    MappingSpec,
    QualityRule,
    QuarantinedRow,
    RunRecord,
    RunStatus,
)
from dint.quality import evaluate_quality
from dint.state import CheckpointStore, TargetStore


def _parse_watermark(value: Any) -> datetime | None:
    """Best-effort parse of a watermark value to a datetime for lag math."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


class IntegrationEngine:
    def __init__(self, audit: AuditLog | None = None):
        self.audit = audit or AuditLog()
        self.connectors: dict[str, SourceConnector] = {}
        self.configs: dict[str, ConnectorConfig] = {}
        self.mappings: dict[str, MappingSpec] = {}
        self.quality_rules: dict[str, list[QualityRule]] = {}
        self.checkpoints = CheckpointStore()
        self.targets = TargetStore()
        self.lineage = LineageGraph()
        self.runs: list[RunRecord] = []
        self.metrics: dict[str, ConnectorMetrics] = {}
        self.quarantine: list[QuarantinedRow] = []

    # -- registration -----------------------------------------------------
    def register_connector(self, config: ConnectorConfig, **kwargs) -> SourceConnector:
        connector = build_connector(config, **kwargs)
        self.connectors[config.connector_id] = connector
        self.configs[config.connector_id] = config
        self.metrics.setdefault(config.connector_id,
                                ConnectorMetrics(connector_id=config.connector_id))
        self.audit.append("connector.registered", "integration-engine",
                          None, {"connector_id": config.connector_id,
                                 "kind": config.kind.value})
        return connector

    def register_mapping(self, spec: MappingSpec) -> None:
        config = self.configs.get(spec.connector_id)
        if config is None:
            raise IntegrationError(
                f"mapping for unknown connector: {spec.connector_id}")
        validate_mapping(spec, config.schema)
        self.mappings[spec.connector_id] = spec
        self.lineage.register(spec)
        self.audit.append("mapping.registered", "integration-engine", None,
                          {"connector_id": spec.connector_id,
                           "target_table": spec.target_table})

    def register_quality_rules(self, connector_id: str,
                               rules: list[QualityRule]) -> None:
        if connector_id not in self.configs:
            raise IntegrationError(f"unknown connector: {connector_id}")
        self.quality_rules[connector_id] = list(rules)

    # -- runs -------------------------------------------------------------
    def run(self, connector_id: str, *, dry_run: bool = False,
            now: datetime | None = None, sleep_fn=time.sleep) -> RunRecord:
        now = now or datetime.now(timezone.utc)
        started = now
        run_id = f"run-{uuid.uuid4().hex[:8]}"
        record = RunRecord(run_id=run_id, connector_id=connector_id,
                           target_table=self.mappings.get(connector_id).target_table
                           if connector_id in self.mappings else "",
                           status=RunStatus.FAILED, started_at=started,
                           dry_run=dry_run)
        self.runs.append(record)
        connector = self.connectors.get(connector_id)
        if connector is None:
            record.error = f"unknown connector: {connector_id}"
            self._finish(record, now)
            return record
        config = self.configs[connector_id]
        checkpoint = self.checkpoints.get(connector_id)
        record.watermark_before = checkpoint.watermark

        try:
            connector.resolve_credential()
            rows = self._read_with_retries(connector, config, checkpoint, record,
                                           sleep_fn)
            record.rows_read = len(rows)
            connector.validate_schema(rows)
            spec = self.mappings.get(connector_id)
            if spec is None:
                raise IntegrationError(
                    f"no mapping registered for connector {connector_id}")
            mapped, mapping_quarantine = self._map_rows(spec, config, rows)
            record.rows_quarantined += len(mapping_quarantine)
            blocking, warnings = evaluate_quality(
                mapped, config.key_field,
                self.quality_rules.get(connector_id, []))
            blocked_keys = {v.row_key for v in blocking}
            quality_quarantine = [
                QuarantinedRow(row_key=m.get(config.key_field), stage="quality",
                               reason="; ".join(
                                   v.reason for v in blocking
                                   if v.row_key == m.get(config.key_field)),
                               row=m)
                for m in mapped if m.get(config.key_field) in blocked_keys]
            record.rows_quarantined += len(quality_quarantine)
            clean = [m for m in mapped
                     if m.get(config.key_field) not in blocked_keys]
            self.quarantine.extend(mapping_quarantine + quality_quarantine)

            written, skipped = self._write(clean, spec, config, dry_run, now)
            record.rows_written = written
            record.rows_skipped_duplicate = skipped
            if not dry_run and rows:
                newest = max((r.get(config.watermark_field) for r in rows
                              if r.get(config.watermark_field) is not None),
                             default=None)
                if newest is not None:
                    self.checkpoints.save(connector_id, newest, len(rows),
                                          run_id, now)
                    record.watermark_after = newest
            record.status = (RunStatus.DRY_RUN if dry_run
                             else RunStatus.PARTIAL if record.rows_quarantined
                             else RunStatus.SUCCEEDED)
            self._update_metrics(connector_id, record, rows, now)
            self.audit.append("run.completed", "integration-engine", run_id,
                              {"connector_id": connector_id,
                               "status": record.status.value,
                               "rows_read": record.rows_read,
                               "rows_written": record.rows_written,
                               "rows_quarantined": record.rows_quarantined,
                               "dry_run": dry_run,
                               "warnings": len(warnings)})
        except (CredentialError, ConnectorError, MappingError,
                IntegrationError) as exc:
            record.status = RunStatus.FAILED
            record.error = str(exc)
            metrics = self.metrics[connector_id]
            metrics.failures += 1
            self.audit.append("run.failed", "integration-engine", run_id,
                              {"connector_id": connector_id, "error": str(exc),
                               "attempts": record.attempts})
        self._finish(record, now)
        return record

    def _read_with_retries(self, connector: SourceConnector,
                           config: ConnectorConfig, checkpoint,
                           record: RunRecord, sleep_fn) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(1, config.max_retries + 1):
            record.attempts = attempt
            try:
                return connector.read_since(checkpoint.watermark)
            except ConnectorError as exc:
                last_error = exc
                if attempt < config.max_retries:
                    sleep_fn(config.retry_backoff_seconds * (2 ** (attempt - 1)))
        raise last_error  # type: ignore[misc]

    def _map_rows(self, spec: MappingSpec, config: ConnectorConfig,
                  rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]],
                                                      list[QuarantinedRow]]:
        mapped: list[dict[str, Any]] = []
        quarantined: list[QuarantinedRow] = []
        for row in rows:
            try:
                mapped_row = apply_mapping(spec, row)
                mapped.append(mapped_row)
            except MappingError as exc:
                quarantined.append(QuarantinedRow(
                    row_key=row.get(config.key_field), stage="mapping",
                    reason=str(exc), row=row))
        return mapped, quarantined

    def _write(self, rows: list[dict[str, Any]], spec: MappingSpec,
               config: ConnectorConfig, dry_run: bool,
               now: datetime) -> tuple[int, int]:
        """Dedup by content hash, then upsert. Returns (written, skipped)."""
        written = 0
        skipped = 0
        seen_hashes: set[str] = set()
        for row in rows:
            digest = row_hash(row)
            if digest in seen_hashes:
                skipped += 1
                continue
            seen_hashes.add(digest)
            key = row.get(config.key_field)
            if not dry_run:
                existing = self.targets.get(spec.target_table, key)
                if existing is not None and existing.row_hash == digest:
                    skipped += 1
                    continue
                self.targets.upsert(spec.target_table, key, row, now)
            written += 1
        return written, skipped

    def _update_metrics(self, connector_id: str, record: RunRecord,
                        rows: list[dict[str, Any]], now: datetime) -> None:
        metrics = self.metrics[connector_id]
        metrics.runs += 1
        metrics.rows_read += record.rows_read
        metrics.rows_written += record.rows_written
        metrics.rows_quarantined += record.rows_quarantined
        metrics.last_run_at = now
        if record.duration_seconds > 0:
            metrics.last_throughput_rows_per_sec = (
                record.rows_read / record.duration_seconds)
        newest = max((_parse_watermark(r.get(self.configs[connector_id].watermark_field))
                      for r in rows), default=None)
        if newest is not None:
            metrics.last_lag_seconds = max(
                0.0, (now - newest).total_seconds())

    def _finish(self, record: RunRecord, now: datetime) -> None:
        record.finished_at = now
        record.duration_seconds = max(
            0.0, (now - record.started_at).total_seconds())

    # -- queries ----------------------------------------------------------
    def run_history(self, connector_id: str | None = None) -> list[RunRecord]:
        if connector_id is None:
            return list(self.runs)
        return [r for r in self.runs if r.connector_id == connector_id]

    def quarantined(self, connector_id: str | None = None) -> list[QuarantinedRow]:
        # Quarantine rows are not tagged per connector in this in-memory
        # store; callers filter by the run history when they need per-
        # connector attribution.
        return list(self.quarantine)
