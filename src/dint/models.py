"""Shared data model for the enterprise data integration platform."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ConnectorKind(str, Enum):
    FILE = "file"      # CSV-style file source (synthetic rows in this project)
    API = "api"        # JSON API-style source, paginated
    TABLE = "table"    # database-table-style source


class RunStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DRY_RUN = "dry_run"
    PARTIAL = "partial"   # succeeded but some rows quarantined


class TransformKind(str, Enum):
    PASSTHROUGH = "passthrough"
    RENAME = "rename"          # same as passthrough, explicit target rename
    CAST = "cast"              # params: {"to": "int"|"float"|"str"|"bool"|"date"}
    CONCAT = "concat"          # params: {"fields": [...], "separator": " "}
    LOOKUP = "lookup"          # params: {"table": {...}, "default": ...}
    HASH = "hash"              # deterministic salted SHA-256 of the value
    DEFAULT = "default"        # params: {"value": ...} when source is null


@dataclass
class ConnectorConfig:
    """Declarative registration of one source connector."""
    connector_id: str
    kind: ConnectorKind
    display_name: str
    source_name: str                    # file name / API resource / table name
    schema: list[str]                   # declared source columns
    watermark_field: str = "updated_at" # incremental cursor column
    key_field: str = "id"               # natural key used for idempotent upsert
    credential_env: str | None = None   # env var holding the credential;
                                        # None = no credential required
    batch_size: int = 100
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0


@dataclass
class FieldMapping:
    source: str
    target: str
    transform: TransformKind = TransformKind.PASSTHROUGH
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class MappingSpec:
    """Declarative field mapping from one connector into one target table."""
    connector_id: str
    target_table: str
    mappings: list[FieldMapping] = field(default_factory=list)

    def target_columns(self) -> list[str]:
        return [m.target for m in self.mappings]


@dataclass
class QualityRule:
    """One data-quality gate applied to mapped rows before they land."""
    rule_id: str
    kind: str                 # "not_null" | "null_rate" | "range" | "unique"
    column: str
    blocking: bool = True     # blocking violations quarantine the row
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityViolation:
    rule_id: str
    kind: str
    column: str
    reason: str
    row_key: Any = None


@dataclass
class QuarantinedRow:
    row_key: Any
    stage: str                # "mapping" | "quality" | "dedup"
    reason: str
    row: dict[str, Any] = field(default_factory=dict)


@dataclass
class Checkpoint:
    connector_id: str
    watermark: Any = None     # last watermark value successfully processed
    rows_processed: int = 0
    last_run_id: str | None = None
    updated_at: datetime | None = None


@dataclass
class TargetRow:
    key: Any
    row: dict[str, Any]
    version: int = 1
    row_hash: str = ""
    updated_at: datetime | None = None


@dataclass
class RunRecord:
    run_id: str
    connector_id: str
    target_table: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None = None
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    rows_skipped_duplicate: int = 0
    dry_run: bool = False
    attempts: int = 1
    error: str | None = None
    watermark_before: Any = None
    watermark_after: Any = None
    duration_seconds: float = 0.0


@dataclass
class ConnectorMetrics:
    connector_id: str
    runs: int = 0
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    last_lag_seconds: float | None = None   # now - newest watermark processed
    last_throughput_rows_per_sec: float | None = None
    last_run_at: datetime | None = None
    failures: int = 0


class IntegrationError(ValueError):
    """Fail-closed error: the integration operation is not permitted."""


class MappingError(IntegrationError):
    pass


class QualityError(IntegrationError):
    pass


class ConnectorError(IntegrationError):
    pass


class CredentialError(IntegrationError):
    pass
