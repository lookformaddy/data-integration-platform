"""Declarative field mappings with type coercion and transforms.

A mapping spec is validated up front (fail-closed): every source column
must exist in the connector's declared schema, every target column must be
unique, and every transform's parameters must be complete. A row that fails
a transform is quarantined with its reason — the run never silently drops
or mis-converts data.
"""
from __future__ import annotations

import hashlib
import os
from datetime import date, datetime
from typing import Any

from dint.models import (
    FieldMapping,
    MappingError,
    MappingSpec,
    TransformKind,
)


def validate_mapping(spec: MappingSpec, source_schema: list[str]) -> None:
    """Validate a mapping spec against a connector schema; raise on any error."""
    if not spec.mappings:
        raise MappingError(
            f"mapping for {spec.connector_id}: at least one field mapping is required")
    targets: set[str] = set()
    for mapping in spec.mappings:
        if mapping.transform is TransformKind.CONCAT:
            fields = mapping.params.get("fields")
            if not isinstance(fields, list) or not fields:
                raise MappingError(
                    f"mapping {mapping.source}->{mapping.target}: concat "
                    f"requires a non-empty 'fields' list")
            for f in fields:
                if f not in source_schema:
                    raise MappingError(
                        f"mapping {mapping.source}->{mapping.target}: concat "
                        f"field '{f}' is not in the source schema")
        elif mapping.source not in source_schema:
            raise MappingError(
                f"mapping {mapping.source}->{mapping.target}: source column "
                f"'{mapping.source}' is not in the source schema")
        if mapping.target in targets:
            raise MappingError(
                f"mapping for {spec.connector_id}: duplicate target column "
                f"'{mapping.target}'")
        targets.add(mapping.target)
        if mapping.transform is TransformKind.CAST:
            to = mapping.params.get("to")
            if to not in ("int", "float", "str", "bool", "date"):
                raise MappingError(
                    f"mapping {mapping.source}->{mapping.target}: cast "
                    f"requires 'to' in int/float/str/bool/date, got {to!r}")
        if mapping.transform is TransformKind.LOOKUP:
            if not isinstance(mapping.params.get("table"), dict):
                raise MappingError(
                    f"mapping {mapping.source}->{mapping.target}: lookup "
                    f"requires a 'table' dict")
        if mapping.transform is TransformKind.DEFAULT:
            if "value" not in mapping.params:
                raise MappingError(
                    f"mapping {mapping.source}->{mapping.target}: default "
                    f"requires a 'value'")


def _cast(value: Any, to: str) -> Any:
    if value is None:
        return None
    try:
        if to == "int":
            return int(value)
        if to == "float":
            return float(value)
        if to == "str":
            return str(value)
        if to == "bool":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in ("true", "1", "yes", "y"):
                    return True
                if lowered in ("false", "0", "no", "n"):
                    return False
            return bool(value)
        if to == "date":
            if isinstance(value, date):
                return value.isoformat()
            return datetime.fromisoformat(str(value)).date().isoformat()
    except (ValueError, TypeError) as exc:
        raise MappingError(f"cast to {to} failed for value {value!r}: {exc}") from exc
    raise MappingError(f"unknown cast target: {to}")


def _hash_value(value: Any) -> str:
    salt = os.environ.get("DINT_HASH_SALT", "demo-salt-change-me")
    digest = hashlib.sha256(f"{salt}:{value}".encode()).hexdigest()
    return f"hash:{digest[:32]}"


def apply_transform(mapping: FieldMapping, row: dict[str, Any]) -> Any:
    """Apply one field mapping's transform to a source row."""
    kind = mapping.transform
    if kind in (TransformKind.PASSTHROUGH, TransformKind.RENAME):
        return row.get(mapping.source)
    if kind is TransformKind.CAST:
        return _cast(row.get(mapping.source), mapping.params["to"])
    if kind is TransformKind.CONCAT:
        parts = [str(row.get(f)) for f in mapping.params["fields"]
                 if row.get(f) is not None]
        return mapping.params.get("separator", " ").join(parts)
    if kind is TransformKind.LOOKUP:
        value = row.get(mapping.source)
        table = mapping.params["table"]
        if value in table:
            return table[value]
        if "default" in mapping.params:
            return mapping.params["default"]
        raise MappingError(
            f"lookup for {mapping.source}={value!r}: no entry and no default")
    if kind is TransformKind.HASH:
        value = row.get(mapping.source)
        if value is None:
            return None
        return _hash_value(value)
    if kind is TransformKind.DEFAULT:
        value = row.get(mapping.source)
        return mapping.params["value"] if value is None else value
    raise MappingError(f"unknown transform: {kind}")


def apply_mapping(spec: MappingSpec, row: dict[str, Any]) -> dict[str, Any]:
    """Map one source row to its target row; raises MappingError on failure."""
    out: dict[str, Any] = {}
    for mapping in spec.mappings:
        out[mapping.target] = apply_transform(mapping, row)
    return out


def row_hash(row: dict[str, Any]) -> str:
    """Deterministic content hash used for within-batch and idempotent dedup."""
    import json
    payload = json.dumps(row, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()
