"""Data quality gates on ingest.

Rules run against mapped rows before they reach the target. Blocking
violations quarantine the row with a reason; non-blocking violations are
recorded as warnings and the row still lands. Batch-level rules
(null_rate, unique) are evaluated over the whole batch, so a systematic
source problem quarantines every affected row instead of letting a
partially-broken load through.
"""
from __future__ import annotations

from typing import Any

from dint.models import QualityRule, QualityViolation


def _check_row_rule(rule: QualityRule, row: dict[str, Any],
                    row_key: Any) -> QualityViolation | None:
    value = row.get(rule.column)
    if rule.kind == "not_null":
        if value is None:
            return QualityViolation(rule.rule_id, rule.kind, rule.column,
                                    f"{rule.column} is null", row_key)
    elif rule.kind == "range":
        low = rule.params.get("min")
        high = rule.params.get("max")
        if value is not None:
            if low is not None and value < low:
                return QualityViolation(rule.rule_id, rule.kind, rule.column,
                                        f"{rule.column}={value} below min {low}",
                                        row_key)
            if high is not None and value > high:
                return QualityViolation(rule.rule_id, rule.kind, rule.column,
                                        f"{rule.column}={value} above max {high}",
                                        row_key)
    elif rule.kind == "schema":
        expected = rule.params.get("type")
        if value is not None and expected:
            type_map = {"int": int, "float": (int, float), "str": str, "bool": bool}
            py_type = type_map.get(expected)
            if py_type is not None and not isinstance(value, py_type):
                return QualityViolation(rule.rule_id, rule.kind, rule.column,
                                        f"{rule.column}={value!r} is not {expected}",
                                        row_key)
    return None


def evaluate_quality(rows: list[dict[str, Any]], key_field: str,
                     rules: list[QualityRule]) -> tuple[list[QualityViolation],
                                                       list[QualityViolation]]:
    """Evaluate rules over a batch.

    Returns (blocking_violations, warnings). A row with any blocking
    violation is quarantined by the caller.
    """
    blocking: list[QualityViolation] = []
    warnings: list[QualityViolation] = []
    for rule in rules:
        if rule.kind == "null_rate":
            threshold = float(rule.params.get("max_rate", 0.05))
            nulls = sum(1 for r in rows if r.get(rule.column) is None)
            rate = nulls / len(rows) if rows else 0.0
            if rate > threshold:
                for r in rows:
                    if r.get(rule.column) is None:
                        violation = QualityViolation(
                            rule.rule_id, rule.kind, rule.column,
                            f"null rate {rate:.2%} on {rule.column} exceeds "
                            f"max {threshold:.2%}", r.get(key_field))
                        (blocking if rule.blocking else warnings).append(violation)
            continue
        if rule.kind == "unique":
            seen: dict[Any, dict[str, Any]] = {}
            for r in rows:
                value = r.get(rule.column)
                if value is None:
                    continue
                if value in seen:
                    violation = QualityViolation(
                        rule.rule_id, rule.kind, rule.column,
                        f"duplicate {rule.column}={value!r}", r.get(key_field))
                    (blocking if rule.blocking else warnings).append(violation)
                else:
                    seen[value] = r
            continue
        for r in rows:
            violation = _check_row_rule(rule, r, r.get(key_field))
            if violation is not None:
                (blocking if rule.blocking else warnings).append(violation)
    return blocking, warnings
