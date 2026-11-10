"""Audit log tests: append, hash chain, tamper detection."""
from dint.audit import AuditLog


def test_append_and_verify():
    log = AuditLog()
    log.append("run.completed", "engine", "run-1", {"rows": 40})
    log.append("run.completed", "engine", "run-2", {"rows": 30})
    assert len(log.entries) == 2
    assert log.verify() is True
    assert log.entries[0].prev_hash == "GENESIS"
    assert log.entries[1].prev_hash == log.entries[0].entry_hash


def test_tamper_detected():
    log = AuditLog()
    log.append("run.completed", "engine", "run-1", {"rows": 40})
    log.append("run.completed", "engine", "run-2", {"rows": 30})
    log.entries[0].details["rows"] = 999
    assert log.verify() is False


def test_deletion_detected():
    log = AuditLog()
    log.append("run.completed", "engine", "run-1", {})
    log.append("run.completed", "engine", "run-2", {})
    del log.entries[0]
    assert log.verify() is False


def test_for_run_filters():
    log = AuditLog()
    log.append("run.completed", "engine", "run-1", {})
    log.append("run.completed", "engine", "run-2", {})
    assert len(log.for_run("run-1")) == 1
    assert log.for_run("run-1")[0].run_id == "run-1"
