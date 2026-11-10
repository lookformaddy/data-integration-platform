"""Engine tests: full loads, incremental runs, dry-run, quarantine,
idempotency, retries, credential failures, metrics, audit trail."""
import pytest

from dint.models import (
    ConnectorConfig,
    ConnectorKind,
    FieldMapping,
    MappingSpec,
    QualityRule,
    RunStatus,
    TransformKind,
)
from tests.conftest import NOW, orders_config, orders_mapping, orders_rules


def test_full_load_writes_all_orders(engine):
    record = engine.run("orders-file", now=NOW)
    assert record.status == RunStatus.SUCCEEDED
    assert record.rows_read == 40
    assert record.rows_written == 40
    assert record.rows_quarantined == 0
    assert len(engine.targets.rows("orders")) == 40
    assert record.watermark_after is not None


def test_incremental_rerun_reads_nothing(engine):
    engine.run("orders-file", now=NOW)
    again = engine.run("orders-file", now=NOW)
    assert again.rows_read == 0
    assert again.rows_written == 0
    assert again.status == RunStatus.SUCCEEDED


def test_dry_run_writes_nothing_and_advances_nothing(engine):
    record = engine.run("orders-file", dry_run=True, now=NOW)
    assert record.status == RunStatus.DRY_RUN
    assert record.rows_read == 40
    assert record.rows_written == 40  # would write
    assert len(engine.targets.rows("orders")) == 0
    assert engine.checkpoints.get("orders-file").watermark is None
    # A real run afterwards still loads everything.
    real = engine.run("orders-file", now=NOW)
    assert real.rows_written == 40


def test_quality_gate_quarantines_bad_row(engine):
    engine.run("orders-file", now=NOW)
    orders = engine.connectors["orders-file"]
    orders._rows.append({
        "order_id": "ORD-9999", "customer_ref": "CUST-0001",
        "product_sku": "SKU-01", "qty": -3, "unit_price": 12.5,
        "region_code": "north", "status": "shipped",
        "updated_at": "2026-11-11T12:00:00+00:00", "seq": 9999})
    record = engine.run("orders-file", now=NOW)
    assert record.status == RunStatus.PARTIAL
    assert record.rows_read == 1
    assert record.rows_written == 0
    assert record.rows_quarantined == 1
    assert any(q.row_key == "ORD-9999" and q.stage == "quality"
               for q in engine.quarantine)


def test_mapping_failure_quarantines_row(empty_engine):
    from dint.connectors import synthetic_orders
    empty_engine.register_connector(orders_config(), rows=synthetic_orders())
    bad = MappingSpec("orders-file", "orders", [
        FieldMapping("qty", "quantity", TransformKind.CAST, {"to": "int"})])
    # Force a cast failure: a qty value that cannot cast.
    empty_engine.connectors["orders-file"]._rows[0]["qty"] = "not-a-number"
    empty_engine.register_mapping(bad)
    record = empty_engine.run("orders-file", now=NOW)
    assert record.rows_quarantined >= 1
    assert any(q.stage == "mapping" for q in empty_engine.quarantine)


def test_duplicate_rows_within_batch_deduped(empty_engine):
    from dint.connectors import synthetic_orders
    rows = synthetic_orders()[:5]
    rows.append(dict(rows[0]))  # exact duplicate row
    empty_engine.register_connector(orders_config(), rows=rows)
    empty_engine.register_mapping(orders_mapping())
    record = empty_engine.run("orders-file", now=NOW)
    assert record.rows_read == 6
    assert record.rows_written == 5
    assert record.rows_skipped_duplicate == 1


def test_retry_then_success(empty_engine):
    from dint.connectors import synthetic_orders
    empty_engine.register_connector(
        orders_config(max_retries=3, retry_backoff_seconds=0.0),
        rows=synthetic_orders(), fail_times=2)
    empty_engine.register_mapping(orders_mapping())
    record = empty_engine.run("orders-file", now=NOW, sleep_fn=lambda _s: None)
    assert record.status == RunStatus.SUCCEEDED
    assert record.attempts == 3
    assert record.rows_written == 40


def test_retries_exhausted_fails_run(empty_engine):
    from dint.connectors import synthetic_orders
    empty_engine.register_connector(
        orders_config(max_retries=2, retry_backoff_seconds=0.0),
        rows=synthetic_orders(), fail_times=5)
    empty_engine.register_mapping(orders_mapping())
    record = empty_engine.run("orders-file", now=NOW, sleep_fn=lambda _s: None)
    assert record.status == RunStatus.FAILED
    assert "transient source failure" in record.error
    assert record.attempts == 2
    assert empty_engine.metrics["orders-file"].failures == 1


def test_missing_credential_fails_run_closed(empty_engine, monkeypatch):
    from dint.connectors import synthetic_orders
    monkeypatch.delenv("DINT_ORDERS_API_KEY", raising=False)
    empty_engine.register_connector(
        orders_config(credential_env="DINT_ORDERS_API_KEY"),
        rows=synthetic_orders())
    empty_engine.register_mapping(orders_mapping())
    record = empty_engine.run("orders-file", now=NOW)
    assert record.status == RunStatus.FAILED
    assert "credential env var" in record.error
    assert len(empty_engine.targets.rows("orders")) == 0


def test_unknown_connector_run_fails(empty_engine):
    record = empty_engine.run("nope", now=NOW)
    assert record.status == RunStatus.FAILED
    assert "unknown connector" in record.error


def test_run_without_mapping_fails(empty_engine):
    from dint.connectors import synthetic_orders
    empty_engine.register_connector(orders_config(), rows=synthetic_orders())
    record = empty_engine.run("orders-file", now=NOW)
    assert record.status == RunStatus.FAILED
    assert "no mapping registered" in record.error


def test_invalid_mapping_rejected_at_registration(empty_engine):
    from dint.connectors import synthetic_orders
    empty_engine.register_connector(orders_config(), rows=synthetic_orders())
    with pytest.raises(Exception, match="not in the source schema"):
        empty_engine.register_mapping(MappingSpec(
            "orders-file", "orders", [FieldMapping("nope", "x")]))


def test_run_history_and_metrics(engine):
    engine.run("orders-file", now=NOW)
    engine.run("customers-api", now=NOW)
    assert len(engine.run_history()) == 2
    assert len(engine.run_history("orders-file")) == 1
    metrics = engine.metrics["orders-file"]
    assert metrics.runs == 1
    assert metrics.rows_written == 40
    assert metrics.last_lag_seconds is not None
    assert metrics.last_lag_seconds >= 0


def test_audit_log_records_runs(engine):
    engine.run("orders-file", now=NOW)
    actions = [e.action for e in engine.audit.entries]
    assert "run.completed" in actions
    assert engine.audit.verify() is True


def test_customers_api_hashes_email(engine):
    engine.run("customers-api", now=NOW)
    rows = engine.targets.rows("customers")
    assert len(rows) == 30
    assert all(r.row["email_hash"].startswith("hash:") for r in rows)
    assert all("email" not in r.row for r in rows)
