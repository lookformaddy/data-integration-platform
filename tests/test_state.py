"""State tests: checkpoints and the versioned target store."""
from datetime import datetime, timezone

from dint.state import CheckpointStore, TargetStore

NOW = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)


def test_checkpoint_defaults_to_none_watermark():
    store = CheckpointStore()
    checkpoint = store.get("orders-file")
    assert checkpoint.watermark is None
    assert checkpoint.rows_processed == 0


def test_checkpoint_save_and_get():
    store = CheckpointStore()
    store.save("orders-file", "2026-11-10T15:00:00+00:00", 40, "run-1", NOW)
    checkpoint = store.get("orders-file")
    assert checkpoint.watermark == "2026-11-10T15:00:00+00:00"
    assert checkpoint.rows_processed == 40
    assert checkpoint.last_run_id == "run-1"


def test_target_upsert_creates_version_one():
    store = TargetStore()
    target, changed = store.upsert("orders", "ORD-1", {"qty": 2}, NOW)
    assert changed is True
    assert target.version == 1
    assert store.get("orders", "ORD-1").row == {"qty": 2}


def test_target_upsert_same_content_is_noop():
    store = TargetStore()
    store.upsert("orders", "ORD-1", {"qty": 2}, NOW)
    target, changed = store.upsert("orders", "ORD-1", {"qty": 2}, NOW)
    assert changed is False
    assert target.version == 1


def test_target_upsert_changed_content_bumps_version():
    store = TargetStore()
    store.upsert("orders", "ORD-1", {"qty": 2}, NOW)
    target, changed = store.upsert("orders", "ORD-1", {"qty": 3}, NOW)
    assert changed is True
    assert target.version == 2
    assert target.row == {"qty": 3}


def test_target_history_keeps_versions():
    store = TargetStore()
    store.upsert("orders", "ORD-1", {"qty": 2}, NOW)
    store.upsert("orders", "ORD-1", {"qty": 3}, NOW)
    history = store.history("orders", "ORD-1")
    assert [h.version for h in history] == [1, 2]
    assert history[0].row == {"qty": 2}


def test_target_rows_and_tables():
    store = TargetStore()
    store.upsert("orders", "ORD-1", {"qty": 2}, NOW)
    store.upsert("customers", "C1", {"tier": "premium"}, NOW)
    assert len(store.rows("orders")) == 1
    assert store.tables() == ["customers", "orders"]
    assert store.get("orders", "missing") is None
