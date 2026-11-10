"""Quality gate tests: each rule kind, blocking vs warning, batch rules."""
from dint.models import QualityRule
from dint.quality import evaluate_quality

ROWS = [
    {"order_id": "O1", "quantity": 2, "unit_price": 10.0, "region": "North"},
    {"order_id": "O2", "quantity": 0, "unit_price": 5.0, "region": None},
    {"order_id": "O3", "quantity": 1, "unit_price": -1.0, "region": "South"},
    {"order_id": "O1", "quantity": 4, "unit_price": 7.0, "region": "East"},
]


def test_clean_rows_pass():
    blocking, warnings = evaluate_quality(
        [ROWS[0]], "order_id",
        [QualityRule("r1", "range", "quantity", params={"min": 1}),
         QualityRule("r2", "not_null", "region")])
    assert blocking == [] and warnings == []


def test_range_below_min_blocks():
    blocking, _ = evaluate_quality(
        ROWS, "order_id",
        [QualityRule("qty", "range", "quantity", params={"min": 1})])
    assert [v.row_key for v in blocking] == ["O2"]
    assert "below min" in blocking[0].reason


def test_range_above_max_blocks():
    blocking, _ = evaluate_quality(
        ROWS, "order_id",
        [QualityRule("price", "range", "unit_price", params={"max": 9.0})])
    assert [v.row_key for v in blocking] == ["O1"]


def test_not_null_blocks():
    blocking, _ = evaluate_quality(
        ROWS, "order_id", [QualityRule("region", "not_null", "region")])
    assert [v.row_key for v in blocking] == ["O2"]


def test_unique_flags_duplicates():
    blocking, _ = evaluate_quality(
        ROWS, "order_id", [QualityRule("uniq", "unique", "order_id")])
    assert len(blocking) == 1
    assert blocking[0].row_key == "O1"
    assert "duplicate" in blocking[0].reason


def test_null_rate_batch_rule():
    rows = [{"id": i, "region": None if i < 3 else "North"} for i in range(10)]
    blocking, _ = evaluate_quality(
        rows, "id",
        [QualityRule("nr", "null_rate", "region", params={"max_rate": 0.2})])
    assert len(blocking) == 3  # every null row is quarantined
    assert all("null rate" in v.reason for v in blocking)


def test_null_rate_under_threshold_passes():
    rows = [{"id": i, "region": None if i == 0 else "North"} for i in range(10)]
    blocking, _ = evaluate_quality(
        rows, "id",
        [QualityRule("nr", "null_rate", "region", params={"max_rate": 0.2})])
    assert blocking == []


def test_non_blocking_rule_is_warning_only():
    blocking, warnings = evaluate_quality(
        ROWS, "order_id",
        [QualityRule("qty", "range", "quantity", params={"min": 1},
                     blocking=False)])
    assert blocking == []
    assert len(warnings) == 1


def test_schema_type_rule():
    blocking, _ = evaluate_quality(
        [{"order_id": "O1", "quantity": "two"}], "order_id",
        [QualityRule("s", "schema", "quantity", params={"type": "int"})])
    assert len(blocking) == 1
    assert "is not int" in blocking[0].reason


def test_empty_batch_passes():
    blocking, warnings = evaluate_quality(
        [], "order_id", [QualityRule("r", "not_null", "region")])
    assert blocking == [] and warnings == []
