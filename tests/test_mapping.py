"""Mapping tests: validation (fail-closed), every transform, row mapping."""
import pytest

from dint.mapping import apply_mapping, apply_transform, row_hash, validate_mapping
from dint.models import FieldMapping, MappingError, MappingSpec, TransformKind
from tests.conftest import ORDERS_SCHEMA, orders_mapping


def test_validate_ok():
    validate_mapping(orders_mapping(), ORDERS_SCHEMA)


def test_validate_empty_mappings():
    with pytest.raises(MappingError, match="at least one field mapping"):
        validate_mapping(MappingSpec("orders-file", "orders", []), ORDERS_SCHEMA)


def test_validate_unknown_source_column():
    spec = MappingSpec("orders-file", "orders",
                       [FieldMapping("nope", "x")])
    with pytest.raises(MappingError, match="not in the source schema"):
        validate_mapping(spec, ORDERS_SCHEMA)


def test_validate_duplicate_target():
    spec = MappingSpec("orders-file", "orders",
                       [FieldMapping("order_id", "x"),
                        FieldMapping("qty", "x")])
    with pytest.raises(MappingError, match="duplicate target column"):
        validate_mapping(spec, ORDERS_SCHEMA)


def test_validate_cast_bad_target():
    spec = MappingSpec("orders-file", "orders",
                       [FieldMapping("qty", "q", TransformKind.CAST,
                                     {"to": "weird"})])
    with pytest.raises(MappingError, match="cast requires"):
        validate_mapping(spec, ORDERS_SCHEMA)


def test_validate_lookup_requires_table():
    spec = MappingSpec("orders-file", "orders",
                       [FieldMapping("region_code", "r", TransformKind.LOOKUP,
                                     {})])
    with pytest.raises(MappingError, match="lookup requires"):
        validate_mapping(spec, ORDERS_SCHEMA)


def test_validate_concat_requires_fields():
    spec = MappingSpec("orders-file", "orders",
                       [FieldMapping("order_id", "label", TransformKind.CONCAT,
                                     {})])
    with pytest.raises(MappingError, match="concat requires"):
        validate_mapping(spec, ORDERS_SCHEMA)


def test_validate_concat_unknown_field():
    spec = MappingSpec("orders-file", "orders",
                       [FieldMapping("order_id", "label", TransformKind.CONCAT,
                                     {"fields": ["order_id", "nope"]})])
    with pytest.raises(MappingError, match="not in the source schema"):
        validate_mapping(spec, ORDERS_SCHEMA)


def test_cast_int_float_str_bool_date():
    row = {"a": "42", "b": "3.5", "c": 7, "d": "true", "e": "2026-11-10"}
    assert apply_transform(FieldMapping("a", "a", TransformKind.CAST, {"to": "int"}), row) == 42
    assert apply_transform(FieldMapping("b", "b", TransformKind.CAST, {"to": "float"}), row) == 3.5
    assert apply_transform(FieldMapping("c", "c", TransformKind.CAST, {"to": "str"}), row) == "7"
    assert apply_transform(FieldMapping("d", "d", TransformKind.CAST, {"to": "bool"}), row) is True
    assert apply_transform(FieldMapping("e", "e", TransformKind.CAST, {"to": "date"}), row) == "2026-11-10"


def test_cast_invalid_value_raises():
    with pytest.raises(MappingError, match="cast to int failed"):
        apply_transform(FieldMapping("a", "a", TransformKind.CAST, {"to": "int"}),
                        {"a": "not-a-number"})


def test_cast_none_stays_none():
    assert apply_transform(FieldMapping("a", "a", TransformKind.CAST, {"to": "int"}),
                           {"a": None}) is None


def test_concat_transform():
    mapping = FieldMapping("order_id", "label", TransformKind.CONCAT,
                           {"fields": ["order_id", "region_code"],
                            "separator": "-"})
    assert apply_transform(mapping, {"order_id": "ORD-1", "region_code": "north"}) == "ORD-1-north"


def test_concat_skips_null_parts():
    mapping = FieldMapping("order_id", "label", TransformKind.CONCAT,
                           {"fields": ["order_id", "region_code"]})
    assert apply_transform(mapping, {"order_id": "ORD-1", "region_code": None}) == "ORD-1"


def test_lookup_hit_and_default():
    mapping = FieldMapping("region_code", "region", TransformKind.LOOKUP,
                           {"table": {"north": "North"}, "default": "Unknown"})
    assert apply_transform(mapping, {"region_code": "north"}) == "North"
    assert apply_transform(mapping, {"region_code": "west"}) == "Unknown"


def test_lookup_missing_without_default_raises():
    mapping = FieldMapping("region_code", "region", TransformKind.LOOKUP,
                           {"table": {"north": "North"}})
    with pytest.raises(MappingError, match="no entry and no default"):
        apply_transform(mapping, {"region_code": "west"})


def test_hash_is_deterministic_and_prefixed(monkeypatch):
    monkeypatch.setenv("DINT_HASH_SALT", "test-salt")
    mapping = FieldMapping("email", "email_hash", TransformKind.HASH)
    first = apply_transform(mapping, {"email": "a@example.com"})
    second = apply_transform(mapping, {"email": "a@example.com"})
    assert first == second
    assert first.startswith("hash:")
    assert apply_transform(mapping, {"email": "b@example.com"}) != first


def test_default_transform():
    mapping = FieldMapping("status", "status", TransformKind.DEFAULT,
                           {"value": "pending"})
    assert apply_transform(mapping, {"status": None}) == "pending"
    assert apply_transform(mapping, {"status": "shipped"}) == "shipped"


def test_apply_mapping_full_row():
    row = {"order_id": "ORD-1000", "customer_ref": "CUST-0000",
           "product_sku": "SKU-01", "qty": "3", "unit_price": "14.49",
           "region_code": "north", "status": "shipped",
           "updated_at": "2026-11-05T10:00:00+00:00", "seq": 1000}
    out = apply_mapping(orders_mapping(), row)
    assert out["order_id"] == "ORD-1000"
    assert out["quantity"] == 3 and isinstance(out["quantity"], int)
    assert out["unit_price"] == 14.49
    assert out["region"] == "North"


def test_row_hash_deterministic_and_content_sensitive():
    row = {"a": 1, "b": "x"}
    assert row_hash(row) == row_hash({"b": "x", "a": 1})
    assert row_hash(row) != row_hash({"a": 2, "b": "x"})
