"""Pytest configuration: put the src layout on the import path so plain
``pytest`` passes from the project root, with or without an install."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

"""Shared fixtures: a seeded integration engine with the three demo
connectors (orders file, customers API, products table) and their
mappings and quality rules, mirroring scripts/demo.py."""
from datetime import datetime, timezone

import pytest

from dint.audit import AuditLog
from dint.engine import IntegrationEngine
from dint.models import (
    ConnectorConfig,
    ConnectorKind,
    FieldMapping,
    MappingSpec,
    QualityRule,
    TransformKind,
)

NOW = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)

ORDERS_SCHEMA = ["order_id", "customer_ref", "product_sku", "qty", "unit_price",
                 "region_code", "status", "updated_at", "seq"]


def orders_config(**overrides) -> ConnectorConfig:
    kwargs = {
        "connector_id": "orders-file", "kind": ConnectorKind.FILE,
        "display_name": "Orders CSV file", "source_name": "orders.csv",
        "schema": ORDERS_SCHEMA, "watermark_field": "updated_at",
        "key_field": "order_id",
    }
    kwargs.update(overrides)
    return ConnectorConfig(**kwargs)


def orders_mapping() -> MappingSpec:
    return MappingSpec(
        connector_id="orders-file", target_table="orders",
        mappings=[
            FieldMapping("order_id", "order_id"),
            FieldMapping("customer_ref", "customer_id"),
            FieldMapping("product_sku", "sku"),
            FieldMapping("qty", "quantity", TransformKind.CAST, {"to": "int"}),
            FieldMapping("unit_price", "unit_price", TransformKind.CAST,
                         {"to": "float"}),
            FieldMapping("region_code", "region", TransformKind.LOOKUP,
                         {"table": {"north": "North", "south": "South",
                                    "east": "East", "west": "West"},
                          "default": "Unknown"}),
            FieldMapping("status", "status"),
            FieldMapping("updated_at", "updated_at"),
        ])


def orders_rules() -> list[QualityRule]:
    return [
        QualityRule("orders-qty-positive", "range", "quantity",
                    params={"min": 1}),
        QualityRule("orders-price-positive", "range", "unit_price",
                    params={"min": 0.01}),
        QualityRule("orders-region-present", "not_null", "region"),
    ]


@pytest.fixture
def engine() -> IntegrationEngine:
    from scripts.demo import build_engine
    return build_engine()


@pytest.fixture
def empty_engine() -> IntegrationEngine:
    return IntegrationEngine(audit=AuditLog())

