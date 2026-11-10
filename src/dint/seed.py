"""Seeded engine: the three demo connectors (orders file, customers API,
products table) with their mappings and quality rules.

Shared by the FastAPI service and the CLI so neither duplicates the
seed definition — and so the CLI stays pure stdlib (no FastAPI import).
"""
from __future__ import annotations

from dint.audit import AuditLog
from dint.connectors import synthetic_customers, synthetic_orders, synthetic_products
from dint.engine import IntegrationEngine
from dint.models import (
    ConnectorConfig,
    ConnectorKind,
    FieldMapping,
    MappingSpec,
    QualityRule,
    TransformKind,
)


def build_seeded_engine() -> IntegrationEngine:
    engine = IntegrationEngine(audit=AuditLog())
    engine.register_connector(ConnectorConfig(
        connector_id="orders-file", kind=ConnectorKind.FILE,
        display_name="Orders CSV file", source_name="orders.csv",
        schema=["order_id", "customer_ref", "product_sku", "qty", "unit_price",
                "region_code", "status", "updated_at", "seq"],
        watermark_field="updated_at", key_field="order_id"),
        rows=synthetic_orders())
    engine.register_connector(ConnectorConfig(
        connector_id="customers-api", kind=ConnectorKind.API,
        display_name="Customers JSON API", source_name="customers",
        schema=["customer_id", "full_name", "email", "tier", "country",
                "signup_date", "updated_at", "seq"],
        watermark_field="updated_at", key_field="customer_id"),
        rows=synthetic_customers())
    engine.register_connector(ConnectorConfig(
        connector_id="products-table", kind=ConnectorKind.TABLE,
        display_name="Products database table", source_name="products",
        schema=["sku", "product_name", "category", "list_price", "in_stock",
                "updated_at", "seq"],
        watermark_field="updated_at", key_field="sku"),
        rows=synthetic_products())
    engine.register_mapping(MappingSpec(
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
        ]))
    engine.register_mapping(MappingSpec(
        connector_id="customers-api", target_table="customers",
        mappings=[
            FieldMapping("customer_id", "customer_id"),
            FieldMapping("full_name", "full_name"),
            FieldMapping("email", "email_hash", TransformKind.HASH),
            FieldMapping("tier", "tier"),
            FieldMapping("country", "country"),
            FieldMapping("signup_date", "signup_date", TransformKind.CAST,
                         {"to": "date"}),
            FieldMapping("updated_at", "updated_at"),
        ]))
    engine.register_mapping(MappingSpec(
        connector_id="products-table", target_table="products",
        mappings=[
            FieldMapping("sku", "sku"),
            FieldMapping("product_name", "product_name"),
            FieldMapping("category", "category"),
            FieldMapping("list_price", "list_price", TransformKind.CAST,
                         {"to": "float"}),
            FieldMapping("in_stock", "in_stock", TransformKind.CAST,
                         {"to": "bool"}),
            FieldMapping("updated_at", "updated_at"),
        ]))
    engine.register_quality_rules("orders-file", [
        QualityRule("orders-qty-positive", "range", "quantity",
                    params={"min": 1}),
        QualityRule("orders-price-positive", "range", "unit_price",
                    params={"min": 0.01}),
        QualityRule("orders-region-present", "not_null", "region"),
    ])
    engine.register_quality_rules("customers-api", [
        QualityRule("customers-tier-present", "not_null", "tier"),
        QualityRule("customers-email-present", "not_null", "email_hash"),
    ])
    engine.register_quality_rules("products-table", [
        QualityRule("products-price-positive", "range", "list_price",
                    params={"min": 0.01}),
        QualityRule("products-name-present", "not_null", "product_name"),
    ])
    return engine
