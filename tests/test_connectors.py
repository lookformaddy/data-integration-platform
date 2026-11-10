"""Connector tests: interface, watermark reads, pagination, credentials,
schema validation, transient-failure simulation."""
import os

import pytest

from dint.connectors import (
    ApiConnector,
    FileConnector,
    TableConnector,
    build_connector,
    synthetic_customers,
    synthetic_orders,
    synthetic_products,
)
from dint.models import ConnectorError, CredentialError
from tests.conftest import orders_config


def test_file_connector_full_load():
    connector = FileConnector(orders_config(), rows=synthetic_orders())
    rows = connector.read_since(None)
    assert len(rows) == 40
    assert rows[0]["updated_at"] <= rows[-1]["updated_at"]  # sorted by watermark


def test_file_connector_watermark_filters():
    connector = FileConnector(orders_config(), rows=synthetic_orders())
    all_rows = connector.read_since(None)
    mid = all_rows[20]["updated_at"]
    newer = connector.read_since(mid)
    assert 0 < len(newer) < 40
    assert all(r["updated_at"] > mid for r in newer)


def test_api_connector_paginates_all_rows():
    connector = ApiConnector(
        orders_config(connector_id="customers-api", key_field="customer_id"),
        rows=synthetic_customers(), page_size=7)
    assert len(connector.read_since(None)) == 30


def test_table_connector_full_load():
    connector = TableConnector(
        orders_config(connector_id="products-table", key_field="sku"),
        rows=synthetic_products())
    assert len(connector.read_since(None)) == 18


def test_build_connector_factory():
    assert isinstance(build_connector(orders_config()), FileConnector)


def test_build_connector_unknown_kind():
    config = orders_config()
    config.kind = "stream"  # type: ignore[assignment]
    with pytest.raises(ConnectorError):
        build_connector(config)


def test_credential_missing_fails_closed(monkeypatch):
    monkeypatch.delenv("DINT_ORDERS_API_KEY", raising=False)
    config = orders_config(credential_env="DINT_ORDERS_API_KEY")
    connector = FileConnector(config, rows=synthetic_orders())
    with pytest.raises(CredentialError):
        connector.resolve_credential()


def test_credential_present_from_env(monkeypatch):
    monkeypatch.setenv("DINT_ORDERS_API_KEY", "secret-token")
    config = orders_config(credential_env="DINT_ORDERS_API_KEY")
    connector = FileConnector(config, rows=synthetic_orders())
    assert connector.resolve_credential() == "secret-token"


def test_no_credential_required_returns_none():
    connector = FileConnector(orders_config(), rows=synthetic_orders())
    assert connector.resolve_credential() is None


def test_schema_validation_rejects_missing_column():
    bad = synthetic_orders()[:1]
    del bad[0]["qty"]
    connector = FileConnector(orders_config(), rows=bad)
    with pytest.raises(ConnectorError, match="missing declared columns"):
        connector.validate_schema(connector.read_since(None))


def test_transient_failure_then_success():
    connector = FileConnector(orders_config(), rows=synthetic_orders(),
                              fail_times=2)
    with pytest.raises(ConnectorError):
        connector.read_since(None)
    with pytest.raises(ConnectorError):
        connector.read_since(None)
    assert len(connector.read_since(None)) == 40
