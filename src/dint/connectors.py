"""Pluggable source connectors behind a common interface.

Every connector yields rows as plain dicts plus a watermark value, so the
engine can treat a CSV file, a paginated JSON API, and a database table the
same way. Credentials are resolved from environment variables only — never
from code or config files — and a missing credential fails the run closed.

The synthetic data here is generated deterministically in-memory; in
production each connector would read from its real system (file store,
HTTP API, JDBC/ODBC) and only the read() implementation would change.
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any

from dint.models import (
    ConnectorConfig,
    ConnectorError,
    ConnectorKind,
    CredentialError,
)

BASE = datetime(2026, 11, 1, 9, 0, tzinfo=timezone.utc)


def _ts(day_offset: int, hour: int = 9) -> str:
    return (BASE + timedelta(days=day_offset, hours=hour)).isoformat()


def synthetic_orders() -> list[dict[str, Any]]:
    """CSV-file-style source: order events, one row per order line."""
    regions = ["north", "south", "east", "west"]
    rows = []
    for i in range(40):
        rows.append({
            "order_id": f"ORD-{1000 + i}",
            "customer_ref": f"CUST-{i % 12:04d}",
            "product_sku": f"SKU-{(i % 6) + 1:02d}",
            "qty": (i % 5) + 1,
            "unit_price": round(9.99 + (i % 8) * 4.5, 2),
            "region_code": regions[i % 4],
            "status": "shipped" if i % 7 else "cancelled",
            "updated_at": _ts(i % 10, 8 + (i % 8)),
            "seq": 1000 + i,
        })
    return rows


def synthetic_customers() -> list[dict[str, Any]]:
    """JSON-API-style source: customer master records, paginated."""
    tiers = ["standard", "premium", "enterprise"]
    rows = []
    for i in range(30):
        rows.append({
            "customer_id": f"CUST-{i:04d}",
            "full_name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "tier": tiers[i % 3],
            "country": ["US", "CA", "GB", "DE"][i % 4],
            "signup_date": (BASE + timedelta(days=i % 20)).date().isoformat(),
            "updated_at": _ts(i % 10, 10 + (i % 6)),
            "seq": 2000 + i,
        })
    return rows


def synthetic_products() -> list[dict[str, Any]]:
    """Database-table-style source: product catalogue rows."""
    rows = []
    for i in range(18):
        rows.append({
            "sku": f"SKU-{i + 1:02d}",
            "product_name": f"Product {i + 1}",
            "category": ["electronics", "home", "apparel"][i % 3],
            "list_price": round(19.99 + i * 3.25, 2),
            "in_stock": i % 4 != 0,
            "updated_at": _ts(i % 8, 7 + (i % 5)),
            "seq": 3000 + i,
        })
    return rows


class SourceConnector(ABC):
    """Common interface every source connector implements."""

    def __init__(self, config: ConnectorConfig, fail_times: int = 0):
        self.config = config
        # Test hook: simulate transient source failures before succeeding.
        self._fail_times = fail_times
        self._attempts = 0

    @property
    def connector_id(self) -> str:
        return self.config.connector_id

    def resolve_credential(self) -> str | None:
        """Return the credential from the environment, or fail closed."""
        env_name = self.config.credential_env
        if env_name is None:
            return None
        value = os.environ.get(env_name)
        if not value:
            raise CredentialError(
                f"connector {self.connector_id}: credential env var "
                f"{env_name} is not set")
        return value

    def _maybe_fail_transiently(self) -> None:
        self._attempts += 1
        if self._attempts <= self._fail_times:
            raise ConnectorError(
                f"connector {self.connector_id}: transient source failure "
                f"(attempt {self._attempts})")

    @abstractmethod
    def read(self, since_watermark: Any = None) -> list[dict[str, Any]]:
        """Return rows newer than ``since_watermark`` (None = full load)."""

    def read_since(self, since_watermark: Any = None) -> list[dict[str, Any]]:
        self._maybe_fail_transiently()
        rows = self.read(since_watermark)
        return sorted(rows, key=lambda r: r.get(self.config.watermark_field))

    def validate_schema(self, rows: list[dict[str, Any]]) -> None:
        """Fail closed if a row is missing a declared source column."""
        for row in rows:
            missing = [c for c in self.config.schema if c not in row]
            if missing:
                raise ConnectorError(
                    f"connector {self.connector_id}: row {row.get(self.config.key_field)} "
                    f"is missing declared columns {missing}")


class FileConnector(SourceConnector):
    """CSV-file-style connector: reads synthetic order rows."""

    def __init__(self, config: ConnectorConfig, rows: list[dict[str, Any]] | None = None,
                 fail_times: int = 0):
        super().__init__(config, fail_times)
        self._rows = rows if rows is not None else synthetic_orders()

    def read(self, since_watermark: Any = None) -> list[dict[str, Any]]:
        rows = list(self._rows)
        if since_watermark is not None:
            rows = [r for r in rows
                    if r.get(self.config.watermark_field) is not None
                    and r[self.config.watermark_field] > since_watermark]
        return rows


class ApiConnector(SourceConnector):
    """JSON-API-style connector: paginated reads over synthetic customers."""

    def __init__(self, config: ConnectorConfig, rows: list[dict[str, Any]] | None = None,
                 fail_times: int = 0, page_size: int = 10):
        super().__init__(config, fail_times)
        self._rows = rows if rows is not None else synthetic_customers()
        self.page_size = page_size

    def read(self, since_watermark: Any = None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        page = 0
        while True:
            chunk = self._rows[page * self.page_size:(page + 1) * self.page_size]
            if not chunk:
                break
            page += 1
            for row in chunk:
                if since_watermark is None or (
                        row.get(self.config.watermark_field) is not None
                        and row[self.config.watermark_field] > since_watermark):
                    out.append(row)
        return out


class TableConnector(SourceConnector):
    """Database-table-style connector: reads synthetic product rows."""

    def __init__(self, config: ConnectorConfig, rows: list[dict[str, Any]] | None = None,
                 fail_times: int = 0):
        super().__init__(config, fail_times)
        self._rows = rows if rows is not None else synthetic_products()

    def read(self, since_watermark: Any = None) -> list[dict[str, Any]]:
        rows = list(self._rows)
        if since_watermark is not None:
            rows = [r for r in rows
                    if r.get(self.config.watermark_field) is not None
                    and r[self.config.watermark_field] > since_watermark]
        return rows


def build_connector(config: ConnectorConfig, **kwargs) -> SourceConnector:
    """Factory: build the connector implementation for a config's kind."""
    if config.kind == ConnectorKind.FILE:
        return FileConnector(config, **kwargs)
    if config.kind == ConnectorKind.API:
        return ApiConnector(config, **kwargs)
    if config.kind == ConnectorKind.TABLE:
        return TableConnector(config, **kwargs)
    raise ConnectorError(f"unknown connector kind: {config.kind}")
