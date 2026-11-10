"""End-to-end demo: three connectors (file / API / table), declarative
mappings with transforms, watermarked incremental ingestion, quality
gates with reason-carrying quarantine, dry-run vs apply, retries with
backoff, idempotent re-runs, lineage and impact analysis, per-connector
metrics, and the hash-chained audit log.

Deterministic, offline, synthetic data only. Run with:
    PYTHONPATH=src python3 scripts/demo.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

os.environ.setdefault("DINT_HASH_SALT", "demo-salt")

from dint.connectors import synthetic_orders  # noqa: E402
from dint.models import ConnectorConfig, ConnectorKind, FieldMapping, MappingSpec, TransformKind  # noqa: E402
from dint.seed import build_seeded_engine as build_engine  # noqa: E402

NOW = datetime(2026, 11, 12, 9, 0, tzinfo=timezone.utc)


def main() -> None:
    engine = build_engine()

    # 1. Dry-run first: see exactly what would land, without writing.
    dry = engine.run("orders-file", dry_run=True, now=NOW)
    print(f"[dry-run] orders-file: read={dry.rows_read} "
          f"would_write={dry.rows_written} "
          f"quarantined={dry.rows_quarantined} status={dry.status.value}")

    # 2. Full load of all three connectors.
    totals = {"read": 0, "written": 0, "quarantined": 0}
    for connector_id in ("orders-file", "customers-api", "products-table"):
        record = engine.run(connector_id, now=NOW)
        totals["read"] += record.rows_read
        totals["written"] += record.rows_written
        totals["quarantined"] += record.rows_quarantined
        print(f"[load] {connector_id}: read={record.rows_read} "
              f"written={record.rows_written} "
              f"quarantined={record.rows_quarantined} "
              f"watermark={record.watermark_after} status={record.status.value}")

    # 3. Incremental re-run: nothing new, so nothing is written (idempotent).
    again = engine.run("orders-file", now=NOW)
    print(f"[incremental] orders-file re-run: read={again.rows_read} "
          f"written={again.rows_written} "
          f"skipped_duplicate={again.rows_skipped_duplicate} "
          f"status={again.status.value}")

    # 4. A late-arriving bad row: negative qty fails the quality gate and
    #    is quarantined with its reason; the good row still lands.
    orders = engine.connectors["orders-file"]
    orders._rows.append({
        "order_id": "ORD-9999", "customer_ref": "CUST-0001",
        "product_sku": "SKU-01", "qty": -3, "unit_price": 12.5,
        "region_code": "north", "status": "shipped",
        "updated_at": "2026-11-11T12:00:00+00:00", "seq": 9999})
    orders._rows.append({
        "order_id": "ORD-9998", "customer_ref": "CUST-0002",
        "product_sku": "SKU-02", "qty": 2, "unit_price": 24.0,
        "region_code": "south", "status": "shipped",
        "updated_at": "2026-11-11T12:00:00+00:00", "seq": 9998})
    bad = engine.run("orders-file", now=NOW)
    print(f"[quality] late rows: read={bad.rows_read} written={bad.rows_written} "
          f"quarantined={bad.rows_quarantined} status={bad.status.value}")
    for q in engine.quarantine:
        if q.row_key in ("ORD-9999",):
            print(f"    quarantined {q.row_key} ({q.stage}): {q.reason}")

    # 5. A flaky source: two transient failures, then success via retries.
    engine.register_connector(ConnectorConfig(
        connector_id="orders-retry", kind=ConnectorKind.FILE,
        display_name="Orders CSV (flaky)", source_name="orders.csv",
        schema=["order_id", "customer_ref", "product_sku", "qty", "unit_price",
                "region_code", "status", "updated_at", "seq"],
        watermark_field="updated_at", key_field="order_id",
        max_retries=3, retry_backoff_seconds=0.0),
        rows=synthetic_orders(), fail_times=2)
    engine.register_mapping(MappingSpec(
        connector_id="orders-retry", target_table="orders_retry",
        mappings=[FieldMapping("order_id", "order_id"),
                  FieldMapping("qty", "quantity", TransformKind.CAST,
                               {"to": "int"})]))
    retry = engine.run("orders-retry", now=NOW, sleep_fn=lambda _s: None)
    print(f"[retries] orders-retry: attempts={retry.attempts} "
          f"written={retry.rows_written} status={retry.status.value}")

    # 6. Lineage: where does orders.region come from, and what breaks if
    #    the source region_code changes?
    edges = engine.lineage.edges_for_target("orders", "region")
    print(f"[lineage] orders.region <- "
          f"{[(e.source_connector, e.source_column, e.transform) for e in edges]}")
    impact = engine.lineage.impact("orders-file", "region_code")
    print(f"[impact] region_code change affects "
          f"{[(e.target_table, e.target_column) for e in impact.affected_targets]} "
          f"jobs={impact.affected_jobs}")

    # 7. Metrics + audit.
    for cid, metrics in engine.metrics.items():
        print(f"[metrics] {cid}: runs={metrics.runs} "
              f"read={metrics.rows_read} written={metrics.rows_written} "
              f"quarantined={metrics.rows_quarantined} "
              f"failures={metrics.failures} "
              f"lag={metrics.last_lag_seconds}")
    print(f"[targets] orders={len(engine.targets.rows('orders'))} rows, "
          f"customers={len(engine.targets.rows('customers'))} rows, "
          f"products={len(engine.targets.rows('products'))} rows")
    print(f"[audit] entries={len(engine.audit.entries)} "
          f"chain_valid={engine.audit.verify()}")
    print(f"[summary] total_read={totals['read']} "
          f"total_written={totals['written']} "
          f"total_quarantined={totals['quarantined']} demo complete")


if __name__ == "__main__":
    main()
