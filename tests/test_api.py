"""FastAPI tests. These run in the venv (FastAPI installed); on a plain
system python without FastAPI they skip, per the project conventions."""
import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from dint.service import app  # noqa: E402

client = TestClient(app)


def test_list_connectors():
    resp = client.get("/connectors")
    assert resp.status_code == 200
    ids = {c["connector_id"] for c in resp.json()}
    assert ids == {"orders-file", "customers-api", "products-table"}


def test_run_job_dry_run_then_apply():
    resp = client.post("/jobs/products-table/run", json={"dry_run": True})
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "dry_run"
    assert body["rows_read"] == 18
    assert body["rows_written"] == 18
    assert client.get("/targets/products").json()["row_count"] == 0
    resp = client.post("/jobs/products-table/run", json={"dry_run": False})
    assert resp.status_code == 201
    assert resp.json()["status"] == "succeeded"
    assert resp.json()["rows_written"] == 18
    assert client.get("/targets/products").json()["row_count"] == 18


def test_run_unknown_connector_is_404():
    assert client.post("/jobs/nope/run", json={}).status_code == 404


def test_run_history_lists_runs():
    client.post("/jobs/customers-api/run", json={"dry_run": True})
    resp = client.get("/runs")
    assert resp.status_code == 200
    assert any(r["connector_id"] == "customers-api" for r in resp.json())


def test_target_row_and_lineage_endpoints():
    client.post("/jobs/orders-file/run", json={})
    resp = client.get("/targets/orders")
    assert resp.status_code == 200
    assert resp.json()["row_count"] == 40
    key = resp.json()["rows"][0]["order_id"]
    row = client.get(f"/targets/orders/{key}")
    assert row.status_code == 200
    assert row.json()["version"] == 1
    lineage = client.get("/lineage/orders/region")
    assert lineage.status_code == 200
    assert lineage.json()["sources"][0]["source_column"] == "region_code"
    impact = client.get("/lineage/impact/orders-file/region_code")
    assert impact.status_code == 200
    assert impact.json()["affected_jobs"] == ["orders-file"]


def test_metrics_and_audit_endpoints():
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "orders-file" in resp.json()
    resp = client.get("/audit")
    assert resp.status_code == 200
    assert any(e["action"] == "run.completed" for e in resp.json())


def test_register_connector_endpoint():
    resp = client.post("/connectors", json={
        "connector_id": "api-test", "kind": "api",
        "display_name": "Test API", "source_name": "test",
        "schema": ["id", "updated_at"], "key_field": "id",
        "target_table": "test_table",
        "mappings": [{"source": "id", "target": "id"}]})
    assert resp.status_code == 201, resp.text
    assert any(c["connector_id"] == "api-test"
               for c in client.get("/connectors").json())


def test_register_connector_bad_mapping_is_400():
    resp = client.post("/connectors", json={
        "connector_id": "api-bad", "kind": "api",
        "display_name": "Bad API", "source_name": "test",
        "schema": ["id"], "key_field": "id",
        "target_table": "bad_table",
        "mappings": [{"source": "nope", "target": "x"}]})
    assert resp.status_code == 400
