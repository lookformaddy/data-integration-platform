"""FastAPI service for the enterprise data integration platform.

Core is pure stdlib; this module requires FastAPI. State is in-memory.
Every run goes through the same fail-closed path as the CLI: credential
resolution -> watermarked read with retries -> mapping -> quality gates
-> dedup -> dry-run report or versioned upsert + checkpoint advance.
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from dint.models import (
    ConnectorConfig,
    ConnectorKind,
    FieldMapping,
    IntegrationError,
    MappingSpec,
    RunStatus,
    TransformKind,
)
from dint.seed import build_seeded_engine

app = FastAPI(title="Enterprise Data Integration Platform", version="1.0.0")

ENGINE = build_seeded_engine()
AUDIT = ENGINE.audit


def _now() -> datetime:
    return datetime.now(timezone.utc)




class MappingIn(BaseModel):
    source: str
    target: str
    transform: str = "passthrough"
    params: dict = {}


class ConnectorIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    connector_id: str
    kind: str
    display_name: str
    source_name: str
    source_schema: list[str] = Field(alias="schema")
    watermark_field: str = "updated_at"
    key_field: str = "id"
    credential_env: str | None = None
    mappings: list[MappingIn] = []
    target_table: str = ""


class RunIn(BaseModel):
    dry_run: bool = False


def _run_view(record) -> dict:
    return {
        "run_id": record.run_id, "connector_id": record.connector_id,
        "target_table": record.target_table, "status": record.status.value,
        "rows_read": record.rows_read, "rows_written": record.rows_written,
        "rows_quarantined": record.rows_quarantined,
        "rows_skipped_duplicate": record.rows_skipped_duplicate,
        "dry_run": record.dry_run, "attempts": record.attempts,
        "error": record.error,
        "watermark_before": record.watermark_before,
        "watermark_after": record.watermark_after,
        "duration_seconds": record.duration_seconds,
    }


@app.get("/connectors")
def list_connectors():
    return [{"connector_id": c.connector_id, "kind": c.kind.value,
             "display_name": c.display_name, "source_name": c.source_name,
             "target_table": ENGINE.mappings[c.connector_id].target_table
             if c.connector_id in ENGINE.mappings else None}
            for c in ENGINE.configs.values()]


@app.post("/connectors", status_code=201)
def register_connector(body: ConnectorIn):
    try:
        config = ConnectorConfig(
            connector_id=body.connector_id, kind=ConnectorKind(body.kind),
            display_name=body.display_name, source_name=body.source_name,
            schema=body.source_schema, watermark_field=body.watermark_field,
            key_field=body.key_field, credential_env=body.credential_env)
        ENGINE.register_connector(config)
        if body.mappings and body.target_table:
            ENGINE.register_mapping(MappingSpec(
                connector_id=body.connector_id, target_table=body.target_table,
                mappings=[FieldMapping(m.source, m.target,
                                       TransformKind(m.transform), m.params)
                          for m in body.mappings]))
    except (IntegrationError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"connector_id": body.connector_id, "kind": body.kind}


@app.post("/jobs/{connector_id}/run", status_code=201)
def run_job(connector_id: str, body: RunIn = RunIn()):
    if connector_id not in ENGINE.connectors:
        raise HTTPException(status_code=404, detail="unknown connector")
    record = ENGINE.run(connector_id, dry_run=body.dry_run, now=_now())
    if record.status == RunStatus.FAILED:
        raise HTTPException(status_code=400, detail=record.error or "run failed")
    return _run_view(record)


@app.get("/runs")
def list_runs(connector_id: str | None = None):
    return [_run_view(r) for r in ENGINE.run_history(connector_id)]


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    record = next((r for r in ENGINE.runs if r.run_id == run_id), None)
    if record is None:
        raise HTTPException(status_code=404, detail="unknown run")
    return _run_view(record)


@app.get("/targets/{table}")
def list_target_rows(table: str):
    rows = ENGINE.targets.rows(table)
    return {"table": table, "row_count": len(rows),
            "rows": [r.row for r in rows],
            "versions": {str(r.key): r.version for r in rows}}


@app.get("/targets/{table}/{key}")
def get_target_row(table: str, key: str):
    row = ENGINE.targets.get(table, key)
    if row is None:
        raise HTTPException(status_code=404, detail="unknown row")
    return {"key": key, "row": row.row, "version": row.version,
            "row_hash": row.row_hash}


@app.get("/lineage/{target_table}/{target_column}")
def lineage_for_target(target_table: str, target_column: str):
    edges = ENGINE.lineage.edges_for_target(target_table, target_column)
    return {"target_table": target_table, "target_column": target_column,
            "sources": [{"source_connector": e.source_connector,
                         "source_column": e.source_column,
                         "transform": e.transform} for e in edges]}


@app.get("/lineage/impact/{connector_id}/{source_column}")
def impact(connector_id: str, source_column: str):
    report = ENGINE.lineage.impact(connector_id, source_column)
    return {"source_connector": report.source_connector,
            "source_column": report.source_column,
            "affected_targets": [
                {"target_table": e.target_table,
                 "target_column": e.target_column,
                 "transform": e.transform}
                for e in report.affected_targets],
            "affected_jobs": report.affected_jobs}


@app.get("/metrics")
def metrics():
    return {cid: {"runs": m.runs, "rows_read": m.rows_read,
                  "rows_written": m.rows_written,
                  "rows_quarantined": m.rows_quarantined,
                  "failures": m.failures,
                  "last_lag_seconds": m.last_lag_seconds,
                  "last_throughput_rows_per_sec": m.last_throughput_rows_per_sec}
            for cid, m in ENGINE.metrics.items()}


@app.get("/quarantine")
def quarantine():
    return [{"row_key": q.row_key, "stage": q.stage, "reason": q.reason}
            for q in ENGINE.quarantine]


@app.get("/audit")
def audit():
    return [{"seq": e.seq, "timestamp": e.timestamp, "actor": e.actor,
             "action": e.action, "run_id": e.run_id,
             "entry_hash": e.entry_hash[:16]}
            for e in AUDIT.entries]
