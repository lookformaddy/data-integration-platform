# Enterprise Data Integration Platform

A connector-based integration hub: pluggable source connectors (file, JSON API, database table) behind one interface, declarative field mappings with type coercion and transforms, watermarked incremental ingestion with checkpointed state, data-quality gates with reason-carrying quarantine, job orchestration with retries and dry-run vs apply, column-level lineage with impact analysis, and a versioned target store with idempotent upserts — all on synthetic data.

## The enterprise problem

Most enterprises do not have one data source; they have dozens — files landing in a bucket, SaaS APIs, and operational databases — each with its own schema, its own cadence, and its own failure modes. Point-to-point pipelines multiply that complexity: every new source means new code, new credential handling, new dedup logic, and a new place where a bad row can silently corrupt a target table. This project implements the hub that replaces them: connectors are registered declaratively, mappings are validated before a run starts, ingestion is incremental by watermark, bad rows are quarantined with reasons instead of dropped or forced through, re-runs are idempotent, and every run is auditable end to end.

## Architecture overview

```
FileConnector ─┐
ApiConnector ──┼──► IntegrationEngine ──► MappingSpec (validate → apply)
TableConnector ┘         │                    │
                         │                    ▼
                    CheckpointStore      QualityGate (schema, null-rate,
                    (watermark)           range, uniqueness)
                         │                    │
                         ▼                    ▼
                    TargetStore ◄── dedup (content hash) ── Quarantine
                    (versioned upsert,                     (reason-carrying)
                     idempotent)
                         │
                         ▼
                    LineageGraph ──► impact analysis
                         │
                         ▼
                    AuditLog (hash-chained, tamper-evident)
```

- **Connectors** (`connectors.py`) — `FileConnector`, `ApiConnector` (paginated), and `TableConnector` share one interface: `read_since(watermark)` returns rows newer than the checkpoint, sorted by watermark. Credentials resolve from environment variables only; a missing credential fails the run closed. A `fail_times` hook simulates transient source failures so retry behaviour is testable.
- **Mappings** (`mapping.py`) — declarative `FieldMapping`s with transforms: passthrough/rename, cast (int/float/str/bool/date), concat, lookup (with default), deterministic salted hash, and default. The spec is validated up front against the connector schema: unknown source columns, duplicate targets, and incomplete transform parameters are all rejected before any row is read. A row that fails a transform is quarantined with its reason.
- **Quality gates** (`quality.py`) — row rules (not-null, range, schema type) and batch rules (null-rate, uniqueness) run on mapped rows before they land. Blocking violations quarantine the row; non-blocking violations are recorded as warnings and the row still lands.
- **State** (`state.py`) — `CheckpointStore` holds the last watermark processed per connector, so each run is incremental. `TargetStore` is a versioned table: upserts keyed on the natural key, each write bumps the version and keeps history, and a row whose content hash is unchanged is skipped, making re-runs idempotent.
- **Engine** (`engine.py`) — one run is: resolve credentials → read with retries and exponential backoff → validate mapping → map → quality gates → dedup → dry-run report or versioned upsert + checkpoint advance. Run history, per-connector metrics (lag, throughput, failures), and the audit log are updated on every run, success or failure.
- **Lineage** (`lineage.py`) — derived from the registered mappings, so it can never drift from what the engine does. `impact(connector, column)` lists every target column and job affected by a source-column change.
- **Audit log** (`audit.py`) — every registration, run, and failure appends a hash-chained entry; any edit, deletion, or reordering is detectable.
- **API + CLI** (`service.py`, `cli.py`) — FastAPI service and an argparse CLI over the same core.

## How to run

```bash
pip install -r requirements.txt   # core is pure stdlib; FastAPI/pytest are optional extras
PYTHONPATH=src python3 scripts/demo.py        # end-to-end demo (offline, synthetic data)
PYTHONPATH=src python3 -m pytest tests/ -q   # test suite
PYTHONPATH=src python3 -m dint.cli status
PYTHONPATH=src python3 -m dint.cli run orders-file --dry-run
uvicorn dint.service:app --host 0.0.0.0 --port 8000   # API (needs FastAPI)
```

## Key design decisions and trade-offs

- **Fail-closed everywhere.** Missing credentials, unknown source columns, invalid mappings, and quality violations all fail the run or quarantine the row — never silently defaulted, skipped, or coerced.
- **Watermarks, not full reloads.** Each connector reads only rows newer than its checkpoint, so a daily run touches only the delta; the checkpoint advances only on a successful apply, so a failed run re-reads the same delta next time.
- **Dry-run is a first-class mode.** A dry-run reports exactly what would be written and quarantined without touching the target store or the checkpoint — the safe way to validate a new mapping or a source change.
- **Idempotency by content hash.** Within-batch duplicates and re-delivered rows are skipped by SHA-256 of the mapped row, so at-least-once delivery from a flaky source cannot double-load the target.
- **Quarantine, not silent loss.** Every rejected row is kept with its stage and reason, so data-quality failures are diagnosable instead of invisible.
- **In-memory store.** Connectors, checkpoints, targets, and lineage are in-memory by design for this portfolio project; the core is deliberately split from storage so a durable backend (metadata DB + object store) can replace it without touching mapping, quality, or orchestration logic.
- **CI shipped as `docs/github-actions-ci.yml`.** The GitHub App used to publish this repo lacks the Workflows permission, so the workflow activates by moving that file to `.github/workflows/ci.yml`.

## What it demonstrates

Connector-based data integration, declarative mappings with validated transforms, watermarked incremental ingestion with checkpointed state, data-quality gates with reason-carrying quarantine, retry with exponential backoff, dry-run vs apply semantics, idempotent upserts into a versioned target store, column-level lineage with impact analysis, per-connector lag/throughput metrics, and tamper-evident auditing — the controls a Director-level data engineering interview probes when asking "how do you land data from a dozen sources without losing track of any of it?"
