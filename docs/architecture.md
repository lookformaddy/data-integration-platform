# Architecture — Enterprise Data Integration Platform

## Diagram (description)

```
 ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
 │ FileConnector│   │ ApiConnector │   │TableConnector│
 │ (CSV-style)  │   │ (paginated   │   │ (table-style)│
 │              │   │  JSON API)   │   │              │
 └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
        │  read_since(watermark)             │
        └──────────────────┬──────────────────┘
                           ▼
                  ┌──────────────────────┐
                  │  IntegrationEngine   │
                  │  credential check →  │
                  │  read w/ retries →   │
                  │  mapping → quality → │
                  │  dedup → write       │
                  └──────┬───────────────┘
                         │
        ┌────────────────┼──────────────────┐
        ▼                ▼                  ▼
 ┌────────────┐  ┌────────────┐     ┌────────────┐
 │ Checkpoint │  │ TargetStore│     │ Quarantine │
 │ Store      │  │ versioned  │     │ stage +    │
 │ watermark  │  │ upsert,    │     │ reason     │
 │ per        │  │ idempotent │     │ per row    │
 │ connector  │  │ by hash    │     │            │
 └────────────┘  └─────┬──────┘     └────────────┘
                       │
                       ▼
                ┌────────────┐
                │LineageGraph│
                │ source →   │
                │ mapping →  │
                │ target,    │
                │ impact     │
                └─────┬──────┘
                      ▼
                ┌────────────┐
                │ AuditLog   │
                │ hash-chain │
                │ append-only│
                └────────────┘
```

Every registration, run, and failure appends to the audit log; the log is
the only cross-cutting dependency, so each module stays independently
testable.

## Module map

| Module | Responsibility |
| --- | --- |
| `models.py` | Dataclasses: ConnectorConfig, FieldMapping, MappingSpec, QualityRule, QualityViolation, QuarantinedRow, Checkpoint, TargetRow, RunRecord, ConnectorMetrics; error types |
| `connectors.py` | SourceConnector interface + File/Api/Table implementations, credential resolution from env vars, synthetic data generators, transient-failure hook |
| `mapping.py` | Mapping validation (fail-closed), transforms (cast/concat/lookup/hash/default), row mapping, content hashing |
| `quality.py` | Quality gates: row rules (not-null, range, schema) and batch rules (null-rate, unique), blocking vs warning |
| `state.py` | CheckpointStore (watermark per connector) and TargetStore (versioned upsert, history, idempotent dedup) |
| `engine.py` | Job orchestration: retries with backoff, dry-run vs apply, run history, per-connector metrics |
| `lineage.py` | Column-level lineage edges from mappings; impact analysis |
| `audit.py` | Hash-chained append-only audit log with verification |
| `seed.py` | Shared seeded engine (3 demo connectors + mappings + rules) used by the service, the CLI, and the demo, so the CLI stays pure stdlib |
| `service.py` | FastAPI service over the same core (in-memory state) |
| `cli.py` | argparse CLI (`demo`, `run`, `status`) over the same core |

## Key flows

**Run.** `run(connector_id)` resolves the credential (fail-closed if the
env var is missing), reads rows newer than the checkpoint with retries
and exponential backoff, validates the schema, applies the mapping
(failures quarantined), runs the quality gates (blocking violations
quarantined, warnings recorded), dedups by content hash, then either
reports the dry-run outcome or upserts into the versioned target store
and advances the checkpoint to the newest watermark read.

**Incremental ingestion.** The checkpoint stores the last watermark
processed. A re-run reads only rows with a watermark strictly greater
than the checkpoint, so it is a no-op when nothing changed; a failed run
does not advance the checkpoint, so the same delta is re-read next time.

**Idempotency.** A row is written only if no row with the same natural
key and the same content hash already exists. Within-batch duplicates
are skipped the same way, so at-least-once delivery cannot double-load
a target.

**Lineage.** Edges are derived from the registered mapping specs, so
they always match what the engine does. `impact(connector, column)`
returns every target column and job that depends on a source column.

## Failure modes

- Missing credential, unknown connector, invalid mapping, schema
  violation, or exhausted retries: the run fails, the error is recorded
  on the run record and in the audit log, and no target row or
  checkpoint changes.
- A row that fails a transform or a blocking quality rule is
  quarantined with its stage and reason; the rest of the batch still
  lands and the run reports `partial`.
- Audit tampering (edit, deletion, reordering) is detected by
  `AuditLog.verify()` recomputing the hash chain.

## Production notes

A production deployment would persist checkpoints, targets, and the
audit log in a metadata database, run connectors against real systems
(file store, HTTP APIs, JDBC), schedule runs with a real orchestrator,
store credentials in a secrets manager, and add dead-letter handling
and alerting on quarantine rates. None of that changes the core
invariants implemented here.
