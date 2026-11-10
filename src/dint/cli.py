"""CLI over the same core as the FastAPI service.

Usage:
    PYTHONPATH=src python3 -m dint.cli demo
    PYTHONPATH=src python3 -m dint.cli run orders-file [--dry-run]
    PYTHONPATH=src python3 -m dint.cli status
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from dint.seed import build_seeded_engine

ENGINE = build_seeded_engine()


def _cmd_demo(_args) -> int:
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
    import demo as demo_module
    demo_module.main()
    return 0


def _cmd_run(args) -> int:
    record = ENGINE.run(args.connector_id, dry_run=args.dry_run,
                        now=datetime.now(timezone.utc))
    print(json.dumps({
        "run_id": record.run_id, "connector_id": record.connector_id,
        "status": record.status.value, "rows_read": record.rows_read,
        "rows_written": record.rows_written,
        "rows_quarantined": record.rows_quarantined,
        "rows_skipped_duplicate": record.rows_skipped_duplicate,
        "attempts": record.attempts, "error": record.error,
    }, indent=2))
    return 0 if record.status.value != "failed" else 1


def _cmd_status(_args) -> int:
    print("connectors:")
    for cid, config in ENGINE.configs.items():
        checkpoint = ENGINE.checkpoints.get(cid)
        metrics = ENGINE.metrics[cid]
        print(f"  {cid} ({config.kind.value}) -> "
              f"{ENGINE.mappings[cid].target_table if cid in ENGINE.mappings else '?'} "
              f"watermark={checkpoint.watermark} "
              f"runs={metrics.runs} written={metrics.rows_written} "
              f"quarantined={metrics.rows_quarantined} failures={metrics.failures}")
    print(f"audit chain valid: {ENGINE.audit.verify()} "
          f"({len(ENGINE.audit.entries)} entries)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dint")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("demo", help="run the end-to-end demo")
    run_parser = sub.add_parser("run", help="run one connector job")
    run_parser.add_argument("connector_id")
    run_parser.add_argument("--dry-run", action="store_true")
    sub.add_parser("status", help="show connectors, checkpoints, metrics")
    args = parser.parse_args(argv)
    if args.command == "demo":
        return _cmd_demo(args)
    if args.command == "run":
        return _cmd_run(args)
    if args.command == "status":
        return _cmd_status(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
