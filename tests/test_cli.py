"""CLI tests: status and run commands over the seeded service engine."""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV = {"PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"}


def _run_cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "dint.cli", *args],
        cwd=ROOT, env={**__import__("os").environ, **ENV},
        capture_output=True, text=True, timeout=60)


def test_cli_status():
    result = _run_cli("status")
    assert result.returncode == 0, result.stderr
    assert "orders-file" in result.stdout
    assert "audit chain valid: True" in result.stdout


def test_cli_run_dry_run():
    result = _run_cli("run", "products-table", "--dry-run")
    assert result.returncode == 0, result.stderr
    body = json.loads(result.stdout)
    assert body["status"] == "dry_run"
    assert body["rows_read"] == 18


def test_cli_run_unknown_connector_fails():
    result = _run_cli("run", "nope")
    assert result.returncode == 1
    assert "unknown connector" in result.stdout
