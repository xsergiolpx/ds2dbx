"""Status JSON read/write for tracking pass completion."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_status(output_dir: Path) -> dict:
    """Read status from _status.json in output dir."""
    status_file = output_dir / "_status.json"
    if status_file.exists():
        with open(status_file) as f:
            return json.load(f)
    return {}


def write_status(output_dir: Path, status: dict):
    """Write status to _status.json."""
    output_dir.mkdir(parents=True, exist_ok=True)
    status_file = output_dir / "_status.json"
    with open(status_file, "w") as f:
        json.dump(status, f, indent=2)


def init_status(output_dir: Path, usecase_name: str, pattern: str) -> dict:
    """Initialize status for a use case."""
    status = {
        "usecase": usecase_name,
        "pattern_detected": pattern,
        "started_at": _now(),
        "passes": {},
    }
    write_status(output_dir, status)
    return status


def start_pass(output_dir: Path, pass_name: str, **extra) -> dict:
    """Mark a pass as started."""
    status = read_status(output_dir)
    status.setdefault("passes", {})[pass_name] = {
        "status": "running",
        "started_at": _now(),
        **extra,
    }
    write_status(output_dir, status)
    return status


def complete_pass(output_dir: Path, pass_name: str, **metrics) -> dict:
    """Mark a pass as completed with metrics."""
    status = read_status(output_dir)
    pass_status = status.setdefault("passes", {}).setdefault(pass_name, {})
    pass_status["status"] = "completed"
    pass_status["completed_at"] = _now()
    pass_status.update(metrics)
    write_status(output_dir, status)
    return status


def fail_pass(output_dir: Path, pass_name: str, error: str) -> dict:
    """Mark a pass as failed."""
    status = read_status(output_dir)
    pass_status = status.setdefault("passes", {}).setdefault(pass_name, {})
    pass_status["status"] = "failed"
    pass_status["error"] = error
    pass_status["failed_at"] = _now()
    write_status(output_dir, status)
    return status


def is_pass_completed(output_dir: Path, pass_name: str) -> bool:
    """Check if a pass has already completed."""
    status = read_status(output_dir)
    return status.get("passes", {}).get(pass_name, {}).get("status") == "completed"
