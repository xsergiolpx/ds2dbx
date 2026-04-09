"""Execute notebooks on the Databricks workspace."""

from __future__ import annotations

import json
import time
from pathlib import Path

from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.utils.subprocess_runner import run_command

console = Console()


def run_notebook_on_workspace(
    ws_notebook_path: str,
    config: Config,
    *,
    cluster_id: str | None = None,
    timeout_minutes: int = 30,
    verbose: bool = False,
) -> dict:
    """Submit a notebook for execution and wait for completion. Returns run result."""
    # Build job submission payload
    task = {
        "task_key": "run",
        "notebook_task": {
            "notebook_path": ws_notebook_path,
            "source": "WORKSPACE",
        },
    }
    if cluster_id:
        task["existing_cluster_id"] = cluster_id
    else:
        task["environment_key"] = "Default"

    payload = {
        "run_name": f"ds2dbx-run-{Path(ws_notebook_path).stem}",
        "tasks": [task],
    }

    # Submit
    result = run_command(
        [
            "databricks", "jobs", "submit",
            "--json", json.dumps(payload),
            "--profile", config.databricks.profile,
        ],
        verbose=verbose,
    )

    if result.returncode != 0:
        return {"status": "SUBMIT_FAILED", "error": result.stderr}

    try:
        run_data = json.loads(result.stdout)
        run_id = run_data.get("run_id")
    except (json.JSONDecodeError, KeyError):
        return {"status": "SUBMIT_FAILED", "error": "Could not parse run_id"}

    # Poll for completion
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        check = run_command(
            [
                "databricks", "runs", "get", str(run_id),
                "--profile", config.databricks.profile,
            ],
            verbose=False,
        )
        if check.returncode == 0:
            try:
                run_info = json.loads(check.stdout)
                state = run_info.get("state", {})
                life_cycle = state.get("life_cycle_state", "")
                result_state = state.get("result_state", "")
                if life_cycle == "TERMINATED":
                    return {
                        "status": result_state or "COMPLETED",
                        "run_id": run_id,
                        "state": state,
                    }
            except json.JSONDecodeError:
                pass
        time.sleep(15)

    return {"status": "TIMEOUT", "run_id": run_id}
