"""Deploy notebooks and workflows to Databricks workspace."""

from __future__ import annotations

import json
from pathlib import Path

from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.utils.subprocess_runner import run_command

console = Console()


def upload_notebook(local_path: Path, ws_path: str, config: Config, verbose: bool = False) -> bool:
    """Upload a notebook to the workspace."""
    result = run_command(
        [
            "databricks", "workspace", "import", ws_path,
            "--file", str(local_path),
            "--language", "PYTHON",
            "--overwrite",
            "--profile", config.databricks.profile,
        ],
        verbose=verbose,
    )
    return result.returncode == 0


def upload_directory(local_dir: Path, ws_path: str, config: Config, verbose: bool = False) -> int:
    """Upload all .py files from a local directory to workspace. Returns count uploaded."""
    count = 0
    for py_file in sorted(local_dir.glob("*.py")):
        notebook_name = py_file.stem
        target = f"{ws_path}/{notebook_name}"
        if upload_notebook(py_file, target, config, verbose):
            count += 1
            if verbose:
                console.print(f"  Uploaded: {target}")
        else:
            console.print(f"  [red]Failed to upload: {py_file.name}[/red]")
    return count


def create_workflow(workflow_json_path: Path, config: Config, verbose: bool = False) -> str | None:
    """Create a Databricks Workflow from a JSON file. Returns job_id or None."""
    with open(workflow_json_path) as f:
        workflow = json.load(f)

    # Adapt for Serverless
    for task in workflow.get("tasks", []):
        task.pop("existing_cluster_id", None)
        task.pop("job_cluster_key", None)
        if "environment_key" not in task:
            task["environment_key"] = "Default"

    result = run_command(
        [
            "databricks", "jobs", "create",
            "--json", json.dumps(workflow),
            "--profile", config.databricks.profile,
        ],
        verbose=verbose,
    )

    if result.returncode == 0:
        try:
            data = json.loads(result.stdout)
            return str(data.get("job_id", ""))
        except json.JSONDecodeError:
            return None
    return None


def deploy_usecase(deploy_dir: Path, ws_base_path: str, config: Config, verbose: bool = False) -> dict:
    """Deploy all notebooks and workflows from a use case deploy directory."""
    metrics = {"notebooks_uploaded": 0, "workflows_created": 0, "workflow_ids": []}

    notebooks_dir = deploy_dir / "notebooks"
    workflows_dir = deploy_dir / "workflows"

    if notebooks_dir.exists():
        ws_notebooks = f"{ws_base_path}/notebooks"
        # Create workspace directory
        run_command(
            ["databricks", "workspace", "mkdirs", ws_notebooks, "--profile", config.databricks.profile],
            verbose=verbose,
        )
        metrics["notebooks_uploaded"] = upload_directory(notebooks_dir, ws_notebooks, config, verbose)

    if workflows_dir.exists():
        for wf_file in sorted(workflows_dir.glob("*.json")):
            job_id = create_workflow(wf_file, config, verbose)
            if job_id:
                metrics["workflows_created"] += 1
                metrics["workflow_ids"].append(job_id)
                if verbose:
                    console.print(f"  Created workflow: job_id={job_id}")

    return metrics
