"""Deploy notebooks and workflows to Databricks workspace."""

from __future__ import annotations

import json
from pathlib import Path

import requests
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
                console.print(f"    Uploaded: {target}")
        else:
            console.print(f"    [red]Failed to upload: {py_file.name}[/red]")
    return count


def create_workflow(
    workflow_json_path: Path,
    ws_notebooks_path: str,
    config: Config,
    verbose: bool = False,
) -> str | None:
    """Create a Databricks Workflow from a JSON file. Returns job_id or None.

    Updates notebook_path references to point to the deployed workspace location
    and adapts cluster config for Serverless.
    """
    with open(workflow_json_path) as f:
        workflow = json.load(f)

    # Adapt tasks for Serverless + fix notebook paths
    env_key = "default_env"
    for task in workflow.get("tasks", []):
        task.pop("existing_cluster_id", None)
        task.pop("job_cluster_key", None)
        task["environment_key"] = env_key

        # Fix notebook path to point to workspace deploy location
        nb_task = task.get("notebook_task", {})
        if "notebook_path" in nb_task:
            nb_name = Path(nb_task["notebook_path"]).name
            nb_task["notebook_path"] = f"{ws_notebooks_path}/{nb_name}"

    # Remove job_clusters (Serverless doesn't need them) and add environments
    workflow.pop("job_clusters", None)
    workflow["environments"] = [
        {
            "environment_key": env_key,
            "spec": {
                "client": "1",
                "dependencies": [],
            },
        }
    ]

    # Use REST API for job creation (CLI --json can have issues with large payloads)
    host = config.get_host()
    token = config.get_token()

    if host and token:
        try:
            resp = requests.post(
                f"{host}/api/2.1/jobs/create",
                json=workflow,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            if resp.status_code == 200:
                return str(resp.json().get("job_id", ""))
            else:
                if verbose:
                    console.print(f"    [red]API error: {resp.status_code} {resp.text[:200]}[/red]")
        except Exception as e:
            if verbose:
                console.print(f"    [red]Request error: {e}[/red]")

    # Fallback to CLI
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


def create_master_workflow(
    workflow_json_path: Path,
    deployed_jobs: dict[str, str],
    config: Config,
    verbose: bool = False,
) -> str | None:
    """Create a master orchestrator workflow that chains sub-workflows via run_job_task.

    BladeBridge generates master workflows where each task has both notebook_task
    and run_job_task. We strip notebook_task and wire run_job_task.job_id to
    the already-deployed sub-workflow job IDs.
    """
    with open(workflow_json_path) as f:
        workflow = json.load(f)

    for task in workflow.get("tasks", []):
        task.pop("existing_cluster_id", None)
        task.pop("job_cluster_key", None)

        # If this task references a sub-workflow, use run_job_task
        task_key = task.get("task_key", "")
        if task_key in deployed_jobs:
            task.pop("notebook_task", None)
            task.pop("environment_key", None)
            task["run_job_task"] = {"job_id": int(deployed_jobs[task_key])}
        elif "run_job_task" in task:
            # Has run_job_task but we don't have the deployed job ID — skip
            task.pop("run_job_task", None)
            task.pop("notebook_task", None)
            task["environment_key"] = "default_env"
        else:
            task["environment_key"] = "default_env"

    workflow.pop("job_clusters", None)
    workflow["environments"] = [
        {
            "environment_key": "default_env",
            "spec": {"client": "1", "dependencies": []},
        }
    ]

    # Remove tasks that have no executable (no notebook_task or run_job_task)
    workflow["tasks"] = [
        t for t in workflow.get("tasks", [])
        if "notebook_task" in t or "run_job_task" in t
    ]

    if not workflow["tasks"]:
        return None

    host = config.get_host()
    token = config.get_token()
    if host and token:
        try:
            resp = requests.post(
                f"{host}/api/2.1/jobs/create",
                json=workflow,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            if resp.status_code == 200:
                return str(resp.json().get("job_id", ""))
            elif verbose:
                console.print(f"    [red]API error: {resp.status_code} {resp.text[:200]}[/red]")
        except Exception as e:
            if verbose:
                console.print(f"    [red]Request error: {e}[/red]")
    return None


def deploy_usecase(
    output_dir: Path,
    ws_base_path: str,
    config: Config,
    verbose: bool = False,
) -> dict:
    """Deploy all pass outputs from a use case to the workspace.

    Collects notebooks from all pass output directories and workflow JSON
    from Pass 3, uploads notebooks, and creates Databricks Jobs.

    Parameters
    ----------
    output_dir:
        The use case output directory (e.g., _ds2dbx_output/UC1...)
    ws_base_path:
        Workspace base path (e.g., /Workspace/Users/user@company.com/ds2dbx_deploy/UC1)
    config:
        ds2dbx configuration
    verbose:
        Show detailed output

    Returns
    -------
    dict with notebooks_uploaded, workflows_created, workflow_ids, job_urls
    """
    metrics = {
        "notebooks_uploaded": 0,
        "workflows_created": 0,
        "workflow_ids": [],
        "job_urls": [],
    }

    host = config.get_host()
    ws_notebooks_path = f"{ws_base_path}/notebooks"

    # Create workspace directory
    run_command(
        ["databricks", "workspace", "mkdirs", ws_notebooks_path,
         "--profile", config.databricks.profile],
        verbose=verbose,
    )

    # --- Step 1: Upload notebooks from all passes ---
    pass_dirs = [
        ("Pass 1 (DDL)", "pass1_ddl/output"),
        ("Pass 2 (Data)", "pass2_data/output"),
        ("Pass 3 (Transpile)", "pass3_transpile/merged"),
        ("Pass 4 (Shell)", "pass4_shell/output"),
        ("Pass 5 (Validate)", "pass5_validate/output"),
    ]

    for label, rel_path in pass_dirs:
        local_dir = output_dir / rel_path
        if not local_dir.exists():
            continue
        py_files = list(local_dir.glob("*.py"))
        if not py_files:
            continue
        console.print(f"  Uploading {label}: {len(py_files)} notebook(s)")
        count = upload_directory(local_dir, ws_notebooks_path, config, verbose)
        metrics["notebooks_uploaded"] += count

    # --- Step 2: Create workflows from Pass 3 JSON ---
    wf_sources = [
        output_dir / "pass3_transpile" / "merged",
        output_dir / "pass3_transpile" / "bladebridge_output",
    ]

    wf_files: list[Path] = []
    seen_names: set[str] = set()
    for wf_dir in wf_sources:
        if wf_dir.exists():
            for f in sorted(wf_dir.glob("*.json")):
                if f.stem not in seen_names:
                    wf_files.append(f)
                    seen_names.add(f.stem)

    if wf_files:
        # Separate sub-workflows from master orchestrators.
        # A master orchestrator has tasks with run_job_task — deploy those last
        # so we can wire them to the deployed sub-workflow job IDs.
        sub_wfs: list[Path] = []
        master_wfs: list[Path] = []
        for wf_file in wf_files:
            with open(wf_file) as f:
                wf_data = json.load(f)
            has_run_job = any("run_job_task" in t for t in wf_data.get("tasks", []))
            if has_run_job:
                master_wfs.append(wf_file)
            else:
                sub_wfs.append(wf_file)

        console.print(
            f"  Creating {len(sub_wfs)} workflow(s)"
            + (f" + {len(master_wfs)} orchestrator(s)" if master_wfs else "")
        )

        # Deploy sub-workflows first, collect name -> job_id mapping
        deployed_jobs: dict[str, str] = {}
        for wf_file in sub_wfs:
            job_id = create_workflow(wf_file, ws_notebooks_path, config, verbose)
            if job_id:
                metrics["workflows_created"] += 1
                metrics["workflow_ids"].append(job_id)
                deployed_jobs[wf_file.stem] = job_id
                job_url = f"{host}/#job/{job_id}" if host else job_id
                metrics["job_urls"].append(job_url)
                console.print(f"    [green]Created:[/green] {wf_file.stem} -> {job_url}")
            else:
                console.print(f"    [red]Failed:[/red] {wf_file.stem}")

        # Deploy master orchestrators, wiring run_job_task to deployed job IDs
        for wf_file in master_wfs:
            job_id = create_master_workflow(
                wf_file, deployed_jobs, config, verbose,
            )
            if job_id:
                metrics["workflows_created"] += 1
                metrics["workflow_ids"].append(job_id)
                job_url = f"{host}/#job/{job_id}" if host else job_id
                metrics["job_urls"].append(job_url)
                console.print(f"    [green]Created orchestrator:[/green] {wf_file.stem} -> {job_url}")
            else:
                console.print(f"    [red]Failed orchestrator:[/red] {wf_file.stem}")

    return metrics
