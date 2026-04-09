"""Pass 3 — DataStage transpilation via BladeBridge + Switch fallback."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.lakebridge import BladeBridgeRunner, PromptManager, SwitchRunner
from ds2dbx.scanner.folder import UseCaseManifest
from ds2dbx.triage.engine import triage_notebooks
from ds2dbx.utils.status import is_pass_completed, start_pass, complete_pass, fail_pass

from ds2dbx.passes.base import BasePass

console = Console()


class Pass3Transpile(BasePass):
    """BladeBridge rule-based transpile, triage, then Switch LLM fix for broken files."""

    @property
    def pass_name(self) -> str:
        return "pass3_transpile"

    def run(self, manifest: UseCaseManifest, force: bool = False) -> dict:
        if is_pass_completed(self.output_dir, self.pass_name) and not force:
            console.print(f"  [yellow]Skipping {self.pass_name} (already completed)[/yellow]")
            return {}

        start_pass(self.output_dir, self.pass_name)

        try:
            metrics = self._execute(manifest)
            complete_pass(self.output_dir, self.pass_name, **metrics)
            return metrics
        except Exception as exc:
            fail_pass(self.output_dir, self.pass_name, error=str(exc))
            raise

    def _execute(self, manifest: UseCaseManifest) -> dict:
        work_dir = self.output_dir / "pass3_transpile"
        ds_input_dir = work_dir / "input"
        bb_output_dir = work_dir / "bladebridge_output"
        switch_input_dir = work_dir / "switch_input"
        switch_output_dir = work_dir / "switch_output"
        merged_dir = work_dir / "merged"

        for d in [ds_input_dir, bb_output_dir, switch_input_dir, switch_output_dir, merged_dir]:
            d.mkdir(parents=True, exist_ok=True)

        if not manifest.datastage_files:
            console.print("  [yellow]No DataStage files found — skipping Pass 3[/yellow]")
            return {
                "bladebridge_notebooks": 0, "bladebridge_workflows": 0,
                "triage_clean": 0, "triage_broken": 0,
                "switch_fixed": 0, "switch_failed": 0, "conversion_rate": 0.0,
            }

        # --- Step 1: Copy DataStage XML to clean input dir ---
        for f in manifest.datastage_files:
            shutil.copy2(f, ds_input_dir / f.name)
        console.print(f"  Copied {len(manifest.datastage_files)} DataStage file(s) to input")

        # --- Step 2: Run BladeBridge ---
        bb = BladeBridgeRunner(self.config, verbose=self.verbose)
        console.print("  Running BladeBridge rule-based transpiler...")
        bb_result = bb.transpile(ds_input_dir, bb_output_dir)
        if bb_result.returncode != 0:
            console.print(f"  [red]BladeBridge failed: {bb_result.stderr[:200]}[/red]")
            raise RuntimeError(f"BladeBridge transpile failed: {bb_result.stderr[:500]}")

        # Count BB output
        bb_notebooks = list(bb_output_dir.rglob("*.py"))
        bb_workflows = list(bb_output_dir.rglob("*.json"))
        console.print(
            f"  BladeBridge produced {len(bb_notebooks)} notebook(s), "
            f"{len(bb_workflows)} workflow(s)"
        )

        # --- Step 3: Triage output ---
        triage_report = work_dir / "triage_report.json"
        clean_files, broken_files, _results = triage_notebooks(
            bb_output_dir, output_path=triage_report
        )
        console.print(
            f"  Triage: {len(clean_files)} clean, {len(broken_files)} broken"
        )

        # --- Step 4: For broken files, embed fix prompt and copy to Switch input ---
        switch_fixed = 0
        switch_failed = 0

        if broken_files:
            pm = PromptManager(self.config)
            for bf in broken_files:
                content = bf.read_text(encoding="utf-8", errors="replace")
                prompted = pm.prepare_input_with_prompt(content, "datastage_fix")
                prompted = f"# TARGET CATALOG: {self.config.catalog}\n# TARGET SCHEMA: {self.config.schema}\n\n{prompted}"
                (switch_input_dir / bf.name).write_text(prompted, encoding="utf-8")

            # --- Step 5: Run Switch on broken files ---
            ws_output = f"{self.config.get_workspace_base()}/{manifest.name}/pass3_switch"
            switch = SwitchRunner(self.config, verbose=self.verbose)

            console.print(f"  Running Switch on {len(broken_files)} broken file(s)...")
            sw_result = switch.transpile(switch_input_dir, ws_output)

            if sw_result.returncode == 0:
                # --- Step 6: Download Switch output ---
                downloaded = switch.download_output(ws_output, switch_output_dir)
                switch_fixed = len(downloaded)
                switch_failed = len(broken_files) - switch_fixed
                console.print(f"  Switch fixed {switch_fixed}, failed {switch_failed}")
            else:
                console.print(f"  [red]Switch failed: {sw_result.stderr[:200]}[/red]")
                switch_failed = len(broken_files)

        # --- Step 7: Merge clean + fixed into merged/ dir ---
        for f in clean_files:
            shutil.copy2(f, merged_dir / f.name)
        for f in switch_output_dir.glob("*.py"):
            shutil.copy2(f, merged_dir / f.name)
        # Copy clean workflows too
        for f in bb_output_dir.rglob("*.json"):
            shutil.copy2(f, merged_dir / f.name)

        # --- Step 8: Post-process workflow JSON ---
        for wf in merged_dir.glob("*.json"):
            _post_process_workflow(wf)

        total_input = len(manifest.datastage_files)
        total_output = len(list(merged_dir.glob("*.py")))
        conversion_rate = (total_output / total_input * 100) if total_input > 0 else 0.0

        metrics = {
            "bladebridge_notebooks": len(bb_notebooks),
            "bladebridge_workflows": len(bb_workflows),
            "triage_clean": len(clean_files),
            "triage_broken": len(broken_files),
            "switch_fixed": switch_fixed,
            "switch_failed": switch_failed,
            "conversion_rate": round(conversion_rate, 1),
        }
        console.print(f"  [green]Pass 3 complete:[/green] {metrics}")
        return metrics


def _post_process_workflow(wf_path: Path) -> None:
    """Remove existing_cluster_id and add environment_key to workflow JSON."""
    try:
        data = json.loads(wf_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return

    modified = False

    # Walk through tasks and fix cluster references
    tasks = data.get("tasks", [])
    for task in tasks:
        if "existing_cluster_id" in task:
            del task["existing_cluster_id"]
            modified = True
        if "job_cluster_key" not in task and "environment_key" not in task:
            task["environment_key"] = "default"
            modified = True

    if modified:
        wf_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
