"""BladeBridge (rule-based) transpiler wrapper."""

from __future__ import annotations

from pathlib import Path

from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.utils.subprocess_runner import RunResult, run_command

console = Console()


class BladeBridgeRunner:
    """Run the BladeBridge rule-based transpiler via the Databricks CLI."""

    def __init__(self, config: Config, verbose: bool = False):
        self.config = config
        self.verbose = verbose
        self.profile = config.databricks.profile

    def transpile(self, input_dir: Path, output_dir: Path) -> RunResult:
        """Run BladeBridge transpilation.

        Executes:
            databricks labs lakebridge transpile
                --input-source <input_dir>
                --output-folder <output_dir>
                --source-dialect datastage
                --target-technology PYSPARK
                --skip-validation true
                --profile <profile>
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        cmd = [
            "databricks", "labs", "lakebridge", "transpile",
            "--input-source", str(input_dir),
            "--output-folder", str(output_dir),
            "--source-dialect", "datastage",
            "--target-technology", self.config.lakebridge.target_technology,
            "--skip-validation", "true",
            "--profile", self.profile,
        ]

        cmd_display = " \\\n    ".join(cmd)
        console.print(f"  [dim]$ {cmd_display}[/dim]")

        return run_command(
            cmd,
            verbose=self.verbose,
            description="BladeBridge transpile",
        )
