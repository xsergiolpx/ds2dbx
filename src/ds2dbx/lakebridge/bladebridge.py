"""BladeBridge (rule-based) transpiler wrapper."""

from __future__ import annotations

from pathlib import Path

from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.utils.lakebridge_resolver import build_lakebridge_cmd, LakebridgeNotFoundError
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

        Raises LakebridgeNotFoundError if the CLI binary cannot be resolved.
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        cmd = build_lakebridge_cmd(self.config, "transpile", [
            "--input-source", str(input_dir),
            "--output-folder", str(output_dir),
            "--source-dialect", "datastage",
            "--target-technology", self.config.lakebridge.target_technology,
            "--skip-validation", "true",
            "--profile", self.profile,
        ])

        cmd_display = " \\\n    ".join(cmd)
        console.print(f"  [dim]$ {cmd_display}[/dim]")

        result = run_command(
            cmd,
            verbose=self.verbose,
            description="BladeBridge transpile",
        )

        # Detect Lakebridge-specific failures and give actionable messages
        if result.returncode != 0:
            stderr = result.stderr.lower()
            if "not found" in stderr or "no such file" in stderr:
                raise LakebridgeNotFoundError(
                    f"BladeBridge command failed — binary not found.\n"
                    f"Stderr: {result.stderr.strip()[:300]}\n"
                    f"Run 'databricks labs lakebridge install-transpile' to install."
                )
            if "rate limit" in stderr or "403" in stderr:
                console.print(
                    "  [yellow]Warning: GitHub rate limit hit during BladeBridge call. "
                    "Set DATABRICKS_LABS_SKIP_UPDATE_CHECK=true or add lakebridge.cli_path to ds2dbx.yml[/yellow]"
                )

        return result
