"""Pass 2 — Upload sample data to UC Volume and render loader notebook."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Template
from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.scanner.folder import UseCaseManifest
from ds2dbx.utils.status import is_pass_completed, start_pass, complete_pass, fail_pass
from ds2dbx.utils.subprocess_runner import run_command

from ds2dbx.passes.base import BasePass

console = Console()


def detect_delimiter(file: Path) -> str:
    """Auto-detect CSV/text delimiter from the first 4KB of a file."""
    sample = file.read_bytes()[:4096].decode("utf-8", errors="ignore")
    if "\x01" in sample:
        return "\u0001"
    if "|" in sample and sample.count("|") > sample.count(","):
        return "|"
    if "\t" in sample and sample.count("\t") > sample.count(","):
        return "\t"
    return ","


class Pass2Data(BasePass):
    """Upload CSV/data files to a UC Volume and render a loader notebook."""

    @property
    def pass_name(self) -> str:
        return "pass2_data"

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
        work_dir = self.output_dir / "pass2_data"
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        data_files = manifest.data_files
        if not data_files:
            console.print("  [yellow]No data files found — skipping Pass 2[/yellow]")
            return {"files_uploaded": 0, "tables_count": 0}

        lb = self.config.lakebridge
        volume_path = (
            f"/Volumes/{lb.switch_catalog}/{lb.switch_schema}/{lb.data_volume}"
        )
        profile = self.config.databricks.profile
        # Sanitize folder name (no spaces for remote paths)
        safe_name = manifest.name.replace(" ", "_")

        # Create the remote directory first
        run_command(
            ["databricks", "fs", "mkdirs", f"dbfs:{volume_path}/{safe_name}", "--profile", profile],
            verbose=self.verbose,
        )

        # --- Step 1: Upload each data file to UC Volume ---
        uploaded = 0
        table_info: list[dict] = []

        for data_file in data_files:
            remote_path = f"{volume_path}/{safe_name}/{data_file.name}"
            console.print(f"  Uploading {data_file.name} -> {remote_path}")

            result = run_command(
                [
                    "databricks", "fs", "cp",
                    str(data_file), f"dbfs:{remote_path}",
                    "--profile", profile,
                    "--overwrite",
                ],
                verbose=self.verbose,
                description=f"Upload {data_file.name}",
            )

            if result.returncode != 0:
                console.print(f"  [red]Failed to upload {data_file.name}: {result.stderr[:200]}[/red]")
                continue

            uploaded += 1

            # Detect delimiter and check for header
            delimiter = detect_delimiter(data_file)
            first_line = data_file.read_text(encoding="utf-8", errors="ignore").split("\n")[0]
            has_header = not any(c.isdigit() for c in first_line.split(delimiter)[0][:10]) if first_line else False
            # Strip known database prefixes from filenames (e.g., datalake.table.csv -> table)
            raw_stem = data_file.stem.lower()
            for prefix in ("datalake.", "datatank.", "common_layer.", "datatank_view."):
                if raw_stem.startswith(prefix):
                    raw_stem = raw_stem[len(prefix):]
                    break
            table_name = raw_stem.replace("-", "_").replace(" ", "_").replace(".", "_")

            table_info.append({
                "table_name": table_name,
                "file_path": remote_path,
                "delimiter": delimiter,
                "delimiter_display": repr(delimiter),
                "has_header": has_header,
                "filename": data_file.name,
            })

        console.print(f"  Uploaded {uploaded}/{len(data_files)} files")

        # --- Step 2: Render loader notebook from Jinja2 template ---
        if table_info:
            from importlib.resources import files

            template_str = files("ds2dbx.templates").joinpath("loader_notebook.py.j2").read_text()
            template = Template(template_str)
            notebook_content = template.render(
                catalog=self.config.catalog,
                schema=self.config.get_source_schema(),
                tables=table_info,
                usecase_name=manifest.name,
                volume_path=f"{volume_path}/{safe_name}",
            )

            notebook_path = output_dir / f"{manifest.name}_data_loader.py"
            notebook_path.write_text(notebook_content, encoding="utf-8")
            console.print(f"  Rendered loader notebook -> {notebook_path.name}")

        metrics = {
            "files_uploaded": uploaded,
            "tables_count": len(table_info),
        }
        console.print(f"  [green]Pass 2 complete:[/green] {metrics}")
        return metrics
