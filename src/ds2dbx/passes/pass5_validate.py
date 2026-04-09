"""Pass 5 — Generate validation notebook based on detected pattern."""

from __future__ import annotations

import re
from pathlib import Path

from jinja2 import Template
from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.scanner.folder import UseCaseManifest
from ds2dbx.utils.status import is_pass_completed, start_pass, complete_pass, fail_pass

from ds2dbx.passes.base import BasePass

console = Console()

# Template selection by pattern
_TEMPLATE_MAP = {
    "multi_join": "validate_multi_join.py.j2",
    "scd2": "validate_scd2.py.j2",
    "file_ingestion": "validate_file_ingestion.py.j2",
    "generic": "validate_generic.py.j2",
}


class Pass5Validate(BasePass):
    """Render a validation notebook tailored to the use case pattern."""

    @property
    def pass_name(self) -> str:
        return "pass5_validate"

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
        work_dir = self.output_dir / "pass5_validate"
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        pattern = manifest.pattern
        template_name = _TEMPLATE_MAP.get(pattern, _TEMPLATE_MAP["generic"])
        console.print(f"  Pattern: {pattern} -> template: {template_name}")

        # --- Extract table names from DDL files or Pass 1 output ---
        table_names = _extract_table_names(manifest, self.output_dir)
        console.print(f"  Found {len(table_names)} table(s) to validate")

        # --- Load and render template ---
        from importlib.resources import files

        template_str = files("ds2dbx.templates").joinpath(template_name).read_text()
        template = Template(template_str)

        notebook_content = template.render(
            catalog=self.config.catalog,
            schema=self.config.schema,
            tables=table_names,
            usecase_name=manifest.name,
            pattern=pattern,
        )

        notebook_path = output_dir / f"{manifest.name}_validation.py"
        notebook_path.write_text(notebook_content, encoding="utf-8")
        console.print(f"  Rendered validation notebook -> {notebook_path.name}")

        metrics = {
            "pattern": pattern,
            "checks_defined": len(table_names),
        }
        console.print(f"  [green]Pass 5 complete:[/green] {metrics}")
        return metrics


def _extract_table_names(manifest: UseCaseManifest, output_dir: Path) -> list[str]:
    """Extract table names from DDL sources.

    Checks Pass 1 output first (already converted), then falls back to
    raw DDL files from the manifest.
    """
    table_names: list[str] = []

    # Try Pass 1 output
    pass1_output = output_dir / "pass1_ddl" / "output"
    sources: list[Path] = []
    if pass1_output.is_dir():
        sources = sorted(pass1_output.glob("*.py"))

    # Fall back to raw DDL
    if not sources:
        sources = manifest.ddl_files

    # Extract CREATE TABLE names
    create_re = re.compile(
        r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
        re.IGNORECASE,
    )

    for src in sources:
        content = src.read_text(encoding="utf-8", errors="replace")
        for match in create_re.finditer(content):
            table_name = match.group(1).strip("`\"'")
            if table_name not in table_names:
                table_names.append(table_name)

    return table_names
