"""Pass 4 — Shell script conversion via Switch LLM transpiler."""

from __future__ import annotations

import re
from pathlib import Path

from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.lakebridge import SwitchRunner
from ds2dbx.scanner.folder import UseCaseManifest
from ds2dbx.utils.status import is_pass_completed, start_pass, complete_pass, fail_pass

from ds2dbx.passes.base import BasePass
from ds2dbx.passes.pass3_transpile import _fill_widget_defaults

console = Console()

# Remnant patterns that should not appear in converted output
_IMPALA_SHELL_RE = re.compile(r"impala-shell", re.IGNORECASE)
_KINIT_RE = re.compile(r"\bkinit\b", re.IGNORECASE)
_KUDU_RE = re.compile(r"\bkudu\b", re.IGNORECASE)
_HDFS_RE = re.compile(r"\bhdfs\s+dfs\b", re.IGNORECASE)
_BEELINE_RE = re.compile(r"\bbeeline\b", re.IGNORECASE)

_REMNANT_PATTERNS = [_IMPALA_SHELL_RE, _KINIT_RE, _KUDU_RE, _HDFS_RE, _BEELINE_RE]


class Pass4Shell(BasePass):
    """Convert shell logic scripts to Databricks notebooks via Switch."""

    @property
    def pass_name(self) -> str:
        return "pass4_shell"

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
        work_dir = self.output_dir / "pass4_shell"
        input_dir = work_dir / "input"
        output_dir = work_dir / "output"
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        logic_scripts = manifest.shell_logic_scripts
        skipped = manifest.shell_skip_scripts

        if not logic_scripts:
            console.print("  [yellow]No logic shell scripts found — skipping Pass 4[/yellow]")
            return {"logic_scripts": 0, "skipped_wrappers": len(skipped), "converted": 0}

        console.print(
            f"  Found {len(logic_scripts)} logic script(s), "
            f"skipping {len(skipped)} SSH wrapper(s)"
        )

        # --- Step 1: Build DDL context from Pass 1 output (if available) ---
        ddl_context = ""
        pass1_output = self.output_dir / "pass1_ddl" / "output"
        if pass1_output.is_dir():
            ddl_parts: list[str] = []
            for f in sorted(pass1_output.glob("*.py")):
                ddl_parts.append(f.read_text(encoding="utf-8", errors="replace"))
            if ddl_parts:
                ddl_context = "\n".join(ddl_parts)

        # --- Step 2: For each logic script, prepare input with context ---
        for script in logic_scripts:
            script_content = script.read_text(encoding="utf-8", errors="replace")

            # Build combined input: catalog/schema hint + DDL context + script
            parts: list[str] = []
            parts.append(f"# TARGET CATALOG: {self.config.catalog}")
            parts.append(f"# TARGET SCHEMA: {self.config.get_target_schema()}")
            parts.append("")
            if ddl_context:
                parts.append(f"# DDL CONTEXT (for reference, do not re-create these tables):\n{ddl_context}\n")
            parts.append(f"# SHELL SCRIPT: {script.name}\n{script_content}")

            # Use .py extension — Switch only processes .py files
            combined_path = input_dir / (script.stem + ".py")
            combined_path.write_text("\n".join(parts), encoding="utf-8")

        # --- Step 3: Run Switch with custom shell prompt ---
        ws_output = f"{self.config.get_workspace_base()}/{manifest.name}/pass4_shell"
        switch = SwitchRunner(self.config, verbose=self.verbose)

        console.print(f"  Running Switch on {len(logic_scripts)} shell script(s)...")
        result = switch.transpile(input_dir, ws_output, custom_prompt="shell")

        if result.returncode != 0:
            console.print(f"  [red]Switch failed: {result.stderr[:200]}[/red]")
            raise RuntimeError(f"Switch transpile failed: {result.stderr[:500]}")

        # --- Step 4: Download output ---
        downloaded = switch.download_output(ws_output, output_dir)
        console.print(f"  Downloaded {len(downloaded)} converted notebook(s)")

        # --- Step 5: Post-process — fill widget defaults + column-aware copy + check remnants ---
        catalog = self.config.catalog
        source_schema = self.config.get_source_schema()
        target_schema = self.config.get_target_schema()
        for f in downloaded:
            content = f.read_text(encoding="utf-8", errors="replace")
            # Fill empty widget defaults with catalog/schema
            fixed = _fill_widget_defaults(content, catalog, source_schema, target_schema)
            if fixed != content:
                content = fixed
            # Fix column-aware INSERT OVERWRITE for BIGDATA_TRG_LOAD notebooks.
            # SELECT * fails when source (K_) has more columns than target (L_).
            content = _fix_select_star_insert(content)
            f.write_text(content, encoding="utf-8")
            remnants = _check_remnants(content)
            if remnants:
                console.print(
                    f"  [yellow]Warning: {f.name} contains remnants: "
                    f"{', '.join(remnants)}[/yellow]"
                )

        metrics = {
            "logic_scripts": len(logic_scripts),
            "skipped_wrappers": len(skipped),
            "converted": len(downloaded),
        }
        console.print(f"  [green]Pass 4 complete:[/green] {metrics}")
        return metrics


def _fix_select_star_insert(content: str) -> str:
    """Replace INSERT OVERWRITE ... SELECT * with column-aware version.

    When K_ (source) table has more columns than L_ (target) table,
    SELECT * causes DUPLICATE_COLUMNS. Read target columns first.
    """
    if "TBL_CRN" not in content or "TBL_TRG" not in content:
        return content
    if "SELECT *" not in content:
        return content

    # Replace the INSERT OVERWRITE ... SELECT * pattern with column-aware copy.
    # Matches: INSERT OVERWRITE {cat}.{schema}.{TBL_TRG}\nSELECT * FROM {cat}.{schema}.{TBL_CRN}
    replacement = (
        '# Column-aware copy: read target schema to avoid DUPLICATE_COLUMNS\n'
        'target_cols = [c.name for c in spark.table(f"vn.{DB_SCHEMA}.{TBL_TRG}").schema]\n'
        'col_list = ", ".join(target_cols)\n'
        'spark.sql(f"""\n'
        'INSERT OVERWRITE vn.{DB_SCHEMA}.{TBL_TRG}\n'
        'SELECT {col_list} FROM vn.{DB_SCHEMA}.{TBL_CRN}\n'
        '""")'
    )
    content = re.sub(
        r'spark\.sql\(f""".*?INSERT\s+OVERWRITE.*?SELECT\s+\*\s+FROM.*?"""\)',
        replacement,
        content,
        flags=re.DOTALL,
    )
    return content


def _check_remnants(content: str) -> list[str]:
    """Return names of legacy tools/patterns still found in the content."""
    found: list[str] = []
    names = ["impala-shell", "kinit", "kudu", "hdfs dfs", "beeline"]
    for pattern, name in zip(_REMNANT_PATTERNS, names):
        if pattern.search(content):
            found.append(name)
    return found
