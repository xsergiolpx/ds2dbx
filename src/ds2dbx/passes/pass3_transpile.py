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

            # --- Step 8: Post-process notebooks and workflow JSON ---
        notebooks_fixed = 0
        for nb in merged_dir.glob("*.py"):
            if _post_process_notebook(nb, catalog=self.config.catalog, schema=self.config.schema):
                notebooks_fixed += 1
        if notebooks_fixed:
            console.print(f"  Post-processed {notebooks_fixed} notebook(s)")

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


# ---------------------------------------------------------------------------
# Notebook post-processing
# ---------------------------------------------------------------------------

# Pattern: spark.sql("SELECT param_name")  (BladeBridge UserVar bug)
_USERVAR_SELECT_RE = re.compile(
    r'df\s*=\s*spark\.sql\(\s*"SELECT\s+(.+?)"\s*\)',
)

# Pattern: Substrings(...) → SUBSTRING(...)
_SUBSTRINGS_RE = re.compile(r'\bSubstrings\s*\(', re.IGNORECASE)

# Pattern: f-string variable {VAR_NAME} in spark.sql(f"""...""")
_FSTRING_VAR_RE = re.compile(r'\{([A-Z_][A-Z0-9_]*)\}')


def _post_process_notebook(nb_path: Path, catalog: str = "", schema: str = "") -> bool:
    """Fix common BladeBridge issues in generated notebooks.

    Returns True if the notebook was modified.
    """
    content = nb_path.read_text(encoding="utf-8", errors="replace")
    original = content

    # --- Fix 1: UserVar notebooks ---
    if "dbutils.jobs.taskValues.set" in content and _USERVAR_SELECT_RE.search(content):
        content = _fix_uservar_notebook(content)

    # --- Fix 2: Inject widget definitions for undefined f-string vars ---
    content = _inject_widget_definitions(content)

    # --- Fix 3: Fill empty widget defaults for catalog/schema variables ---
    if catalog or schema:
        content = _fill_widget_defaults(content, catalog, schema)

    if content != original:
        nb_path.write_text(content, encoding="utf-8")
        return True
    return False


def _fix_uservar_notebook(content: str) -> str:
    """Fix BladeBridge UserVar notebooks that use SELECT <param> instead of widgets.

    BladeBridge generates:
        df = spark.sql("SELECT posn_dt")
        data = df.collect()
        val = data[0][0]
        dbutils.jobs.taskValues.set(key = 'posn_dt', value = val)

    Should be:
        posn_dt = dbutils.widgets.get("posn_dt")
        dbutils.jobs.taskValues.set(key = 'posn_dt', value = posn_dt)
    """
    lines = content.splitlines()
    new_lines: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]

        # Detect: df = spark.sql("SELECT <expr>")
        m = _USERVAR_SELECT_RE.match(line.strip())
        if m:
            expr = m.group(1).strip()

            # Check if this is a simple parameter reference (just a name, no SQL functions)
            if re.match(r'^[a-zA-Z_]\w*$', expr):
                # Simple param: replace with dbutils.widgets.get
                param_name = expr

                # Skip the next 2 lines (data = df.collect(); val = data[0][0])
                # and rewrite the taskValues.set
                skip_count = 0
                task_value_line = None
                for j in range(i + 1, min(i + 4, len(lines))):
                    stripped = lines[j].strip()
                    if stripped.startswith("data = ") or stripped.startswith("val = "):
                        skip_count += 1
                    elif "dbutils.jobs.taskValues.set" in stripped:
                        task_value_line = stripped
                        skip_count += 1
                        break

                new_lines.append(f'{param_name} = dbutils.widgets.get("{param_name}")')
                if task_value_line:
                    new_lines.append(
                        f'dbutils.jobs.taskValues.set(key="{param_name}", value={param_name})'
                    )
                i += 1 + skip_count
                continue

            else:
                # Complex expression — try to convert to Python
                expr_fixed = _convert_uservar_expr(expr)

                # Skip the collect/set pattern
                skip_count = 0
                key_name = "unknown"
                for j in range(i + 1, min(i + 4, len(lines))):
                    stripped = lines[j].strip()
                    if stripped.startswith("data = ") or stripped.startswith("val = "):
                        skip_count += 1
                    elif "dbutils.jobs.taskValues.set" in stripped:
                        km = re.search(r"key\s*=\s*'(\w+)'", stripped)
                        key_name = km.group(1) if km else "unknown"
                        skip_count += 1
                        break

                new_lines.append(f'{key_name} = {expr_fixed}')
                new_lines.append(
                    f'dbutils.jobs.taskValues.set(key="{key_name}", value={key_name})'
                )
                i += 1 + skip_count
                continue

        new_lines.append(line)
        i += 1

    return "\n".join(new_lines)


def _convert_uservar_expr(expr: str) -> str:
    """Convert a BladeBridge UserVar SQL expression to Python.

    Handles:
    - String literals: SELECT 'DATALAKE' -> "DATALAKE"
    - SUBSTRING: SELECT Substrings(posn_dt, 1, 4) -> posn_dt[0:4]
    - Concatenation with + : SUBSTRING(..) + SUBSTRING(..) -> f-string slicing
    """
    expr = expr.strip()

    # Case 1: String literal — SELECT 'value'
    m = re.match(r"^'([^']*)'$", expr)
    if m:
        return f'"{m.group(1)}"'

    # Case 2: Simple SUBSTRING(param, start, len)
    # BladeBridge uses "Substrings ( param , start , len )"
    substr_parts = re.findall(
        r"[Ss]ubstrings?\s*\(\s*(\w+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)",
        expr,
    )
    if substr_parts:
        # Convert all SUBSTRING calls to Python slicing
        pieces = []
        for param, start_str, length_str in substr_parts:
            start = int(start_str) - 1  # SQL is 1-indexed, Python is 0-indexed
            length = int(length_str)
            pieces.append(f"{param}[{start}:{start + length}]")

        if len(pieces) == 1:
            return pieces[0]
        # Multiple substrings concatenated
        return " + ".join(pieces)

    # Case 3: SUBSTRING (no 's') — standard SQL function
    substr_std = re.findall(
        r"SUBSTRING\s*\(\s*(\w+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)",
        expr, re.IGNORECASE,
    )
    if substr_std:
        pieces = []
        for param, start_str, length_str in substr_std:
            start = int(start_str) - 1
            length = int(length_str)
            pieces.append(f"{param}[{start}:{start + length}]")
        return " + ".join(pieces) if pieces else f'""  # TODO: {expr}'

    # Fallback: can't convert — use spark.sql with a literal wrapper
    return f'""  # TODO: convert expression: {expr}'


def _inject_widget_definitions(content: str) -> str:
    """Inject dbutils.widgets.get() for f-string variables used in spark.sql() but not defined.

    Scans for patterns like spark.sql(f'''...{VAR_NAME}...''') where VAR_NAME
    is not assigned earlier in the notebook, and adds widget definitions.
    """
    lines = content.splitlines()

    # Collect all variable assignments (simple: VAR = ...)
    assigned_vars: set[str] = set()
    for line in lines:
        m = re.match(r'^(\w+)\s*=\s*', line.strip())
        if m:
            assigned_vars.add(m.group(1))
    # Also count imports and function defs
    assigned_vars |= {"spark", "sc", "dbutils", "os", "json", "re", "df", "data", "val",
                       "lit", "col", "when", "expr", "current_timestamp", "current_date",
                       "trim", "substring", "explode", "count", "to_timestamp"}

    # Find all f-string variables used in spark.sql() calls
    needed_vars: set[str] = set()
    in_fstring_sql = False
    for line in lines:
        if re.search(r'spark\.sql\(\s*f\s*"{3}', line) or re.search(r'spark\.sql\(\s*f\s*"', line):
            in_fstring_sql = True
        if in_fstring_sql:
            for vm in _FSTRING_VAR_RE.finditer(line):
                var_name = vm.group(1)
                if var_name not in assigned_vars:
                    needed_vars.add(var_name)
        if in_fstring_sql and ('"""' in line[line.find('"""') + 3:] if '"""' in line else '"' in line.rstrip()):
            in_fstring_sql = False

    if not needed_vars:
        return content

    # Build widget definition block
    widget_lines = ["# Auto-generated widget definitions for workflow parameters"]
    for var in sorted(needed_vars):
        widget_lines.append(f'dbutils.widgets.text("{var}", "")')
    widget_lines.append("")
    for var in sorted(needed_vars):
        widget_lines.append(f'{var} = dbutils.widgets.get("{var}")')
    widget_lines.append("")

    # Insert after the first cell separator or after imports
    insert_idx = 0
    for i, line in enumerate(lines):
        if "# COMMAND ----------" in line:
            insert_idx = i + 1
            break
        if line.startswith("import ") or line.startswith("from "):
            insert_idx = i + 1

    # Find the right insertion point after initial setup
    for i in range(insert_idx, len(lines)):
        if "# COMMAND ----------" in lines[i]:
            insert_idx = i + 1
            break

    new_lines = lines[:insert_idx] + ["# COMMAND ----------"] + widget_lines + lines[insert_idx:]
    return "\n".join(new_lines)


# Keywords in widget names that should default to catalog or schema
_CATALOG_KEYWORDS = {"DB_NAME", "CATALOG", "DATABASE"}
_SCHEMA_KEYWORDS = {"DB_SCHEMA", "SCHEMA", "SCMA_NM"}


def _fill_widget_defaults(content: str, catalog: str, schema: str) -> str:
    """Fill empty widget defaults with catalog/schema values.

    BladeBridge generates widgets like:
        dbutils.widgets.text(name='DB_ODBC_CON_DB_NAME', defaultValue='')

    This fills empty defaults with the target catalog/schema.
    """
    def _replace_empty_default(m: re.Match) -> str:
        name = m.group(1)
        name_upper = name.upper()

        # Check if the widget name suggests a catalog or schema
        for kw in _CATALOG_KEYWORDS:
            if kw in name_upper:
                return f"dbutils.widgets.text(name='{name}', defaultValue='{catalog}')"
        for kw in _SCHEMA_KEYWORDS:
            if kw in name_upper:
                return f"dbutils.widgets.text(name='{name}', defaultValue='{schema}')"

        return m.group(0)  # No match — leave as-is

    # Match: dbutils.widgets.text(name='X', defaultValue='')
    content = re.sub(
        r"dbutils\.widgets\.text\(name='(\w+)',\s*defaultValue=''\)",
        _replace_empty_default,
        content,
    )

    return content


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
