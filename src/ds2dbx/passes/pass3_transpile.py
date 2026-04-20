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

        # --- Step 2b: Pre-fix UserVar notebooks before triage ---
        # UserVar notebooks have a known pattern that our post-processor handles
        # deterministically. Fix them now so they're triaged as "clean" and don't
        # get mangled by the Switch LLM.
        for nb in bb_output_dir.glob("*.py"):
            content = nb.read_text(encoding="utf-8", errors="replace")
            if "dbutils.jobs.taskValues.set" in content and _USERVAR_SELECT_RE.search(content):
                fixed = _fix_uservar_notebook(content)
                if fixed != content:
                    nb.write_text(fixed, encoding="utf-8")

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
                prompted = (
                    f"# TARGET CATALOG: {self.config.catalog}\n"
                    f"# SOURCE SCHEMA: {self.config.get_source_schema()}\n"
                    f"# TARGET SCHEMA: {self.config.get_target_schema()}\n\n"
                    f"{prompted}"
                )
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
        # Fallback: include BladeBridge versions of notebooks Switch couldn't fix.
        # These are broken but post-processing may still fix them deterministically.
        # Without this, unfixed notebooks are silently dropped and the workflow fails.
        merged_names = {f.stem for f in merged_dir.glob("*.py")}
        for f in bb_output_dir.glob("*.py"):
            if f.stem not in merged_names:
                shutil.copy2(f, merged_dir / f.name)
                console.print(f"  [yellow]Fallback: using BladeBridge version for {f.name}[/yellow]")
        # Copy clean workflows too
        for f in bb_output_dir.rglob("*.json"):
            shutil.copy2(f, merged_dir / f.name)

            # --- Step 8: Post-process notebooks and workflow JSON ---
        notebooks_fixed = 0
        # Collect source files from manifest for delimiter detection
        source_files = [Path(p) for p in manifest.source_files] if hasattr(manifest, 'source_files') else []
        for nb in merged_dir.glob("*.py"):
            if _post_process_notebook(
                nb,
                catalog=self.config.catalog,
                source_schema=self.config.get_source_schema(),
                target_schema=self.config.get_target_schema(),
                source_files=source_files,
            ):
                notebooks_fixed += 1
        if notebooks_fixed:
            console.print(f"  Post-processed {notebooks_fixed} notebook(s)")

        for wf in merged_dir.glob("*.json"):
            _post_process_workflow(wf, source_files=source_files)

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


def _post_process_notebook(
    nb_path: Path,
    catalog: str = "",
    source_schema: str = "",
    target_schema: str = "",
    source_files: list[Path] | None = None,
) -> bool:
    """Fix common BladeBridge issues in generated notebooks.

    Returns True if the notebook was modified.
    """
    content = nb_path.read_text(encoding="utf-8", errors="replace")
    original = content

    # --- Fix 0: Remove invalid imports (oracledb, SparkContext, SparkSession) ---
    # BladeBridge fallback notebooks may have these. They cause ImportError on Databricks.
    content = re.sub(r'^import oracledb\s*\n?', '', content, flags=re.MULTILINE)
    content = re.sub(r'^from pyspark import SparkContext\s*\n?', '', content, flags=re.MULTILINE)
    content = re.sub(r'^.*SparkSession\.builder.*getOrCreate.*\n?', '', content, flags=re.MULTILINE)

    # --- Fix 1: UserVar notebooks ---
    if "dbutils.jobs.taskValues.set" in content and _USERVAR_SELECT_RE.search(content):
        content = _fix_uservar_notebook(content)

    # --- Fix 2: Inject widget definitions for undefined f-string vars ---
    content = _inject_widget_definitions(content)

    # --- Fix 3: Fill empty widget defaults for catalog/schema variables ---
    if catalog or source_schema or target_schema:
        content = _fill_widget_defaults(content, catalog, source_schema, target_schema)

    # --- Fix 4: Ensure all f-string vars in JOB_RCNCL INSERTs are defined ---
    content = _fix_insert_fstring_vars(content)

    # --- Fix 4a: Cast JOB_RCNCL columns to STRING (deterministic fallback for Bug 29) ---
    content = _fix_job_rcncl_writes(content)

    # --- Fix 4a2: Fix RCNL → JOB_RCNCL table name ---
    # Some LLM conversions write to "RCNL" instead of "JOB_RCNCL"
    if re.search(r'saveAsTable\([^)]*\.RCNL\b[^_]', content):
        content = re.sub(r'(saveAsTable\([^)]*\.)RCNL\b(?!_)', r'\1JOB_RCNCL', content)

    # --- Fix 4b: Wrap JOB_RCNCL saveAsTable with retry for concurrent writes ---
    # Parallel sub-workflows write to JOB_RCNCL simultaneously → ConcurrentAppendException.
    # Wrap the write in a retry loop.
    if "JOB_RCNCL" in content and "saveAsTable" in content:
        # Replace: RCNL.write.mode('append').saveAsTable(...)
        # With: retry wrapper
        content = re.sub(
            r"(\w+)\.write\.mode\('append'\)\.saveAsTable\(([^)]*JOB_RCNCL[^)]*)\)",
            r"import time as _time\n"
            r"for _retry in range(5):\n"
            r"    try:\n"
            r"        \1.write.mode('append').saveAsTable(\2)\n"
            r"        break\n"
            r"    except Exception as _e:\n"
            r"        if 'ConcurrentAppend' in str(_e) and _retry < 4:\n"
            r"            _time.sleep(2 * (_retry + 1))\n"
            r"        else:\n"
            r"            raise",
            content,
        )

    # --- Fix 6: No-op notebooks with TBL_CRN/TBL_TRG but dummy SELECT ---
    # BladeBridge sometimes generates notebooks that receive TBL_CRN and TBL_TRG
    # parameters but lose the actual INSERT OVERWRITE logic, leaving only
    # "SELECT CAST(1 AS ...) AS dummy". Replace with the actual copy logic.
    content = _fix_noop_copy_notebook(content, catalog, target_schema)

    # --- Fix 7: UserVar TODO expressions ---
    # Switch LLM sometimes leaves TODO comments instead of converting expressions:
    #   POS_DT = ""  # TODO: convert expression: current_date() AS POS_DT
    # Convert these to actual Python/Spark expressions.
    content = _fix_uservar_todo_expressions(content)

    # --- Fix 9: Mainframe file delimiter + schema + header filtering ---
    # When a notebook reads a CSV from Volumes and has a .toDF() with many columns,
    # it's a mainframe file ingestion pattern. Fix the delimiter, add explicit schema,
    # and filter header/trailer rows.
    content = _fix_mainframe_file_read(content, source_files or [])

    # --- Fix 10a: LoadEBAN notebooks read from source_schema, not target_schema ---
    # The LLM uses various patterns to reference the schema:
    # 1. FROM {catalog}.{target_schema}.{SRC_TBL}
    # 2. FROM {catalog}.{schema}.{SRC_TBL} where schema = "ds2dbx_target"
    # 3. FROM {catalog}.{DB_ODBC_CON_DB_SCHEMA}.{SRC_TBL}
    # All of these should read from source_schema for LoadEBAN notebooks.
    if "LoadEBAN" in nb_path.name and source_schema and target_schema:
        # Fix hardcoded full path
        content = content.replace(
            f"FROM {catalog}.{target_schema}.",
            f"FROM {catalog}.{source_schema}.",
        )
        # Fix f-string with explicit target_schema variable
        content = content.replace(
            f"FROM {{catalog}}.{{target_schema}}.",
            f"FROM {{catalog}}.{{source_schema}}.",
        )
        # Fix f-string with generic {schema} variable when schema = target_schema.
        # Add source_schema variable and replace {schema} → {source_schema} in FROM clauses.
        if re.search(r'schema\s*=\s*["\']' + re.escape(target_schema) + r'["\']', content):
            # Add source_schema definition if not present
            if "source_schema" not in content:
                content = re.sub(
                    r'(schema\s*=\s*["\']' + re.escape(target_schema) + r'["\'])',
                    rf'\1\nsource_schema = "{source_schema}"',
                    content,
                    count=1,
                )
            # Replace FROM {catalog}.{schema}. with FROM {catalog}.{source_schema}. in SQL
            content = re.sub(
                r'FROM\s+\{catalog\}\.\{schema\}\.',
                'FROM {catalog}.{source_schema}.',
                content,
            )
            # Also fix FROM {schema}. (no catalog) → FROM {catalog}.{source_schema}.
            content = re.sub(
                r'FROM\s+\{schema\}\.',
                'FROM {catalog}.{source_schema}.',
                content,
            )

        # Also fix: FROM {source_schema}. (no catalog) → FROM {catalog}.{source_schema}.
        content = re.sub(
            r'FROM\s+\{source_schema\}\.',
            'FROM {catalog}.{source_schema}.',
            content,
        )
        # Fix: FROM {WIDGET_VAR}. (no catalog) → FROM {catalog}.{WIDGET_VAR}.
        # Handles patterns like FROM {DB_JDBC_CON_DB_SCHEMA}.{SRC_TBL}
        content = re.sub(
            r'FROM\s+\{(\w*(?:SCHEMA|schema)\w*)\}\.(?!\{catalog\})',
            r'FROM {catalog}.{\1}.',
            content,
        )
        # Add catalog variable if not present
        if f'catalog = "{catalog}"' not in content and "catalog = " not in content:
            content = re.sub(
                r'(source_schema\s*=\s*"[^"]*")',
                rf'catalog = "{catalog}"\n\1',
                content,
                count=1,
            )

    # --- Fix 10b: RCNCL notebooks read from target_schema, not source_schema ---
    # RCNCL reconciles target tables. The LLM sometimes puts source_schema in
    # FROM clauses, but RCNCL reads from target_schema.
    if "RCNCL_TRG" in nb_path.name and target_schema and source_schema:
        # Fix f-string style: FROM {catalog}.{source_schema}.
        content = content.replace(
            f"FROM {{catalog}}.{{source_schema}}.",
            f"FROM {{catalog}}.{{target_schema}}.",
        )
        # Fix hardcoded style: FROM vn.ds2dbx_source.
        if source_schema and target_schema:
            content = content.replace(
                f"FROM {catalog}.{source_schema}.",
                f"FROM {catalog}.{target_schema}.",
            )

    if content != original:
        nb_path.write_text(content, encoding="utf-8")
        return True
    return False


def _fix_job_rcncl_writes(content: str) -> str:
    """Ensure .saveAsTable(...JOB_RCNCL) writes use .cast('string') on all columns.

    Finds RCNL/RCNCL DataFrame .select() blocks that feed into JOB_RCNCL writes
    and adds .cast('string') to any column that doesn't already have it.
    """
    if "JOB_RCNCL" not in content:
        return content

    # Find: RCNL = DSLink9.select( ... col('X'), ... )
    # followed by RCNL.write...saveAsTable(...JOB_RCNCL...)
    lines = content.split("\n")
    i = 0
    new_lines = []
    while i < len(lines):
        line = lines[i]
        # Detect RCNL select block writing to JOB_RCNCL
        if re.match(r"\s*(?:RCNL|RCNCL_?\d?)\s*=\s*\w+\.select\(", line):
            # Check if this feeds into JOB_RCNCL (look ahead)
            block_end = i
            for j in range(i + 1, min(i + 30, len(lines))):
                if "saveAsTable" in lines[j] and "JOB_RCNCL" in lines[j]:
                    block_end = j
                    break
                if lines[j].strip() and not lines[j].strip().startswith("col(") and \
                   not lines[j].strip().startswith(")") and not lines[j].strip().startswith("#") and \
                   "alias(" in lines[j] or lines[j].strip().startswith("col("):
                    continue
                if re.match(r"\s*\)", lines[j]):
                    continue

            if block_end > i and "JOB_RCNCL" in "\n".join(lines[i:block_end + 1]):
                # Add .cast('string') to col() calls that don't have it
                for k in range(i, block_end):
                    l = lines[k]
                    if "col(" in l and ".cast(" not in l and "alias(" in l:
                        # Add .cast('string') before .alias()
                        l = re.sub(r"(col\([^)]+\))(\.alias\()", r"\1.cast('string')\2", l)
                    elif "col(" in l and ".cast(" not in l and l.strip().endswith(","):
                        l = re.sub(r"(col\([^)]+\))", r"\1.cast('string')", l)
                    elif "lit(" in l and ".cast(" not in l:
                        l = re.sub(r"(lit\([^)]+\))", r"\1.cast('string')", l)
                    elif "coalesce(" in l and ".cast(" not in l:
                        l = re.sub(r"(coalesce\([^)]+\))", r"\1.cast('string')", l)
                    elif "current_timestamp()" in l and ".cast(" not in l:
                        l = l.replace("current_timestamp()", "current_timestamp().cast('string')")
                    lines[k] = l

        new_lines.append(lines[i])
        i += 1

    return "\n".join(new_lines)


_JOB_RCNCL_INSERT_RE = re.compile(
    r'spark\.sql\(f?"{3}[^"]*INSERT\s+INTO\s+[^"]*JOB_RCNCL[^"]*"{3}\s*\)',
    re.DOTALL | re.IGNORECASE,
)


def _fix_job_rcncl_inserts(content: str, catalog: str = "", target_schema: str = "") -> str:
    """Rewrite any INSERT INTO JOB_RCNCL to a known-correct pattern.

    The Switch LLM sometimes generates broken INSERT statements (stray quotes,
    incomplete VALUES, CAST wrapping). This deterministically replaces them
    with a clean f-string INSERT using widget variables.
    """
    if "JOB_RCNCL" not in content or "INSERT" not in content.upper():
        return content

    # Find widget variables defined in the notebook for the 14 JOB_RCNCL columns
    # Standard mapping: which widget provides each column
    col_map = {
        "JOB_NM": "JOB_NM",
        "PRJ_NM": "PRJ_NM",
        "SCMA_NM": "SCMA_NM",
        "TBL_NM": "TBL_NM",
        "SEQ_OR_SCP_NM": "SEQ_NM",
        "AMT_COL_SRC": None,  # empty string
        "TTL_AMT_SRC": None,  # '0'
        "TTL_REC_SRC": None,  # '0'
        "AMT_COL_TGT": None,  # empty string
        "TTL_AMT_TGT": None,  # '0'
        "TTL_REC_TGT": None,  # '0'
        "POS_DT": "posn_dt",
        "STRT_TMS": "STRT_TMS",
        "END_TMS": None,  # '9999-12-31 23:59:59'
    }

    # Check which variables exist in the notebook
    defined_vars = set()
    for m in re.finditer(r"(\w+)\s*=\s*dbutils\.widgets\.get", content):
        defined_vars.add(m.group(1))

    # Also check for alternative names
    alt_names = {
        "TBL_NM": ["P_TBL_NM", "TBL_NM"],
        "SEQ_NM": ["SEQ_NM", "SEQ_OR_SCP_NM"],
        "posn_dt": ["posn_dt", "POS_DT", "POSN_DT"],
        "STRT_TMS": ["STRT_TMS", "strt_tms"],
    }

    def _get_var(col_name: str) -> str:
        widget_name = col_map.get(col_name)
        if widget_name is None:
            if col_name in ("AMT_COL_SRC", "AMT_COL_TGT"):
                return "''"
            if col_name in ("TTL_AMT_SRC", "TTL_REC_SRC", "TTL_AMT_TGT", "TTL_REC_TGT"):
                return "'0'"
            if col_name == "END_TMS":
                return "'9999-12-31 23:59:59'"
            return "''"
        # Find the actual variable name in the notebook
        for alt in alt_names.get(widget_name, [widget_name]):
            if alt in defined_vars:
                return f"'{{{alt}}}'"
        return f"'{{{widget_name}}}'"

    schema_ref = f"{{{catalog}}}.{{{target_schema}}}" if catalog and target_schema else "JOB_RCNCL"
    if not catalog:
        schema_ref = "JOB_RCNCL"

    replacement = (
        f'spark.sql(f"""\n'
        f'INSERT INTO {catalog}.{target_schema}.JOB_RCNCL\n'
        f'(JOB_NM, PRJ_NM, SCMA_NM, TBL_NM, SEQ_OR_SCP_NM, AMT_COL_SRC, TTL_AMT_SRC, TTL_REC_SRC, AMT_COL_TGT, TTL_AMT_TGT, TTL_REC_TGT, POS_DT, STRT_TMS, END_TMS)\n'
        f'VALUES (\n'
        f'    {_get_var("JOB_NM")}, {_get_var("PRJ_NM")}, {_get_var("SCMA_NM")}, {_get_var("TBL_NM")}, {_get_var("SEQ_OR_SCP_NM")},\n'
        f'    {_get_var("AMT_COL_SRC")}, {_get_var("TTL_AMT_SRC")}, {_get_var("TTL_REC_SRC")}, {_get_var("AMT_COL_TGT")}, {_get_var("TTL_AMT_TGT")}, {_get_var("TTL_REC_TGT")},\n'
        f'    {_get_var("POS_DT")}, {_get_var("STRT_TMS")}, {_get_var("END_TMS")}\n'
        f')\n'
        f'""")'
    )

    content = _JOB_RCNCL_INSERT_RE.sub(replacement, content)
    return content


def _fix_insert_fstring_vars(content: str) -> str:
    """Ensure all {VAR} references in INSERT INTO JOB_RCNCL f-strings are defined.

    The Switch LLM sometimes uses f-string variables in INSERT VALUES that aren't
    defined as Python variables (e.g., PRJ_NM used in f-string but only exists as
    a DataFrame column via lit('EBAN').alias('PRJ_NM')). This adds missing variable
    definitions before the INSERT statement.
    """
    if "JOB_RCNCL" not in content or "INSERT" not in content.upper():
        return content

    lines = content.splitlines()

    # Collect all defined Python variables
    defined_vars = set()
    for line in lines:
        m = re.match(r'^(\w+)\s*=\s*', line.strip())
        if m:
            defined_vars.add(m.group(1))
    # Known builtins
    defined_vars |= {"catalog", "schema", "target_schema", "source_schema", "spark", "dbutils"}

    # Find {VAR} references in INSERT INTO JOB_RCNCL VALUES(...)
    insert_vars: set[str] = set()
    in_insert = False
    for line in lines:
        if "INSERT INTO" in line.upper() and "JOB_RCNCL" in line:
            in_insert = True
        if in_insert:
            for m in re.finditer(r"\{(\w+)\}", line):
                var = m.group(1)
                if var not in {"catalog", "target_schema", "source_schema", "schema"}:
                    insert_vars.add(var)
            if '"""' in line and in_insert and "INSERT" not in line.upper():
                in_insert = False

    # Find undefined vars
    undefined = insert_vars - defined_vars
    if not undefined:
        return content

    # Try to find values from lit('value').alias('VAR') patterns
    var_defs: list[str] = []
    for var in sorted(undefined):
        m = re.search(rf"lit\(['\"]([^'\"]*?)['\"]\)\.alias\(['\"]?{var}['\"]?\)", content)
        if m:
            var_defs.append(f"{var} = '{m.group(1)}'")
        else:
            var_defs.append(f"{var} = ''")

    # Insert definitions before the first INSERT INTO JOB_RCNCL
    new_lines = []
    inserted = False
    for line in lines:
        if not inserted and "INSERT INTO" in line.upper() and "JOB_RCNCL" in line:
            new_lines.append("# Auto-defined variables for JOB_RCNCL INSERT f-string")
            for defn in var_defs:
                new_lines.append(defn)
            inserted = True
        new_lines.append(line)

    return "\n".join(new_lines)


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

    # Normalize malformed BladeBridge concatenation before parsing.
    # BladeBridge sometimes produces: "SUBSTRING(...) || SUBSTRING(...) SUBSTRING( + param, ...)"
    # The stray "+" and missing "||" break regex matching.
    normalized = expr
    # Remove stray + before parameter names inside SUBSTRING: "( + POS_DT" -> "( POS_DT"
    normalized = re.sub(r'\(\s*\+\s*(\w)', r'( \1', normalized)
    # Normalize "||" and missing concatenation operators to spaces (we just extract all SUBSTRINGs)
    normalized = normalized.replace('||', ' ')

    # Case 2: Simple SUBSTRING(param, start, len)
    # BladeBridge uses "Substrings ( param , start , len )"
    substr_parts = re.findall(
        r"[Ss]ubstrings?\s*\(\s*(\w+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)",
        normalized,
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
        normalized, re.IGNORECASE,
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


def _fill_widget_defaults(content: str, catalog: str, source_schema: str, target_schema: str) -> str:
    """Fill empty widget defaults with catalog/schema values.

    BladeBridge generates widgets like:
        dbutils.widgets.text(name='DB_ODBC_CON_DB_NAME', defaultValue='')

    This fills empty defaults with the appropriate catalog/schema.
    Context-aware: LoadEBAN notebooks read from source_schema, RCNCL/BIGDATA
    notebooks read from target_schema. Detect by checking if the notebook reads
    from source_schema (has FROM {source_schema} or SRC_TBL widget).
    """
    # Determine if this notebook reads from source (LoadEBAN pattern)
    # vs writes to target (RCNCL/BIGDATA pattern).
    # LoadEBAN notebooks have SRC_TBL widget and read via UNION ALL from source.
    # RCNCL/BIGDATA notebooks read from target tables (P_*, K_*).
    has_src_tbl_widget = bool(re.search(r"SRC_TBL", content))
    has_union_all = bool(re.search(r"UNION\s+ALL", content, re.IGNORECASE))
    has_source_schema_ref = bool(re.search(r"source_schema", content))
    is_source_reader = (has_src_tbl_widget and has_union_all) or has_source_schema_ref

    def _replace_empty_default(m: re.Match) -> str:
        name = m.group(1)
        name_upper = name.upper()

        # Check if the widget name suggests a catalog or schema
        for kw in _CATALOG_KEYWORDS:
            if kw in name_upper:
                return f"dbutils.widgets.text(name = '{name}', defaultValue = '{catalog}')"
        for kw in _SCHEMA_KEYWORDS:
            if kw in name_upper:
                # RCNCL_CON_DB_SCHEMA always uses target_schema (JOB_RCNCL lives there).
                # DB_JDBC/ODBC_CON_DB_SCHEMA in LoadEBAN uses source_schema (reads source).
                # Everything else uses target_schema.
                if "RCNCL" in name_upper:
                    schema_val = target_schema
                elif is_source_reader and ("JDBC" in name_upper or "ODBC" in name_upper):
                    schema_val = source_schema
                else:
                    schema_val = target_schema
                return f"dbutils.widgets.text(name = '{name}', defaultValue = '{schema_val}')"

        return m.group(0)  # No match — leave as-is

    # Match: dbutils.widgets.text(name='X', defaultValue='') — with optional spaces around =
    content = re.sub(
        r"dbutils\.widgets\.text\(name\s*=\s*'(\w+)'\s*,\s*defaultValue\s*=\s*''\)",
        _replace_empty_default,
        content,
    )

    return content


def _fix_noop_copy_notebook(content: str, catalog: str = "", target_schema: str = "") -> str:
    """Fix no-op notebooks that have TBL_CRN/TBL_TRG params but just SELECT 1 AS dummy.

    BladeBridge sometimes generates INSERT OVERWRITE wrappers as dummy notebooks.
    The original DataStage job copies data from TBL_CRN (source table in same schema)
    to TBL_TRG (target table). Replace the dummy SELECT with actual INSERT OVERWRITE.
    """
    has_tbl_crn = "TBL_CRN" in content
    has_tbl_trg = "TBL_TRG" in content
    has_dummy = bool(re.search(r"SELECT\s+CAST\(\s*1\s+AS\s+", content, re.IGNORECASE))

    if not (has_tbl_crn and has_tbl_trg and has_dummy):
        return content

    # Build the replacement logic: INSERT OVERWRITE TBL_TRG SELECT * FROM TBL_CRN
    cat = catalog or "vn"
    schema = target_schema or "ds2dbx_target"

    # Replace the dummy SELECT and show() with the actual copy logic.
    # Match the pattern: processing node SOURCE ... spark.sql(...)SELECT CAST(1...)... + processing node LOG_ROW ... show()
    # Handle both spark.sql("...") and spark.sql(f"""...""") formats.
    # Use column-aware copy to handle schema differences (K_ table may have extra columns).
    replacement = (
        f'# Processing: INSERT OVERWRITE from TBL_CRN into TBL_TRG\n'
        f'# Read target table columns to handle schema differences\n'
        f'target_cols = [c.name for c in spark.table(f"{cat}.{schema}.{{TBL_TRG}}").schema]\n'
        f'col_list = ", ".join(target_cols)\n'
        f'spark.sql(f"""\n'
        f'INSERT OVERWRITE {cat}.{schema}.{{TBL_TRG}}\n'
        f'SELECT {{col_list}} FROM {cat}.{schema}.{{TBL_CRN}}\n'
        f'""")'
    )
    content = re.sub(
        r'# Processing node \w+, type SOURCE.*?'
        r'\w+\s*=\s*spark\.sql\(\s*f?["\'{]{0,3}.*?SELECT\s+CAST\(\s*1\s+.*?'
        r'# Processing node \w+, type LOG_ROW.*?'
        r'\w+\.show\(\)',
        replacement,
        content,
        flags=re.DOTALL,
    )

    return content


def _fix_uservar_todo_expressions(content: str) -> str:
    """Fix UserVar TODO expressions left by the LLM.

    Handles patterns like:
        POS_DT = ""  # TODO: convert expression: current_date() AS POS_DT
        STRT_TMS = ""  # TODO: convert expression: date_format(current_timestamp(), ...) AS STRT_TMS
    """
    if "# TODO: convert expression:" not in content:
        return content

    lines = content.splitlines()
    new_lines = []
    for line in lines:
        m = re.match(
            r'^(\w+)\s*=\s*""\s*#\s*TODO:\s*convert expression:\s*(.+)$',
            line.strip(),
        )
        if m:
            var_name = m.group(1)
            expr = m.group(2).strip()

            # Remove "AS VAR_NAME" suffix if present
            expr = re.sub(r'\s+AS\s+\w+\s*$', '', expr, flags=re.IGNORECASE).strip()

            # Convert known expressions to Python
            if re.search(r'current_date\(\)', expr, re.IGNORECASE):
                new_lines.append(
                    f'{var_name} = str(spark.sql("SELECT current_date()").first()[0])'
                )
                continue
            elif re.search(r'current_timestamp\(\)', expr, re.IGNORECASE):
                new_lines.append(
                    f'{var_name} = str(spark.sql("SELECT date_format(current_timestamp(), \'yyyy-MM-dd HH:mm:ss\')").first()[0])'
                )
                continue

        new_lines.append(line)

    return "\n".join(new_lines)


def _fix_mainframe_file_read(content: str, source_files: list[Path]) -> str:
    """Fix mainframe file ingestion: delimiter, explicit schema, header filter.

    Detects notebooks that read a CSV from Volumes and have many column aliases
    (either via .toDF() or .select(col("_c0").alias(...))). Fixes:
    1. Delimiter — detect from actual source file
    2. Schema — add explicit StructType so header rows don't break column count
    3. Header filter — filter rows where a key column is null/empty
    """
    if ".csv(" not in content:
        return content

    # --- Detect column names from .toDF() or _c* alias patterns ---
    col_names: list[str] = []

    # Pattern 1: .toDF("col1", "col2", ...)
    todf_match = re.search(r'(\w+)\s*=\s*(\w+)\.toDF\(\s*\n?(.*?)\)', content, re.DOTALL)
    if todf_match:
        col_names = re.findall(r'"([^"]+)"', todf_match.group(3))

    # Pattern 2: .select(col("_c0").alias("COL1"), ...) or col('_c0').alias(...)
    if not col_names:
        c_aliases = re.findall(r"col\(['\"]_c\d+['\"]\)\.alias\(['\"]([^'\"]+)['\"]\)", content)
        if len(c_aliases) >= 5:
            col_names = c_aliases

    # Pattern 3: Comments mapping _c* to column names (LLM puts mapping in comments)
    if not col_names:
        comment_mapping = re.findall(r'#\s*_c\d+\s*=\s*(\w+)', content)
        if len(comment_mapping) >= 5:
            col_names = comment_mapping

    if len(col_names) < 5:
        return content  # Not a mainframe file pattern

    # --- Detect delimiter from source file ---
    detected_sep = "|"  # default for mainframe
    for sf in source_files:
        if sf.exists():
            try:
                lines = sf.read_text(encoding="utf-8", errors="replace").splitlines()
                max_fields = 0
                for line in lines[1:5]:
                    for sep in ["|", "\t", ",", ";"]:
                        nf = len(line.split(sep))
                        if nf > max_fields:
                            max_fields = nf
                            detected_sep = sep
                break
            except Exception:
                pass

    # --- Fix delimiter in .option("sep", ...) ---
    content = re.sub(
        r'\.option\(\s*"sep"\s*,\s*"[^"]*"\s*\)',
        f'.option("sep", "{detected_sep}")',
        content,
    )

    # --- Add explicit StructType schema to CSV read ---
    # This ensures Spark reads the correct number of columns even when
    # the header/trailer row has fewer fields.
    schema_import = 'from pyspark.sql.types import StructType, StructField, StringType'
    col_list_str = ", ".join(f'"{c}"' for c in col_names)
    schema_def = (
        f'{schema_import}\n'
        f'_col_names = [{col_list_str}]\n'
        f'_schema = StructType([StructField(c, StringType(), True) for c in _col_names])'
    )

    # Insert schema before the csv read and add .schema(_schema)
    csv_read_re = re.compile(
        r'(\w+)\s*=\s*spark\.read'
        r'\.option\("header"\s*,\s*"false"\)'
        r'\.option\("sep"\s*,\s*"[^"]*"\)'
        r'\.csv\(',
    )
    csv_m = csv_read_re.search(content)
    if csv_m:
        var_name = csv_m.group(1)
        # Insert schema definition before the read
        content = content[:csv_m.start()] + schema_def + "\n" + content[csv_m.start():]
        # Re-find after insertion
        csv_m = csv_read_re.search(content)
        if csv_m:
            # Add .schema(_schema) before .csv(
            old = csv_m.group(0)
            new = old.replace('.csv(', '.schema(_schema).csv(')
            content = content.replace(old, new, 1)

    # --- Replace .toDF() or _c* select with named columns from schema ---
    if todf_match:
        # Remove the .toDF() call
        content = re.sub(
            r'\w+\s*=\s*\w+\.toDF\([^)]*\)',
            '# Column names applied via schema at read time',
            content,
        )
    else:
        # Replace all _c* column references with named columns from schema
        # Handles: col("_c0"), col('_c0'), etc.
        for i, name in enumerate(col_names):
            content = re.sub(
                rf"""col\(['"]{re.escape(f'_c{i}')}'?\)""",
                f"col('{name}')",
                content,
            )

    # --- Add header/trailer filter after the CSV read ---
    # Use the first column (typically ORG code like "001") to identify data rows.
    # Header/trailer rows have "000" in the first column while data rows have "001"+.
    # Filter: first column must not be "000" or null/empty.
    var_name = csv_m.group(1) if csv_m else "DSLink2"
    first_col = col_names[0]
    filter_line = (
        f'\n# Filter out header/trailer rows (mainframe control records)\n'
        f'{var_name} = {var_name}'
        f'.filter((col("{first_col}") != "000") & col("{first_col}").isNotNull())'
    )

    # Insert filter after the csv read closing paren
    csv_close = re.search(r'\.schema\(_schema\)\.csv\([^)]*\)', content)
    if csv_close:
        insert_pos = csv_close.end()
        content = content[:insert_pos] + filter_line + content[insert_pos:]

    return content


def _post_process_workflow(wf_path: Path, source_files: list[Path] | None = None) -> None:
    """Remove existing_cluster_id, add environment_key, and fix base_parameters."""
    try:
        data = json.loads(wf_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return

    modified = False

    # Walk through tasks and fix cluster references + base_parameters
    tasks = data.get("tasks", [])

    # Remove Sequencer_*/Abort tasks (DataStage flow-control artifacts, no purpose in Databricks)
    removed_keys = set()
    for task in tasks:
        tk = task.get("task_key", "")
        if tk.startswith("Sequencer") or tk == "Abort":
            removed_keys.add(tk)
    if removed_keys:
        tasks = [t for t in tasks if t.get("task_key", "") not in removed_keys]
        # Remove from depends_on lists
        for task in tasks:
            deps = task.get("depends_on", [])
            if deps:
                task["depends_on"] = [d for d in deps if d.get("task_key", "") not in removed_keys]
                if not task["depends_on"]:
                    del task["depends_on"]
        data["tasks"] = tasks
        modified = True

    # Wire shell-converted notebooks into the workflow.
    # When a task has KUDU_SCRIPT parameter, it originally called a shell script.
    # BladeBridge captures only the pre/post-SQL reconciliation, not the actual
    # script logic. The script was converted by pass4 — inject it as a preceding task.
    shell_output_dir = wf_path.parent.parent / "pass4_shell" / "output"
    if shell_output_dir.exists():
        shell_notebooks = {nb.stem: nb.name for nb in shell_output_dir.glob("*.py")}
        if shell_notebooks:
            # Resolve KUDU_SCRIPT values from UserVar notebooks (since workflow
            # has {{tasks.UserVar.values.KUDU_SCRIPT}} at definition time)
            kudu_script_values = {}
            for nb in wf_path.parent.glob("*UserVar*.py"):
                uv_content = nb.read_text(encoding="utf-8", errors="replace")
                for m in re.finditer(r"""["']KUDU_SCRIPT["']\s*,\s*value\s*=\s*["']([^"']+)["']""", uv_content):
                    kudu_script_values["KUDU_SCRIPT"] = m.group(1)
                # Also check: val = "CIS/SCD_K_..."  + taskValues.set(key='KUDU_SCRIPT')
                for m in re.finditer(r'val\s*=\s*["\']([^"\']+)["\'].*?taskValues\.set\(\s*key\s*=\s*["\']KUDU_SCRIPT', uv_content, re.DOTALL):
                    kudu_script_values["KUDU_SCRIPT"] = m.group(1)

            new_tasks = []
            for task in tasks:
                bp = task.get("notebook_task", {}).get("base_parameters", {})
                kudu_script = bp.get("KUDU_SCRIPT", "")
                # Resolve task value references
                if "{{" in kudu_script:
                    kudu_script = kudu_script_values.get("KUDU_SCRIPT", "")

                if kudu_script and shell_notebooks:
                    script_stem = Path(kudu_script).stem
                    matching_nb = None
                    for nb_stem in shell_notebooks:
                        if script_stem.lower() in nb_stem.lower() or nb_stem.lower() in script_stem.lower():
                            matching_nb = nb_stem
                            break

                    if matching_nb:
                        shell_task_key = f"SHELL_{matching_nb}"
                        shell_task = {
                            "task_key": shell_task_key,
                            "depends_on": task.get("depends_on", []).copy(),
                            "notebook_task": {
                                "notebook_path": f"/Workspace/Users/{matching_nb}",
                                "source": "WORKSPACE",
                                "base_parameters": {
                                    "DB_SCHEMA": bp.get("SCMA_NM", bp.get("DB_ODBC_CON_DB_SCHEMA", "")),
                                    "SRC_TBL": "{{tasks.UserVar.values.TBL_NM}}",
                                    "K_TBL": "K_{{tasks.UserVar.values.TBL_NM}}",
                                    "POS_DT": bp.get("POS_DT", "{{job.parameters.POS_DT}}"),
                                },
                            },
                            "environment_key": "default",
                            "max_retries": 3,
                            "min_retry_interval_millis": 2000,
                            "run_if": "ALL_SUCCESS",
                        }
                        new_tasks.append(shell_task)
                        task["depends_on"] = [{"task_key": shell_task_key}]
                        modified = True

                new_tasks.append(task)

            if new_tasks != tasks:
                tasks = new_tasks
                data["tasks"] = tasks

    # Parallelize independent sub-workflow tasks in orchestrator workflows.
    # BladeBridge chains sub-workflows sequentially by default, but if each task
    # is a run_job_task (sub-workflow) with no shared data dependencies, they can
    # run in parallel. Detect this pattern and remove the linear depends_on chain.
    all_run_job = all("run_job_task" in t for t in tasks)
    has_linear_chain = all(
        len(t.get("depends_on", [])) <= 1 for t in tasks
    )
    if all_run_job and has_linear_chain and len(tasks) > 1:
        for task in tasks:
            task.pop("depends_on", None)
        modified = True

    # Strip BladeBridge quotes from job-level parameter defaults
    for param in data.get("parameters", []):
        default = param.get("default", "")
        if isinstance(default, str) and re.match(r"^'[^']*'$", default):
            param["default"] = default[1:-1]
            modified = True

    # Fix D: Derive POS_DT default from source file date suffix.
    # Source files like CPACP.DIH.KB1DH007.CPAHF.D010426 encode the date as DDMMYY.
    # Convert to YYYY-MM-DD and set as POS_DT default so the workflow finds the file.
    if source_files:
        for param in data.get("parameters", []):
            if param.get("name") == "POS_DT":
                for sf in (source_files or []):
                    m = re.search(r'\.D(\d{2})(\d{2})(\d{2})$', sf.name)
                    if m:
                        dd, mm, yy = m.group(1), m.group(2), m.group(3)
                        year = f"20{yy}"
                        new_date = f"{year}-{mm}-{dd}"
                        if param.get("default") != new_date:
                            param["default"] = new_date
                            modified = True
                        break

    for task in tasks:
        if "existing_cluster_id" in task:
            del task["existing_cluster_id"]
            modified = True

        # Convert spark_python_task → notebook_task (Serverless compatibility)
        # BladeBridge generates spark_python_task with %TASK_PATH% placeholders
        sp_task = task.get("spark_python_task", {})
        if "python_file" in sp_task:
            py_file = sp_task["python_file"]
            nb_name = Path(py_file).stem
            task.pop("spark_python_task")
            task["notebook_task"] = {
                "notebook_path": f"/Workspace/Users/{nb_name}",
                "source": "WORKSPACE",
            }
            modified = True

        if "job_cluster_key" not in task and "environment_key" not in task:
            task["environment_key"] = "default"
            modified = True

        # Fix base_parameters: strip quotes, convert task value interpolation
        nb_task = task.get("notebook_task", {})
        bp = nb_task.get("base_parameters", {})
        if bp:
            new_bp = _fix_base_parameters(bp)
            if new_bp != bp:
                nb_task["base_parameters"] = new_bp
                modified = True

    if modified:
        wf_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# Pattern: TaskName.PARAM (BladeBridge task value reference)
_TASK_VALUE_RE = re.compile(r'(\w+)\.(\w+)')

# Pattern: TaskName.{{job.parameters.PARAM}} (mixed BladeBridge + Databricks syntax)
_MIXED_JOB_PARAM_RE = re.compile(r'\w+\.\{\{(job\.parameters\.\w+)\}\}')


def _fix_base_parameters(bp: dict) -> dict:
    """Fix BladeBridge base_parameters syntax to valid Databricks syntax.

    Handles:
    1. Surrounding single quotes: "'VALUE'" -> "VALUE"
    2. Task value references: "TaskName.PARAM" -> "{{tasks.TaskName.values.PARAM}}"
    3. Mixed syntax: "TaskName.{{job.parameters.X}}" -> "{{job.parameters.X}}"
    4. Concatenation expressions: "'P_' + TaskName.PARAM" -> "P_{{tasks.TaskName.values.PARAM}}"
    """
    fixed = {}
    for key, val in bp.items():
        if not isinstance(val, str) or not val:
            fixed[key] = val
            continue
        fixed[key] = _fix_param_value(val)
    return fixed


def _fix_param_value(val: str) -> str:
    """Convert a single BladeBridge parameter value to Databricks syntax."""
    val = val.strip()

    # Simple quoted literal: "'VALUE'" -> "VALUE"
    if re.match(r"^'[^']*'$", val):
        return val[1:-1]

    # Already valid Databricks syntax: "{{...}}" or "'{{...}}'" (with stray quotes)
    if val.startswith("{{") and val.endswith("}}"):
        return val
    if val.startswith("'{{") and val.endswith("}}'"):
        return val[1:-1]

    # Mixed: "TaskName.{{job.parameters.PARAM}}" -> "{{job.parameters.PARAM}}"
    if _MIXED_JOB_PARAM_RE.fullmatch(val):
        m = _MIXED_JOB_PARAM_RE.fullmatch(val)
        return "{{" + m.group(1) + "}}"

    # Concatenation expression with + operator
    if "+" in val:
        return _fix_concat_expression(val)

    # Simple task value reference: "TaskName.PARAM" -> "{{tasks.TaskName.values.PARAM}}"
    m = _TASK_VALUE_RE.fullmatch(val)
    if m:
        task_name, param = m.group(1), m.group(2)
        return "{{" + f"tasks.{task_name}.values.{param}" + "}}"

    return val


def _fix_concat_expression(expr: str) -> str:
    """Convert BladeBridge concatenation expression to Databricks interpolation string.

    Examples:
        "'P_' + TaskName.tbl_nm" -> "P_{{tasks.TaskName.values.tbl_nm}}"
        "TaskName.FILE + '.D' + TaskName.EXT" -> "{{tasks.TaskName.values.FILE}}.D{{tasks.TaskName.values.EXT}}"
    """
    parts = [p.strip() for p in expr.split("+")]
    result = []
    for part in parts:
        # Quoted literal: 'VALUE'
        if re.match(r"^'[^']*'$", part):
            result.append(part[1:-1])
        # Mixed: TaskName.{{job.parameters.X}}
        elif _MIXED_JOB_PARAM_RE.fullmatch(part):
            m = _MIXED_JOB_PARAM_RE.fullmatch(part)
            result.append("{{" + m.group(1) + "}}")
        # Task value ref: TaskName.PARAM
        elif _TASK_VALUE_RE.fullmatch(part):
            m = _TASK_VALUE_RE.fullmatch(part)
            result.append("{{" + f"tasks.{m.group(1)}.values.{m.group(2)}" + "}}")
        else:
            result.append(part)
    return "".join(result)
