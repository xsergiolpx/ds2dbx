"""Pass 1 — DDL conversion via Switch LLM transpiler."""

from __future__ import annotations

import re
from pathlib import Path

from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.lakebridge import SwitchRunner
from ds2dbx.scanner.folder import UseCaseManifest
from ds2dbx.utils.status import is_pass_completed, start_pass, complete_pass, fail_pass

from ds2dbx.passes.base import BasePass

console = Console()

# Patterns to strip from LLM output if it missed them
_HDFS_RE = re.compile(r"LOCATION\s+'hdfs://[^']*'", re.IGNORECASE)
_STORED_AS_RE = re.compile(r"STORED\s+AS\s+\w+", re.IGNORECASE)
_ROW_FORMAT_RE = re.compile(
    r"^ROW\s+FORMAT\s+DELIMITED[^\n]*(?:\n(?!(?:CREATE|ALTER|spark\.sql|\)|#|$)).*)*",
    re.IGNORECASE | re.MULTILINE,
)
_KUDU_TBLPROPS_RE = re.compile(
    r"TBLPROPERTIES\s*\([^)]*kudu[^)]*\)", re.IGNORECASE | re.DOTALL
)
_PARTITION_HASH_RE = re.compile(
    r"PARTITION\s+BY\s+HASH\s*\([^)]*\)\s*PARTITIONS\s+\d+", re.IGNORECASE
)


class Pass1DDL(BasePass):
    """Concatenate DDL files, transpile via Switch, and post-process."""

    @property
    def pass_name(self) -> str:
        return "pass1_ddl"

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
        work_dir = self.output_dir / "pass1_ddl"
        input_dir = work_dir / "input"
        output_dir = work_dir / "output"
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        ddl_files = manifest.ddl_files
        if not ddl_files:
            console.print("  [yellow]No DDL files found — skipping Pass 1[/yellow]")
            return {"input_files": 0, "output_files": 0, "post_checks_passed": 0, "post_checks_failed": 0}

        # --- Step 1: Concatenate DDL files with section headers ---
        # Use .py extension — Switch only processes .py files
        concat_path = input_dir / "all_ddl.py"
        parts: list[str] = []
        # Add catalog/schema as comments that the LLM prompt references
        parts.append(f"# TARGET CATALOG: {self.config.catalog}")
        parts.append(f"# TARGET SCHEMA: {self.config.get_target_schema()}")
        parts.append("")
        for ddl in ddl_files:
            parts.append(f"-- ========== {ddl.name} ==========")
            parts.append(ddl.read_text(encoding="utf-8", errors="replace"))
            parts.append("")
        concat_path.write_text("\n".join(parts), encoding="utf-8")
        console.print(f"  Concatenated {len(ddl_files)} DDL files -> {concat_path.name}")

        # --- Step 2: Run Switch with custom DDL prompt ---
        ws_output = f"{self.config.get_workspace_base()}/{manifest.name}/pass1_ddl"
        switch = SwitchRunner(self.config, verbose=self.verbose)

        console.print("  Running Switch LLM transpiler on DDL...")
        result = switch.transpile(input_dir, ws_output, custom_prompt="ddl")
        if result.returncode != 0:
            console.print(f"  [red]Switch failed: {result.stderr[:200]}[/red]")
            raise RuntimeError(f"Switch transpile failed: {result.stderr[:500]}")

        # --- Step 3: Download output ---
        downloaded = switch.download_output(ws_output, output_dir)
        console.print(f"  Downloaded {len(downloaded)} file(s) from workspace")

        # --- Step 3b: Fallback if Switch produced empty output ---
        # When the LLM returns nothing, generate DDL deterministically using regex.
        if not downloaded:
            console.print("  [yellow]Switch DDL output empty — using deterministic fallback[/yellow]")
            fallback_path = output_dir / "all_ddl.py"
            content = concat_path.read_text(encoding="utf-8", errors="replace")
            catalog = self.config.catalog
            target_schema = self.config.get_target_schema()
            converted = _deterministic_ddl_convert(content, catalog, target_schema)
            fallback_path.write_text(converted, encoding="utf-8")
            downloaded = [fallback_path]

        # --- Step 4: Post-process outputs ---
        checks_passed = 0
        checks_failed = 0
        for f in downloaded:
            original = f.read_text(encoding="utf-8", errors="replace")
            cleaned = _post_process(
                original,
                catalog=self.config.catalog,
                target_schema=self.config.get_target_schema(),
            )
            f.write_text(cleaned, encoding="utf-8")
            issues = _count_remnants(cleaned)
            if issues == 0:
                checks_passed += 1
            else:
                checks_failed += 1
                console.print(f"  [yellow]Warning: {f.name} still has {issues} legacy remnant(s)[/yellow]")

        metrics = {
            "input_files": len(ddl_files),
            "output_files": len(downloaded),
            "post_checks_passed": checks_passed,
            "post_checks_failed": checks_failed,
        }
        console.print(f"  [green]Pass 1 complete:[/green] {metrics}")
        return metrics


def _post_process(content: str, catalog: str = "", target_schema: str = "") -> str:
    """Remove HDFS locations, STORED AS, ROW FORMAT that the LLM may have missed."""
    # Fix malformed CREATE TABLE where LLM commented out the statement
    content = _repair_commented_create_table(content)
    content = _HDFS_RE.sub("", content)
    content = _STORED_AS_RE.sub("", content)
    content = _ROW_FORMAT_RE.sub("", content)
    content = _KUDU_TBLPROPS_RE.sub("", content)
    content = _PARTITION_HASH_RE.sub("", content)

    # Fix 4-part names: catalog.schema.original_schema.table → catalog.schema.table
    # The LLM sometimes preserves the original schema (e.g., common_layer, datatank_view)
    # as part of the table name, creating invalid 4-part references.
    if catalog and target_schema:
        # Match: catalog.schema.EXTRA_PART.table_name — collapse to catalog.schema.table_name
        content = re.sub(
            rf'({re.escape(catalog)}\.{re.escape(target_schema)})\.\w+\.(\w+)',
            rf'\1.\2',
            content,
        )

    # Ensure all CREATE TABLE use IF NOT EXISTS
    content = re.sub(
        r'CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)',
        'CREATE TABLE IF NOT EXISTS ',
        content,
        flags=re.IGNORECASE,
    )

    # Clean up leftover blank lines
    content = re.sub(r"\n{3,}", "\n\n", content)
    return content


# Pattern: "# Removed: CREATE TABLE ..." or "# Removed CREATE TABLE ..."
# followed by column defs, closing ")", and orphaned triple-quote close.
_COMMENTED_CREATE_RE = re.compile(
    r'^(# Removed:?\s*)(CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+\S+\s*\()'
    r'(.*?)'        # column definitions (may span multiple lines)
    r'^(\))'        # closing paren at start of line
    r'((?:\s*\n.*?PARTITIONED\s+BY\s*\([^)]*\))?)'  # optional PARTITIONED BY
    r'\s*\n\"{3}\)',   # orphaned closing triple-quote + paren
    re.MULTILINE | re.DOTALL | re.IGNORECASE,
)


def _repair_commented_create_table(content: str) -> str:
    r"""Repair CREATE TABLE statements the LLM incorrectly commented out.

    The LLM sometimes generates a ``# Removed:`` comment instead of
    wrapping the CREATE TABLE in ``spark.sql(triple-quote ... triple-quote)``.
    This detects that pattern and restores the proper spark.sql() call.
    """
    tq = '"""'  # triple-quote token

    def _replacer(m: re.Match) -> str:
        create_stmt = m.group(2)   # CREATE TABLE IF NOT EXISTS ...
        columns = m.group(3)       # column definitions
        closing = m.group(4)       # )
        partition = m.group(5) or ""  # optional PARTITIONED BY
        return f'spark.sql({tq}\n{create_stmt}{columns}{closing}{partition}\n{tq})'

    return _COMMENTED_CREATE_RE.sub(_replacer, content)


def _deterministic_ddl_convert(content: str, catalog: str, target_schema: str) -> str:
    """Convert DDL deterministically using regex when Switch LLM fails.

    This is a fallback that handles the most common Hive/Kudu DDL patterns
    without relying on the LLM. It produces valid Databricks SQL.
    """
    lines = content.splitlines()
    output_lines = [
        "# Databricks notebook source",
        "# MAGIC %md",
        "# MAGIC # DDL — Deterministic Conversion (Switch fallback)",
        "",
        "# COMMAND ----------",
        f'spark.sql("USE CATALOG {catalog}")',
        f'spark.sql("USE SCHEMA {target_schema}")',
        "",
    ]

    # Extract CREATE TABLE/VIEW statements and convert them
    current_stmt = []
    in_create = False

    for line in lines:
        stripped = line.strip()
        # Detect start of CREATE statement
        if re.match(r'CREATE\s+(EXTERNAL\s+)?TABLE|CREATE\s+(OR\s+REPLACE\s+)?VIEW', stripped, re.IGNORECASE):
            in_create = True
            current_stmt = [line]
            continue
        if in_create:
            current_stmt.append(line)
            # Detect end of statement
            if stripped.endswith(';') or (stripped == ')' and not any(kw in line.upper() for kw in ['PARTITION', 'STORED', 'ROW'])):
                stmt = "\n".join(current_stmt)
                converted = _convert_single_ddl(stmt, catalog, target_schema)
                if converted:
                    output_lines.append("# COMMAND ----------")
                    output_lines.append(converted)
                    output_lines.append("")
                in_create = False
                current_stmt = []

    # Handle last statement if no trailing semicolon
    if current_stmt:
        stmt = "\n".join(current_stmt)
        converted = _convert_single_ddl(stmt, catalog, target_schema)
        if converted:
            output_lines.append("# COMMAND ----------")
            output_lines.append(converted)

    return "\n".join(output_lines)


def _convert_single_ddl(stmt: str, catalog: str, target_schema: str) -> str:
    """Convert a single DDL statement to Databricks spark.sql()."""
    # Remove storage clauses
    stmt = _HDFS_RE.sub("", stmt)
    stmt = _STORED_AS_RE.sub("", stmt)
    stmt = _ROW_FORMAT_RE.sub("", stmt)
    stmt = _KUDU_TBLPROPS_RE.sub("", stmt)
    stmt = _PARTITION_HASH_RE.sub("", stmt)
    # Remove trailing semicolons
    stmt = re.sub(r';\s*$', '', stmt)
    # Remove NOT NULL / NULL column constraints
    stmt = re.sub(r'\s+NOT\s+NULL', '', stmt, flags=re.IGNORECASE)
    stmt = re.sub(r'\s+NULL(?=\s*[,)])', '', stmt, flags=re.IGNORECASE)
    # Remove PRIMARY KEY
    stmt = re.sub(r',?\s*PRIMARY\s+KEY\s*\([^)]*\)', '', stmt, flags=re.IGNORECASE)
    # Remove ENCODING/COMPRESSION column attributes
    stmt = re.sub(r'\s+ENCODING\s+\w+', '', stmt, flags=re.IGNORECASE)
    stmt = re.sub(r'\s+COMPRESSION\s+\w+', '', stmt, flags=re.IGNORECASE)
    stmt = re.sub(r'\s+DEFAULT\s+\w+', '', stmt, flags=re.IGNORECASE)
    stmt = re.sub(r'\s+BLOCK_SIZE\s+\d+', '', stmt, flags=re.IGNORECASE)

    # Fix table names: strip source schema, use catalog.target_schema.table_name
    # Pattern: CREATE [EXTERNAL] TABLE [IF NOT EXISTS] schema.table_name
    stmt = re.sub(
        r'CREATE\s+EXTERNAL\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)',
        lambda m: f'CREATE TABLE IF NOT EXISTS {catalog}.{target_schema}.{m.group(3)}',
        stmt, flags=re.IGNORECASE,
    )
    stmt = re.sub(
        r'CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)',
        lambda m: f'CREATE TABLE IF NOT EXISTS {catalog}.{target_schema}.{m.group(3)}',
        stmt, flags=re.IGNORECASE,
    )
    stmt = re.sub(
        r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+(\w+)\.(\w+)',
        lambda m: f'CREATE OR REPLACE VIEW {catalog}.{target_schema}.{m.group(3)}',
        stmt, flags=re.IGNORECASE,
    )

    # Fix FROM references in views
    stmt = re.sub(
        r'FROM\s+(\w+)\.(\w+)',
        lambda m: f'FROM {catalog}.{target_schema}.{m.group(2)}',
        stmt, flags=re.IGNORECASE,
    )

    # Clean up empty lines
    stmt = re.sub(r'\n{3,}', '\n\n', stmt)
    stmt = stmt.strip()

    if not stmt:
        return ""

    return f'spark.sql("""\n{stmt}\n""")'


def _count_remnants(content: str) -> int:
    """Count any legacy Hadoop/Kudu remnants still present."""
    count = 0
    for pattern in [_HDFS_RE, _STORED_AS_RE, _ROW_FORMAT_RE, _KUDU_TBLPROPS_RE, _PARTITION_HASH_RE]:
        count += len(pattern.findall(content))
    return count
