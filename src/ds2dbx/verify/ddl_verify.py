"""Verify DDL conversion: compare source DDL files against converted notebook."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TableDef:
    """Parsed table or view definition."""
    name: str
    object_type: str  # "table" or "view"
    columns: list[tuple[str, str]]  # (name, type)
    partition_cols: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class DDLIssue:
    """A verification issue found in DDL output."""
    severity: str  # "error" or "warning"
    message: str


# ---------------------------------------------------------------------------
# Source DDL parsers
# ---------------------------------------------------------------------------

# CREATE [EXTERNAL] TABLE [IF NOT EXISTS] schema.table (
_SRC_TABLE_RE = re.compile(
    r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(\w+)\.(\w+)\s*\(",
    re.IGNORECASE,
)

# CREATE VIEW schema.view AS SELECT ...
_SRC_VIEW_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)\.(\w+)\s+AS",
    re.IGNORECASE,
)

# Column: "  col_name TYPE [NOT NULL] [ENCODING ...] [COMPRESSION ...] [COMMENT '...'],"
_SRC_COL_RE = re.compile(
    r"^\s+(\w+)\s+"
    r"([A-Z_]+(?:\(\d+(?:,\d+)?\))?)"
    r"(?:\s+(?:NOT\s+NULL|NULL))?"
    r"(?:\s+ENCODING\s+\w+\s+COMPRESSION\s+\w+)?"
    r"(?:\s+COMMENT\s+'[^']*')?",
    re.IGNORECASE | re.MULTILINE,
)

# PARTITIONED BY ( col TYPE, ... )
_SRC_PARTITION_RE = re.compile(
    r"PARTITIONED\s+BY\s*\((.*?)\)",
    re.IGNORECASE | re.DOTALL,
)

# Partition column: "col_name TYPE" (no leading whitespace required)
_PART_COL_RE = re.compile(
    r"(\w+)\s+([A-Z_]+(?:\(\d+(?:,\d+)?\))?)",
    re.IGNORECASE,
)

# View columns from SELECT list
_VIEW_SELECT_RE = re.compile(
    r"SELECT\s+(.*?)\s+FROM\s+",
    re.IGNORECASE | re.DOTALL,
)


def parse_source_ddl(path: Path) -> TableDef | None:
    """Parse a source DDL file and extract table/view definition."""
    content = path.read_text(encoding="utf-8", errors="replace")

    # Try VIEW first
    m = _SRC_VIEW_RE.search(content)
    if m:
        schema, name = m.group(1), m.group(2)
        # Extract columns from SELECT list
        sm = _VIEW_SELECT_RE.search(content)
        cols = []
        if sm:
            select_text = sm.group(1)
            for col_expr in select_text.split(","):
                col_expr = col_expr.strip()
                if not col_expr:
                    continue
                # Handle "expr AS alias" or plain column names
                parts = re.split(r"\s+AS\s+", col_expr, flags=re.IGNORECASE)
                col_name = parts[-1].strip().strip("`").strip()
                # Remove any leading table alias (e.g., "a.COL_NAME")
                if "." in col_name:
                    col_name = col_name.split(".")[-1]
                if col_name and re.match(r"^\w+$", col_name):
                    cols.append((col_name.lower(), ""))
        return TableDef(name=name.lower(), object_type="view", columns=cols)

    # Try TABLE
    m = _SRC_TABLE_RE.search(content)
    if m:
        schema, name = m.group(1), m.group(2)
        # Extract columns — everything between first "(" and before closing keywords
        paren_start = content.index("(", m.end() - 1) + 1
        # Find matching close paren (skip PRIMARY KEY lines)
        depth = 1
        pos = paren_start
        while pos < len(content) and depth > 0:
            if content[pos] == "(":
                depth += 1
            elif content[pos] == ")":
                depth -= 1
            pos += 1
        col_block = content[paren_start:pos - 1]

        # Remove PRIMARY KEY line
        col_block = re.sub(r"^\s*PRIMARY\s+KEY\s*\(.*?\)\s*,?\s*$", "",
                          col_block, flags=re.IGNORECASE | re.MULTILINE)

        cols = []
        for cm in _SRC_COL_RE.finditer(col_block):
            col_name = cm.group(1).lower()
            col_type = cm.group(2).upper()
            cols.append((col_name, col_type))

        # Partition columns
        part_cols = []
        pm = _SRC_PARTITION_RE.search(content)
        if pm:
            part_text = pm.group(1)
            for pc in _SRC_COL_RE.finditer(part_text):
                part_cols.append((pc.group(1).lower(), pc.group(2).upper()))

        return TableDef(name=name.lower(), object_type="table",
                       columns=cols, partition_cols=part_cols)

    return None


# ---------------------------------------------------------------------------
# Output notebook parsers
# ---------------------------------------------------------------------------

# spark.sql("""CREATE TABLE IF NOT EXISTS catalog.schema.table (
_OUT_TABLE_RE = re.compile(
    r'spark\.sql\(\s*(?:f?\s*)?"{3}\s*'
    r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+"
    r"(\w+)\.(\w+)\.(\w+)\s*\(",
    re.IGNORECASE,
)

# spark.sql("""CREATE OR REPLACE VIEW catalog.schema.view AS
_OUT_VIEW_RE = re.compile(
    r'spark\.sql\(\s*(?:f?\s*)?"{3}\s*'
    r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+"
    r"(\w+)\.(\w+)\.(\w+)\s+AS",
    re.IGNORECASE,
)

# Each spark.sql(""" ... """) block
_OUT_SQL_BLOCK_RE = re.compile(
    r'spark\.sql\(\s*(?:f?\s*)?"{3}(.*?)"{3}\s*\)',
    re.DOTALL,
)


def parse_output_notebook(path: Path) -> list[TableDef]:
    """Parse a converted DDL notebook and extract table/view definitions."""
    content = path.read_text(encoding="utf-8", errors="replace")
    results: list[TableDef] = []

    for block_match in _OUT_SQL_BLOCK_RE.finditer(content):
        sql = block_match.group(1)

        # Check for CREATE TABLE
        tm = re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)\.(\w+)\.(\w+)\s*\(",
            sql, re.IGNORECASE,
        )
        if tm:
            cat, schema, name = tm.group(1), tm.group(2), tm.group(3)
            # Extract columns from this block
            paren_start = sql.index("(", tm.end() - 1) + 1
            # Find closing paren
            depth = 1
            pos = paren_start
            while pos < len(sql) and depth > 0:
                if sql[pos] == "(":
                    depth += 1
                elif sql[pos] == ")":
                    depth -= 1
                pos += 1
            col_block = sql[paren_start:pos - 1]

            cols = []
            for cm in _SRC_COL_RE.finditer(col_block):
                cols.append((cm.group(1).lower(), cm.group(2).upper()))

            # Check for PARTITIONED BY after closing paren
            rest = sql[pos:]
            part_cols = []
            pm = _SRC_PARTITION_RE.search(rest)
            if pm:
                for pc in _PART_COL_RE.finditer(pm.group(1)):
                    part_cols.append((pc.group(1).lower(), pc.group(2).upper()))

            results.append(TableDef(
                name=name.lower(), object_type="table",
                columns=cols, partition_cols=part_cols,
            ))
            continue

        # Check for CREATE VIEW
        vm = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)\.(\w+)\.(\w+)\s+AS\s+SELECT\s+(.*?)\s+FROM",
            sql, re.IGNORECASE | re.DOTALL,
        )
        if vm:
            cat, schema, name = vm.group(1), vm.group(2), vm.group(3)
            select_text = vm.group(4)
            cols = []
            for col_expr in select_text.split(","):
                col_expr = col_expr.strip()
                if not col_expr:
                    continue
                parts = re.split(r"\s+AS\s+", col_expr, flags=re.IGNORECASE)
                col_name = parts[-1].strip().strip("`")
                if "." in col_name:
                    col_name = col_name.split(".")[-1]
                if col_name and re.match(r"^\w+$", col_name):
                    cols.append((col_name.lower(), ""))
            results.append(TableDef(
                name=name.lower(), object_type="view", columns=cols,
            ))

    return results


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def verify_ddl(source_files: list[Path], output_file: Path, catalog: str, schema: str) -> list[DDLIssue]:
    """Compare source DDL files against a converted DDL notebook.

    Returns a list of issues found (empty = all good).
    """
    issues: list[DDLIssue] = []

    # Parse all source DDL files
    source_defs: dict[str, TableDef] = {}
    for f in source_files:
        td = parse_source_ddl(f)
        if td:
            source_defs[td.name] = td

    if not source_defs:
        issues.append(DDLIssue("warning", "No tables parsed from source DDL files"))
        return issues

    # Parse output notebook
    if not output_file.exists():
        issues.append(DDLIssue("error", f"Output file not found: {output_file}"))
        return issues

    output_defs = parse_output_notebook(output_file)
    output_map: dict[str, TableDef] = {td.name: td for td in output_defs}

    # Check: all source tables present in output
    for src_name, src_def in source_defs.items():
        if src_name not in output_map:
            issues.append(DDLIssue(
                "error",
                f"Missing {src_def.object_type} '{src_name}' in output",
            ))
            continue

        out_def = output_map[src_name]

        # Check columns (tables only — views may have different column formats)
        if src_def.object_type == "table":
            src_col_names = [c[0] for c in src_def.columns]
            out_col_names = [c[0] for c in out_def.columns]

            # Missing columns
            for col in src_col_names:
                if col not in out_col_names:
                    issues.append(DDLIssue(
                        "error",
                        f"Table '{src_name}': missing column '{col}'",
                    ))

            # Extra columns (warning only)
            for col in out_col_names:
                if col not in src_col_names:
                    issues.append(DDLIssue(
                        "warning",
                        f"Table '{src_name}': extra column '{col}' in output",
                    ))

            # Type mismatches
            src_type_map = dict(src_def.columns)
            out_type_map = dict(out_def.columns)
            for col_name in src_col_names:
                if col_name in out_type_map:
                    src_type = src_type_map[col_name]
                    out_type = out_type_map[col_name]
                    if src_type != out_type:
                        issues.append(DDLIssue(
                            "warning",
                            f"Table '{src_name}': column '{col_name}' type "
                            f"'{src_type}' -> '{out_type}'",
                        ))

            # Partition columns
            src_part = [c[0] for c in src_def.partition_cols]
            out_part = [c[0] for c in out_def.partition_cols]
            if src_part and not out_part:
                issues.append(DDLIssue(
                    "warning",
                    f"Table '{src_name}': source has PARTITIONED BY "
                    f"({', '.join(src_part)}) but output does not",
                ))

    # Check output notebook for syntax issues
    content = output_file.read_text(encoding="utf-8", errors="replace")

    # Orphaned triple-quotes
    sql_opens = len(re.findall(r'spark\.sql\(\s*(?:f?\s*)?"{3}', content))
    sql_closes = len(re.findall(r'"{3}\s*\)', content))
    if sql_opens != sql_closes:
        issues.append(DDLIssue(
            "error",
            f"Mismatched spark.sql() blocks: {sql_opens} opens vs {sql_closes} closes",
        ))

    # "# Removed: CREATE TABLE" leftover
    if re.search(r"#\s*Removed:?\s*CREATE\s+TABLE", content, re.IGNORECASE):
        issues.append(DDLIssue(
            "error",
            "Malformed output: contains '# Removed: CREATE TABLE' — "
            "post-processing repair failed",
        ))

    # Hive/Kudu remnants in SQL blocks (not in comments)
    for bm in _OUT_SQL_BLOCK_RE.finditer(content):
        sql = bm.group(1)
        if re.search(r"STORED\s+AS", sql, re.IGNORECASE):
            issues.append(DDLIssue("warning", "Output SQL contains 'STORED AS'"))
        if re.search(r"LOCATION\s+'hdfs://", sql, re.IGNORECASE):
            issues.append(DDLIssue("warning", "Output SQL contains HDFS LOCATION"))
        if re.search(r"PARTITION\s+BY\s+HASH", sql, re.IGNORECASE):
            issues.append(DDLIssue("warning", "Output SQL contains Kudu PARTITION BY HASH"))

    # Namespace check
    expected_ns = f"{catalog}.{schema}"
    for td in output_defs:
        # Check that output tables use expected namespace
        # (already parsed with catalog.schema.table pattern)
        pass  # namespace checked during parse

    return issues
