"""Verify shell script conversion: compare source .sh against converted notebook."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ShellIssue:
    """A verification issue found in shell output."""
    severity: str  # "error" or "warning"
    message: str


# ---------------------------------------------------------------------------
# Source shell parsers
# ---------------------------------------------------------------------------

# impala-shell ... -q "SQL_HERE"
_IMPALA_Q_RE = re.compile(
    r'impala-shell\s+[^"]*-q\s+"((?:[^"\\]|\\.)*)"',
    re.DOTALL,
)

# SQL table references: schema.table (2-part name)
_SQL_TABLE_REF_RE = re.compile(
    r"\b(\w+)\.(\w+)\b",
)

# SQL statement types
_SQL_STMT_PATTERNS = {
    "INSERT": re.compile(r"\bINSERT\s+(?:INTO|OVERWRITE)\b", re.IGNORECASE),
    "DELETE": re.compile(r"\bDELETE\s+FROM\b", re.IGNORECASE),
    "UPDATE": re.compile(r"\bUPDATE\s+\w+", re.IGNORECASE),
    "SELECT": re.compile(r"\bSELECT\b", re.IGNORECASE),
    "MERGE": re.compile(r"\bMERGE\s+INTO\b", re.IGNORECASE),
    "CREATE": re.compile(r"\bCREATE\s+", re.IGNORECASE),
}

# Infrastructure commands that should be removed
_INFRA_PATTERNS = {
    "impala-shell": re.compile(r"\bimpala-shell\b", re.IGNORECASE),
    "kinit": re.compile(r"\bkinit\b", re.IGNORECASE),
    "beeline": re.compile(r"\bbeeline\b", re.IGNORECASE),
    "hdfs dfs": re.compile(r"\bhdfs\s+dfs\b", re.IGNORECASE),
    "ssh": re.compile(r"\bssh\s+", re.IGNORECASE),
}

# Impala/Hive commands that should be removed
_REMOVABLE_SQL = {
    "INVALIDATE METADATA": re.compile(r"\bINVALIDATE\s+METADATA\b", re.IGNORECASE),
    "REFRESH": re.compile(r"\bREFRESH\s+\w+", re.IGNORECASE),
    "COMPUTE STATS": re.compile(r"\bCOMPUTE\s+(?:INCREMENTAL\s+)?STATS\b", re.IGNORECASE),
    "SET SYNC_DDL": re.compile(r"\bSET\s+SYNC_DDL\b", re.IGNORECASE),
}


def _extract_sql_from_shell(content: str) -> list[str]:
    """Extract SQL statements from impala-shell -q calls."""
    stmts = []
    for m in _IMPALA_Q_RE.finditer(content):
        sql = m.group(1).strip()
        if sql:
            stmts.append(sql)
    return stmts


def _extract_sql_from_notebook(content: str) -> list[str]:
    """Extract SQL from spark.sql() calls in a notebook."""
    stmts = []
    for m in re.finditer(r'spark\.sql\(\s*(?:f?\s*)?"{3}(.*?)"{3}\s*\)', content, re.DOTALL):
        sql = m.group(1).strip()
        if sql:
            stmts.append(sql)
    # Also match single-line spark.sql("...")
    for m in re.finditer(r'spark\.sql\(\s*(?:f?\s*)?"([^"]+)"\s*\)', content):
        sql = m.group(1).strip()
        if sql and not sql.startswith("USE "):
            stmts.append(sql)
    return stmts


def _classify_sql(sql: str) -> str | None:
    """Classify a SQL statement type. Returns None for removable stmts."""
    for name, pattern in _REMOVABLE_SQL.items():
        if pattern.search(sql):
            return None  # Should be removed in conversion
    for name, pattern in _SQL_STMT_PATTERNS.items():
        if pattern.search(sql):
            return name
    return "OTHER"


def _extract_table_refs(sql: str) -> set[str]:
    """Extract table references (schema.table) from SQL."""
    refs = set()
    # Skip common false positives
    skip = {"row_number", "date_trunc", "current_timestamp", "current_date"}
    for m in _SQL_TABLE_REF_RE.finditer(sql):
        schema, table = m.group(1).lower(), m.group(2).lower()
        if schema not in skip and table not in skip:
            refs.add(f"{schema}.{table}")
    return refs


def verify_shell(source_file: Path, output_file: Path) -> list[ShellIssue]:
    """Compare a source shell script against its converted notebook.

    Returns a list of issues found (empty = all good).
    """
    issues: list[ShellIssue] = []

    src_content = source_file.read_text(encoding="utf-8", errors="replace")
    if not output_file.exists():
        issues.append(ShellIssue("error", f"Output file not found: {output_file}"))
        return issues
    out_content = output_file.read_text(encoding="utf-8", errors="replace")

    # --- Check 1: Infrastructure commands removed ---
    for name, pattern in _INFRA_PATTERNS.items():
        if pattern.search(out_content):
            # Check if it's in a comment
            for line in out_content.splitlines():
                stripped = line.lstrip()
                if stripped.startswith("#") or stripped.startswith("//"):
                    continue
                if pattern.search(line):
                    issues.append(ShellIssue(
                        "error",
                        f"Infrastructure command '{name}' still present in output code",
                    ))
                    break

    # --- Check 2: SQL statement coverage ---
    src_stmts = _extract_sql_from_shell(src_content)
    out_stmts = _extract_sql_from_notebook(out_content)

    # Count business SQL in source (excluding removable)
    src_business = []
    for sql in src_stmts:
        cls = _classify_sql(sql)
        if cls is not None:  # Not a removable statement
            src_business.append((cls, sql))

    # Count SQL in output
    out_business = []
    for sql in out_stmts:
        cls = _classify_sql(sql)
        if cls is not None:
            out_business.append((cls, sql))

    # Compare statement type counts
    src_type_counts: dict[str, int] = {}
    for cls, _ in src_business:
        src_type_counts[cls] = src_type_counts.get(cls, 0) + 1

    out_type_counts: dict[str, int] = {}
    for cls, _ in out_business:
        out_type_counts[cls] = out_type_counts.get(cls, 0) + 1

    # UPDATEs in source may become MERGEs in output — combine them
    src_dml = src_type_counts.get("INSERT", 0) + src_type_counts.get("DELETE", 0) + \
              src_type_counts.get("UPDATE", 0)
    out_dml = out_type_counts.get("INSERT", 0) + out_type_counts.get("DELETE", 0) + \
              out_type_counts.get("UPDATE", 0) + out_type_counts.get("MERGE", 0)

    if out_dml < src_dml:
        issues.append(ShellIssue(
            "error",
            f"DML statement count mismatch: source has {src_dml} "
            f"(INSERT/DELETE/UPDATE), output has {out_dml} "
            f"(INSERT/DELETE/UPDATE/MERGE)",
        ))
    elif out_dml == 0 and src_dml == 0:
        pass  # No DML in either — OK
    elif out_dml > 0:
        pass  # Output has at least as many DML — OK

    # --- Check 3: Table references preserved ---
    src_tables = set()
    for _, sql in src_business:
        src_tables |= _extract_table_refs(sql)

    # Source tables use 2-part names; output uses 3-part. Extract the table part.
    src_table_names = {ref.split(".")[-1] for ref in src_tables}

    out_tables = set()
    for _, sql in out_business:
        out_tables |= _extract_table_refs(sql)
    out_table_names = {ref.split(".")[-1] for ref in out_tables}

    missing_tables = src_table_names - out_table_names
    if missing_tables:
        issues.append(ShellIssue(
            "warning",
            f"Table references in source but not in output: "
            f"{', '.join(sorted(missing_tables))}",
        ))

    # --- Check 4: Databricks API usage present ---
    # Converted notebooks should use spark.sql() for SQL logic, or dbutils for
    # file validation scripts (e.g., validate_UNIX_PIPE_COUNT_HEAD_FOOT.ksh).
    has_spark_sql = "spark.sql" in out_content
    has_dbutils = "dbutils." in out_content
    has_spark_read = "spark.read" in out_content
    if not (has_spark_sql or has_dbutils or has_spark_read):
        issues.append(ShellIssue(
            "error",
            "Output notebook contains no spark.sql(), dbutils, or spark.read calls",
        ))

    # --- Check 5: Widget parameters for shell variables ---
    # Count shell positional params used
    src_params = set(re.findall(r"\$(\d+)", src_content))
    src_named_params = set(re.findall(r"\$\{?(\w+)\}?", src_content))
    # Remove common infra vars
    infra_vars = {"IMPALAHOST", "PORT", "KEYTAB", "SERVICE", "CONFILE",
                  "HADOOP_HOME", "HIVE_HOME", "?", "0"}
    src_named_params -= infra_vars

    if src_params and "dbutils.widgets" not in out_content:
        issues.append(ShellIssue(
            "warning",
            f"Source uses positional params ({', '.join(sorted(src_params))}) "
            f"but output has no dbutils.widgets",
        ))

    # --- Check 6: Removable SQL not carried over ---
    for name, pattern in _REMOVABLE_SQL.items():
        for _, sql in out_business:
            if pattern.search(sql):
                issues.append(ShellIssue(
                    "warning",
                    f"Removable SQL '{name}' still present in output spark.sql()",
                ))
                break

    return issues
