"""ETL pattern detection from use-case file contents."""

from __future__ import annotations

from ds2dbx.scanner.folder import UseCaseManifest


def _read_all(paths: list) -> str:
    """Read and concatenate the text of multiple files, ignoring errors."""
    parts: list[str] = []
    for p in paths:
        try:
            parts.append(p.read_text(encoding="utf-8", errors="replace"))
        except OSError:
            continue
    return "\n".join(parts)


def detect_pattern(manifest: UseCaseManifest) -> str:
    """Detect the ETL pattern for a use case based on file contents.

    Returns one of: "scd2", "file_ingestion", "multi_join", "generic".
    """
    shell_text = _read_all(manifest.shell_logic_scripts)

    # SCD Type-2 pattern
    scd2_markers = ("LAST_VRSN_F", "VLD_FM_DT", "VLD_TO_DT")
    if all(marker in shell_text for marker in scd2_markers):
        return "scd2"

    # File ingestion pattern
    ingestion_markers = ("validateHeader", "validateFooter", "HEAD_FOOT")
    if any(marker in shell_text for marker in ingestion_markers):
        return "file_ingestion"

    # Multi-join pattern: many DDL files + JOIN keyword in shell
    shell_upper = shell_text.upper()
    if len(manifest.ddl_files) > 4 and "JOIN" in shell_upper:
        return "multi_join"

    return "generic"
