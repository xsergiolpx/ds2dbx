"""CSV delimiter auto-detection."""

from pathlib import Path


def detect_delimiter(file: Path) -> str:
    """Detect CSV delimiter by inspecting the first few lines."""
    try:
        sample = file.read_bytes()[:4096].decode("utf-8", errors="ignore")
    except Exception:
        return ","

    # SOH (most common in Hadoop/Hive ecosystems)
    if "\x01" in sample:
        return "\u0001"
    # Pipe-delimited
    if "|" in sample and sample.count("|") > sample.count(","):
        return "|"
    # Tab-delimited
    if "\t" in sample and sample.count("\t") > sample.count(","):
        return "\t"
    # Default: comma
    return ","


def delimiter_display_name(delimiter: str) -> str:
    """Human-readable name for a delimiter."""
    return {
        ",": "comma",
        "|": "pipe",
        "\t": "tab",
        "\u0001": "SOH (\\x01)",
    }.get(delimiter, repr(delimiter))
