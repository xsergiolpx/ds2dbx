"""Shell script classification — detect SSH wrappers and other skip-worthy scripts."""

from __future__ import annotations

from pathlib import Path

from ds2dbx.config import Config


def is_ssh_wrapper(file: Path, config: Config) -> bool:
    """Detect whether a shell script is an SSH wrapper that should be skipped.

    A script is considered an SSH wrapper if:
      - Its filename matches one of the configured skip_patterns, OR
      - The heuristic is enabled AND the file is short (<20 lines)
        and contains 'ssh '.
    """
    # Check filename against skip patterns
    for pattern in config.shell_scripts.skip_patterns:
        if file.name == pattern:
            return True

    # Heuristic: short file that just wraps an ssh call
    if config.shell_scripts.skip_heuristic:
        try:
            text = file.read_text(encoding="utf-8", errors="replace")
            lines = text.splitlines()
            if len(lines) < 20 and "ssh " in text:
                return True
        except OSError:
            pass

    return False
