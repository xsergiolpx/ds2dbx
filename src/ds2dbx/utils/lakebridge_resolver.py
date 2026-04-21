"""Resolve the Lakebridge CLI command and validate availability."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.utils.subprocess_runner import run_command

console = Console()


class LakebridgeNotFoundError(RuntimeError):
    """Raised when Lakebridge CLI cannot be found or called."""
    pass


def resolve_databricks_cmd(config: Config) -> str:
    """Return the path to the databricks CLI binary.

    Resolution order:
    1. config.lakebridge.cli_path (explicit user override)
    2. 'databricks' on PATH (default)

    Raises LakebridgeNotFoundError with actionable diagnostics when not found.
    """
    cli_path = config.lakebridge.cli_path

    if cli_path:
        # User specified an explicit path
        p = Path(cli_path)
        if p.is_file():
            return str(p)
        # Maybe they specified a directory containing the binary
        if p.is_dir():
            candidate = p / "databricks"
            if candidate.is_file():
                return str(candidate)
        raise LakebridgeNotFoundError(
            f"Configured cli_path '{cli_path}' does not exist or is not executable.\n"
            f"Check the 'lakebridge.cli_path' setting in your ds2dbx.yml."
        )

    # Default: find 'databricks' on PATH
    found = shutil.which("databricks")
    if found:
        return found

    raise LakebridgeNotFoundError(
        "'databricks' CLI not found on PATH.\n"
        "Install it: https://docs.databricks.com/dev-tools/cli/install.html\n"
        "Or set 'lakebridge.cli_path' in ds2dbx.yml to the full path."
    )


def build_lakebridge_cmd(config: Config, subcommand: str, args: list[str]) -> list[str]:
    """Build a full Lakebridge CLI command list.

    Parameters
    ----------
    config : Config
        ds2dbx configuration (used to resolve cli_path).
    subcommand : str
        Lakebridge subcommand: 'transpile' or 'llm-transpile'.
    args : list[str]
        Additional arguments after the subcommand.

    Returns
    -------
    list[str]
        Full command list ready for subprocess.
    """
    cli = resolve_databricks_cmd(config)
    return [cli, "labs", "lakebridge", subcommand] + args


def check_lakebridge_available(
    config: Config,
    *,
    profile: str = "DEFAULT",
) -> tuple[bool, bool, str]:
    """Check if Lakebridge CLI, BladeBridge, and Switch are available.

    Returns (bladebridge_ok, switch_ok, diagnostic_message).
    """
    try:
        cli = resolve_databricks_cmd(config)
    except LakebridgeNotFoundError as e:
        return False, False, str(e)

    # Check BladeBridge (transpile)
    r1 = run_command([cli, "labs", "lakebridge", "transpile", "--help", "--profile", profile])
    bb_ok = r1.returncode == 0 and "transpile" in r1.stdout.lower()

    # Check Switch (llm-transpile)
    r2 = run_command([cli, "labs", "lakebridge", "llm-transpile", "--help", "--profile", profile])
    sw_ok = r2.returncode == 0

    diag_parts = []
    if not bb_ok:
        stderr_hint = r1.stderr.strip()[:200] if r1.stderr else "no output"
        diag_parts.append(
            f"BladeBridge (transpile) check failed (exit {r1.returncode}): {stderr_hint}\n"
            f"Install: databricks labs lakebridge install-transpile"
        )
    if not sw_ok:
        stderr_hint = r2.stderr.strip()[:200] if r2.stderr else "no output"
        diag_parts.append(
            f"Switch (llm-transpile) check failed (exit {r2.returncode}): {stderr_hint}\n"
            f"Install: databricks labs lakebridge install-transpile"
        )

    if bb_ok and sw_ok:
        msg = f"OK (cli: {cli})"
    else:
        msg = "\n".join(diag_parts)
        if config.lakebridge.cli_path:
            msg += f"\n(Using configured cli_path: {config.lakebridge.cli_path})"
        else:
            msg += f"\n(Resolved CLI: {cli})"

    return bb_ok, sw_ok, msg
