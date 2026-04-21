"""Shell command execution with logging."""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from typing import Optional

from rich.console import Console

console = Console()


@dataclass
class RunResult:
    returncode: int
    stdout: str
    stderr: str
    duration_sec: float
    command: str


def run_command(
    cmd: list[str],
    *,
    cwd: Optional[str] = None,
    timeout: int = 7200,
    verbose: bool = False,
    capture: bool = True,
    description: str = "",
) -> RunResult:
    """Run a shell command and return the result."""
    cmd_str = " ".join(cmd)
    if verbose:
        console.print(f"  [dim]$ {cmd_str}[/dim]")

    # Skip GitHub update check for databricks labs commands (avoids 403 rate limiting)
    env = {**os.environ, "DATABRICKS_LABS_SKIP_UPDATE_CHECK": "true"}

    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=capture,
            text=True,
            timeout=timeout,
            env=env,
        )
        duration = time.time() - start

        if verbose and result.stdout:
            for line in result.stdout.splitlines()[-20:]:
                console.print(f"    [dim]{line}[/dim]")

        if result.returncode != 0 and result.stderr:
            if verbose:
                for line in result.stderr.splitlines()[-10:]:
                    console.print(f"    [red]{line}[/red]")

        return RunResult(
            returncode=result.returncode,
            stdout=result.stdout or "",
            stderr=result.stderr or "",
            duration_sec=duration,
            command=cmd_str,
        )

    except subprocess.TimeoutExpired:
        duration = time.time() - start
        return RunResult(
            returncode=-1,
            stdout="",
            stderr=f"Command timed out after {timeout}s",
            duration_sec=duration,
            command=cmd_str,
        )
    except FileNotFoundError:
        return RunResult(
            returncode=-1,
            stdout="",
            stderr=f"Command not found: {cmd[0]}",
            duration_sec=0,
            command=cmd_str,
        )
