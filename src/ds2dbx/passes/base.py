"""Base class for all conversion passes."""

from __future__ import annotations

from pathlib import Path

from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.scanner.folder import UseCaseManifest
from ds2dbx.utils.status import is_pass_completed, start_pass, complete_pass, fail_pass

console = Console()


class BasePass:
    """Abstract base for a single conversion pass."""

    def __init__(self, config: Config, output_dir: Path, verbose: bool = False):
        self.config = config
        self.output_dir = output_dir
        self.verbose = verbose

    def run(self, manifest: UseCaseManifest, force: bool = False) -> dict:
        """Run the pass. Returns metrics dict. Checks idempotency via status.json."""
        raise NotImplementedError

    @property
    def pass_name(self) -> str:
        raise NotImplementedError
