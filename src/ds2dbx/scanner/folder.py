"""Folder structure detection — scan a use-case directory and classify its files."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ds2dbx.config import Config
from ds2dbx.scanner.shell_classifier import is_ssh_wrapper


@dataclass
class UseCaseManifest:
    """Summary of all relevant files discovered in a use-case directory."""

    name: str
    path: Path
    pattern: str = "generic"
    ddl_files: list[Path] = field(default_factory=list)
    data_files: list[Path] = field(default_factory=list)
    source_files: list[Path] = field(default_factory=list)
    datastage_files: list[Path] = field(default_factory=list)
    shell_logic_scripts: list[Path] = field(default_factory=list)
    shell_skip_scripts: list[Path] = field(default_factory=list)


def _sorted_files(directory: Path, suffixes: set[str] | None = None) -> list[Path]:
    """Return sorted list of files in *directory*, optionally filtered by suffix."""
    if not directory.is_dir():
        return []
    files = [f for f in directory.iterdir() if f.is_file()]
    if suffixes:
        files = [f for f in files if f.suffix.lower() in suffixes]
    return sorted(files)


def scan_usecase(path: Path, config: Config) -> UseCaseManifest:
    """Scan a use-case directory and return a manifest of classified files.

    Expected sub-folders: Shell/, DDL/, Datastage/, Data/, source/.
    Shell scripts are split into logic scripts and skip scripts based on
    the SSH-wrapper classifier.
    """
    manifest = UseCaseManifest(name=path.name, path=path)

    # DDL files
    manifest.ddl_files = _sorted_files(path / "DDL", {".sql", ".ddl"})

    # Data files (any type)
    data_dir = path / "Data"
    if data_dir.is_dir():
        manifest.data_files = sorted(f for f in data_dir.iterdir() if f.is_file())

    # Source files
    source_dir = path / "source"
    if source_dir.is_dir():
        manifest.source_files = sorted(f for f in source_dir.iterdir() if f.is_file())

    # DataStage exports
    manifest.datastage_files = _sorted_files(path / "Datastage", {".dsx", ".xml", ".json"})

    # Shell scripts — classify into logic vs skip
    shell_dir = path / "Shell"
    if shell_dir.is_dir():
        for f in sorted(shell_dir.iterdir()):
            if not f.is_file():
                continue
            if f.suffix.lower() not in (".sh", ".ksh", ".bash", ""):
                continue
            if is_ssh_wrapper(f, config):
                manifest.shell_skip_scripts.append(f)
            else:
                manifest.shell_logic_scripts.append(f)

    return manifest


def discover_usecases(path: Path) -> list[Path]:
    """Find all use-case subdirectories under *path*.

    Looks for directories whose name starts with ``UC`` or contains
    ``use`` (case-insensitive).  Returns a sorted list of paths.
    """
    if not path.is_dir():
        return []
    results: list[Path] = []
    for child in path.iterdir():
        if not child.is_dir():
            continue
        name = child.name
        if name.startswith("UC") or "use" in name.lower():
            results.append(child)
    return sorted(results)
