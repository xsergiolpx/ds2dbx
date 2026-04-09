"""Scanner module — discover and classify use-case directories."""

from ds2dbx.scanner.folder import UseCaseManifest, discover_usecases, scan_usecase
from ds2dbx.scanner.pattern import detect_pattern
from ds2dbx.scanner.shell_classifier import is_ssh_wrapper

__all__ = [
    "UseCaseManifest",
    "discover_usecases",
    "scan_usecase",
    "detect_pattern",
    "is_ssh_wrapper",
]
