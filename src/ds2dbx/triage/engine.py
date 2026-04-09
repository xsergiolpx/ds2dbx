"""Triage engine - scan notebooks for known BladeBridge bug patterns."""

from __future__ import annotations

import importlib.resources
import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

import yaml


@dataclass
class TriageResult:
    """Result of triaging a single file."""

    filename: str
    classification: str  # "clean" or "broken"
    issues: list[dict] = field(default_factory=list)
    # Each issue: {name, severity, description, line_number}


def load_bugs() -> list[dict]:
    """Load bug patterns from bugs.yml bundled with this package."""
    bugs_ref = importlib.resources.files("ds2dbx.triage").joinpath("bugs.yml")
    with importlib.resources.as_file(bugs_ref) as bugs_path:
        with open(bugs_path, "r") as f:
            data = yaml.safe_load(f)
    return data["bugs"]


def _compile_pattern(bug: dict) -> re.Pattern:
    """Compile a bug pattern into a regex, respecting multiline flag."""
    flags = re.MULTILINE
    if bug.get("multiline"):
        flags |= re.DOTALL
    return re.compile(bug["pattern"], flags)


def triage_file(file: Path, bugs: list[dict]) -> TriageResult:
    """Scan a single file against all bug patterns and return a TriageResult."""
    content = file.read_text(encoding="utf-8", errors="replace")
    lines = content.splitlines()
    issues: list[dict] = []

    for bug in bugs:
        compiled = _compile_pattern(bug)

        if bug.get("multiline"):
            # For multiline patterns, search the whole content and map match
            # positions back to line numbers.
            for match in compiled.finditer(content):
                line_number = content[:match.start()].count("\n") + 1
                issues.append(
                    {
                        "name": bug["name"],
                        "severity": bug["severity"],
                        "description": bug["description"],
                        "line_number": line_number,
                    }
                )
        else:
            # Line-by-line search for single-line patterns.
            for line_number, line in enumerate(lines, start=1):
                if compiled.search(line):
                    issues.append(
                        {
                            "name": bug["name"],
                            "severity": bug["severity"],
                            "description": bug["description"],
                            "line_number": line_number,
                        }
                    )

    classification = "broken" if issues else "clean"
    return TriageResult(
        filename=str(file),
        classification=classification,
        issues=issues,
    )


def triage_directory(
    directory: Path,
) -> tuple[list[Path], list[Path], list[TriageResult]]:
    """Triage all .py files in a directory.

    Returns:
        (clean_files, broken_files, all_results)
    """
    bugs = load_bugs()
    clean_files: list[Path] = []
    broken_files: list[Path] = []
    all_results: list[TriageResult] = []

    py_files = sorted(directory.rglob("*.py"))

    for py_file in py_files:
        result = triage_file(py_file, bugs)
        all_results.append(result)
        if result.classification == "clean":
            clean_files.append(py_file)
        else:
            broken_files.append(py_file)

    return clean_files, broken_files, all_results


def triage_notebooks(
    directory: Path,
    output_path: Path | None = None,
) -> tuple[list[Path], list[Path], list[TriageResult]]:
    """High-level entry point: triage a directory and optionally save report.

    Returns:
        (clean_files, broken_files, all_results)
    """
    clean_files, broken_files, all_results = triage_directory(directory)

    if output_path is not None:
        save_triage_report(all_results, output_path)

    return clean_files, broken_files, all_results


def save_triage_report(results: list[TriageResult], output_path: Path) -> None:
    """Write triage results to a JSON report file."""
    report = {
        "summary": {
            "total_files": len(results),
            "clean_files": sum(1 for r in results if r.classification == "clean"),
            "broken_files": sum(1 for r in results if r.classification == "broken"),
            "total_issues": sum(len(r.issues) for r in results),
            "critical_issues": sum(
                1
                for r in results
                for i in r.issues
                if i["severity"] == "critical"
            ),
            "high_issues": sum(
                1
                for r in results
                for i in r.issues
                if i["severity"] == "high"
            ),
        },
        "results": [asdict(r) for r in results],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
