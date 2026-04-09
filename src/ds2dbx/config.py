"""Configuration loading and management."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class DatabricksConfig:
    profile: str = "DEFAULT"
    host: str = ""


@dataclass
class WorkspaceConfig:
    base_path: str = "/Workspace/Users/{username}/ds2dbx_output"


@dataclass
class LakebridgeConfig:
    switch_catalog: str = "migration_pilot"
    switch_schema: str = "lakebridge"
    switch_volume: str = "switch_volume"
    data_volume: str = "sample_data"
    foundation_model: str = "databricks-claude-opus-4-6"
    target_technology: str = "PYSPARK"
    concurrency: int = 4
    max_fix_attempts: int = 5


@dataclass
class PromptsConfig:
    strategy: str = "custom"  # "custom" or "inline"
    ddl: Optional[str] = None
    shell: Optional[str] = None
    datastage_fix: Optional[str] = None


@dataclass
class ShellScriptsConfig:
    skip_patterns: list[str] = field(
        default_factory=lambda: ["Insert_data.sh", "Insert_data_common.sh"]
    )
    skip_heuristic: bool = True


@dataclass
class DataLoadingConfig:
    default_delimiter: str = "auto"
    infer_schema: bool = True
    encoding: str = "utf-8"


@dataclass
class Config:
    databricks: DatabricksConfig = field(default_factory=DatabricksConfig)
    catalog: str = "migration_pilot"
    schema: str = "converted"
    workspace: WorkspaceConfig = field(default_factory=WorkspaceConfig)
    lakebridge: LakebridgeConfig = field(default_factory=LakebridgeConfig)
    prompts: PromptsConfig = field(default_factory=PromptsConfig)
    shell_scripts: ShellScriptsConfig = field(default_factory=ShellScriptsConfig)
    data_loading: DataLoadingConfig = field(default_factory=DataLoadingConfig)

    def get_workspace_base(self) -> str:
        username = self._get_username()
        return self.workspace.base_path.format(username=username)

    def _get_username(self) -> str:
        try:
            result = subprocess.run(
                ["databricks", "current-user", "me", "--profile", self.databricks.profile],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                return data.get("userName", "unknown")
        except Exception:
            pass
        return os.environ.get("USER", "unknown")

    def _get_auth_env(self) -> dict:
        """Parse databricks auth env output (JSON format)."""
        try:
            result = subprocess.run(
                ["databricks", "auth", "env", "--profile", self.databricks.profile],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                return data.get("env", data)  # Handle both nested and flat format
        except Exception:
            pass
        return {}

    def get_host(self) -> str:
        if self.databricks.host:
            return self.databricks.host
        return self._get_auth_env().get("DATABRICKS_HOST", "")

    def get_token(self) -> str:
        return self._get_auth_env().get("DATABRICKS_TOKEN", "")


def _dict_to_dataclass(cls, data: dict):
    """Recursively convert a dict into nested dataclasses."""
    if not isinstance(data, dict):
        return data
    field_types = {f.name: f.type for f in cls.__dataclass_fields__.values()} if hasattr(cls, '__dataclass_fields__') else {}
    kwargs = {}
    for key, val in data.items():
        if key in field_types and isinstance(val, dict):
            # Resolve string type annotations
            ft = field_types[key]
            if isinstance(ft, str):
                ft = globals().get(ft) or locals().get(ft)
            if ft and hasattr(ft, '__dataclass_fields__'):
                val = _dict_to_dataclass(ft, val)
        kwargs[key] = val
    return cls(**kwargs)


TYPE_MAP = {
    "databricks": DatabricksConfig,
    "workspace": WorkspaceConfig,
    "lakebridge": LakebridgeConfig,
    "prompts": PromptsConfig,
    "shell_scripts": ShellScriptsConfig,
    "data_loading": DataLoadingConfig,
}


def load_config(
    config_path: Optional[Path] = None,
    cli_overrides: Optional[dict] = None,
) -> Config:
    """Load config from YAML file with CLI overrides."""
    # Find config file
    search_paths = []
    if config_path:
        search_paths.append(config_path)
    search_paths.extend([
        Path.cwd() / "ds2dbx.yml",
        Path.home() / ".ds2dbx" / "config.yml",
    ])

    raw = {}
    for path in search_paths:
        if path.exists():
            with open(path) as f:
                raw = yaml.safe_load(f) or {}
            break

    # Build config from raw dict
    config = Config()
    for key, val in raw.items():
        if key in TYPE_MAP and isinstance(val, dict):
            setattr(config, key, _dict_to_dataclass(TYPE_MAP[key], val))
        elif hasattr(config, key):
            setattr(config, key, val)

    # Apply CLI overrides
    if cli_overrides:
        for key, val in cli_overrides.items():
            if val is not None and hasattr(config, key):
                setattr(config, key, val)

    return config


def save_config(config: Config, path: Path):
    """Save config to a clean YAML file (omits None values and empty dicts)."""
    from dataclasses import asdict

    def _clean(d: dict) -> dict:
        """Remove None values, empty strings, and empty sub-dicts."""
        out = {}
        for k, v in d.items():
            if v is None or v == "":
                continue
            if isinstance(v, dict):
                v = _clean(v)
                if not v:
                    continue
            out[k] = v
        return out

    data = _clean(asdict(config))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
