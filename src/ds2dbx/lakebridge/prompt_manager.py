"""Prompt management for Switch LLM transpilation passes."""

from __future__ import annotations

import importlib.resources
from pathlib import Path
from typing import Optional

import yaml

from ds2dbx.config import Config

# Map pass types to their YAML filenames inside the prompts package.
_PROMPT_FILES = {
    "ddl": "ddl_prompt.yml",
    "shell": "shell_prompt.yml",
    "datastage_fix": "datastage_fix_prompt.yml",
}


class PromptManager:
    """Load and inject conversion prompts for Switch LLM passes."""

    def __init__(self, config: Config):
        self.config = config

    def prepare_input_with_prompt(self, content: str, pass_type: str) -> str:
        """Embed conversion instructions as a commented header.

        The prompt text is prepended as a block of ``#`` comments so the LLM
        sees it as context while the original source remains intact.

        Parameters
        ----------
        content:
            The original source code to be converted.
        pass_type:
            One of ``'ddl'``, ``'shell'``, or ``'datastage_fix'``.

        Returns
        -------
        str
            The content with the prompt header prepended.
        """
        prompt_text = self.get_prompt_text(pass_type)
        if not prompt_text:
            return content

        # Format prompt as comment block
        comment_lines = [f"# {line}" if line.strip() else "#" for line in prompt_text.splitlines()]
        header = "\n".join(comment_lines)
        return f"{header}\n\n{content}"

    def get_prompt_text(self, pass_type: str) -> str:
        """Load prompt YAML and return the ``system_message`` text.

        Resolution order:
        1. Custom path from ``config.prompts.<pass_type>`` (if set).
        2. Built-in YAML from the ``ds2dbx.prompts`` package directory.

        Returns an empty string if the prompt cannot be loaded.
        """
        # Check config for a custom prompt path
        custom_path = self._custom_path_for(pass_type)
        if custom_path and custom_path.exists():
            return self._load_yaml_prompt(custom_path)

        # Fall back to built-in prompts package
        return self._load_builtin_prompt(pass_type)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _custom_path_for(self, pass_type: str) -> Optional[Path]:
        """Return the user-configured custom prompt path, if any."""
        raw = getattr(self.config.prompts, pass_type, None)
        if raw:
            p = Path(raw)
            return p if p.is_absolute() else Path.cwd() / p
        return None

    def _load_yaml_prompt(self, path: Path) -> str:
        """Read a YAML file and extract the ``system_message`` field."""
        try:
            with open(path) as f:
                data = yaml.safe_load(f) or {}
            return data.get("system_message", "")
        except Exception:
            return ""

    def _load_builtin_prompt(self, pass_type: str) -> str:
        """Load a prompt YAML from the ``ds2dbx.prompts`` package."""
        filename = _PROMPT_FILES.get(pass_type)
        if not filename:
            return ""

        try:
            files = importlib.resources.files("ds2dbx.prompts")
            resource = files.joinpath(filename)
            text = resource.read_text(encoding="utf-8")
            data = yaml.safe_load(text) or {}
            return data.get("system_message", "")
        except Exception:
            return ""
