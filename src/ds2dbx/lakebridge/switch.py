"""Switch (LLM-based) transpiler wrapper."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import requests
import yaml
from rich.console import Console

from ds2dbx.config import Config
from ds2dbx.utils.subprocess_runner import RunResult, run_command

console = Console()

# Workspace path where Switch reads its config
_SWITCH_CONFIG_WS = (
    "/Workspace/Users/{username}/.lakebridge/switch/resources/switch_config.yml"
)
_SWITCH_PROMPTS_WS = (
    "/Workspace/Users/{username}/.lakebridge/switch/resources/custom_prompts"
)


def _estimate_tokens(char_count: int) -> int:
    """Estimate token count using the ~4 chars/token heuristic for Claude."""
    return max(1, char_count // 4)


def _count_tokens_in_dir(directory: Path) -> tuple[int, int]:
    """Count total chars and estimated tokens for all files in a directory."""
    total_chars = 0
    for f in directory.rglob("*"):
        if f.is_file():
            try:
                total_chars += len(f.read_text(encoding="utf-8", errors="replace"))
            except OSError:
                pass
    return total_chars, _estimate_tokens(total_chars)


def _format_tokens(tokens: int) -> str:
    """Format token count with K/M suffix."""
    if tokens >= 1_000_000:
        return f"{tokens / 1_000_000:.1f}M"
    if tokens >= 1_000:
        return f"{tokens / 1_000:.1f}K"
    return str(tokens)


class SwitchRunner:
    """Run the Switch LLM-based transpiler via the Databricks CLI."""

    def __init__(self, config: Config, verbose: bool = False):
        self.config = config
        self.verbose = verbose
        self.profile = config.databricks.profile

    def transpile(
        self,
        input_dir: Path,
        output_ws_folder: str,
        *,
        custom_prompt: str | None = None,
    ) -> RunResult:
        """Run Switch LLM transpilation and wait for job completion.

        Parameters
        ----------
        input_dir:
            Local directory containing files to transpile.
        output_ws_folder:
            Workspace path for output notebooks.
        custom_prompt:
            Optional pass type ('ddl' or 'shell') to activate the
            corresponding custom prompt YAML on the workspace before
            running Switch.  When set, also increases max_fix_attempts.
        """
        lb = self.config.lakebridge

        # --- Show estimated input tokens ---
        input_chars, input_tokens = _count_tokens_in_dir(input_dir)
        prompt_tokens = 0
        if custom_prompt:
            prompt_tokens = self._get_prompt_tokens(custom_prompt)
        total_input = input_tokens + prompt_tokens
        console.print(
            f"  [bold cyan]LLM input:[/bold cyan] ~{_format_tokens(total_input)} tokens "
            f"(source: {_format_tokens(input_tokens)}, prompt: {_format_tokens(prompt_tokens)})"
        )

        # --- Upload custom prompt & update config if requested ---
        if custom_prompt:
            self._setup_custom_prompt(custom_prompt)

        cmd = [
            "databricks", "labs", "lakebridge", "llm-transpile",
            "--input-source", str(input_dir),
            "--output-ws-folder", output_ws_folder,
            "--source-dialect", "unknown_etl",
            "--catalog-name", lb.switch_catalog,
            "--schema-name", lb.switch_schema,
            "--volume", lb.switch_volume,
            "--foundation-model", lb.foundation_model,
            "--accept-terms", "true",
            "--profile", self.profile,
        ]

        result = run_command(
            cmd,
            verbose=self.verbose,
            description="Switch LLM transpile",
            timeout=60,  # The command itself returns quickly (fire-and-forget)
        )

        # Parse run ID from output (look for runs/NNNN pattern)
        all_output = result.stdout + result.stderr
        run_match = re.search(r"runs/(\d+)", all_output)
        if run_match:
            run_id = run_match.group(1)
            console.print(f"  Switch job triggered (run_id={run_id}), waiting for completion...")
            success = self._wait_for_run(run_id, show_url=True)
            if not success:
                return RunResult(
                    returncode=1,
                    stdout=result.stdout,
                    stderr="Switch job failed or timed out",
                    duration_sec=result.duration_sec,
                    command=result.command,
                )
        else:
            # If we can't find a run ID, the command might have failed
            if result.returncode != 0:
                return result
            # Or it might have completed synchronously
            console.print("  Switch command completed (no async job detected)")

        return result

    # ------------------------------------------------------------------
    # Token estimation
    # ------------------------------------------------------------------

    def _get_prompt_tokens(self, pass_type: str) -> int:
        """Estimate token count for the custom prompt YAML."""
        import importlib.resources

        prompt_file_map = {
            "ddl": "switch_ddl_prompt.yml",
            "shell": "switch_shell_prompt.yml",
        }
        filename = prompt_file_map.get(pass_type)
        if not filename:
            return 0
        try:
            files = importlib.resources.files("ds2dbx.prompts")
            content = files.joinpath(filename).read_text(encoding="utf-8")
            return _estimate_tokens(len(content))
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Custom prompt management
    # ------------------------------------------------------------------

    def _setup_custom_prompt(self, pass_type: str) -> None:
        """Upload the custom prompt YAML and update switch_config.yml."""
        import importlib.resources

        prompt_file_map = {
            "ddl": "switch_ddl_prompt.yml",
            "shell": "switch_shell_prompt.yml",
        }
        filename = prompt_file_map.get(pass_type)
        if not filename:
            console.print(f"  [yellow]No custom prompt for pass type '{pass_type}'[/yellow]")
            return

        # Load prompt content from package
        try:
            files = importlib.resources.files("ds2dbx.prompts")
            prompt_content = files.joinpath(filename).read_text(encoding="utf-8")
        except Exception as exc:
            console.print(f"  [yellow]Could not load prompt {filename}: {exc}[/yellow]")
            return

        username = self.config._get_username()
        prompts_ws = _SWITCH_PROMPTS_WS.format(username=username)
        config_ws = _SWITCH_CONFIG_WS.format(username=username)

        # 1. Create remote directory for custom prompts
        run_command(
            ["databricks", "workspace", "mkdirs", prompts_ws, "--profile", self.profile],
            verbose=self.verbose,
        )

        # 2. Write prompt YAML to a temp local file and upload
        import tempfile
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yml", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(prompt_content)
            tmp_path = tmp.name

        remote_prompt_path = f"{prompts_ws}/{filename}"
        run_command(
            [
                "databricks", "workspace", "import", remote_prompt_path,
                "--file", tmp_path,
                "--format", "AUTO",
                "--profile", self.profile,
                "--overwrite",
            ],
            verbose=self.verbose,
            description=f"Upload custom prompt {filename}",
        )
        Path(tmp_path).unlink(missing_ok=True)

        # 3. Update switch_config.yml with custom prompt path and max_fix_attempts
        lb = self.config.lakebridge
        max_fix = max(lb.max_fix_attempts, 5)

        switch_config = {
            "target_type": "notebook",
            "source_format": "sql" if pass_type == "ddl" else "generic",
            "comment_lang": "English",
            "log_level": "INFO",
            "token_count_threshold": 20000,
            "concurrency": lb.concurrency,
            "max_fix_attempts": max_fix,
            "conversion_prompt_yaml": remote_prompt_path,
            "sdp_language": "python",
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yml", delete=False, encoding="utf-8"
        ) as tmp:
            yaml.dump(switch_config, tmp, default_flow_style=False, sort_keys=False)
            config_tmp = tmp.name

        console.print(
            f"  Updating Switch config: conversion_prompt_yaml={remote_prompt_path}, "
            f"max_fix_attempts={max_fix}, source_format={switch_config['source_format']}"
        )
        run_command(
            [
                "databricks", "workspace", "import", config_ws,
                "--file", config_tmp,
                "--format", "AUTO",
                "--profile", self.profile,
                "--overwrite",
            ],
            verbose=self.verbose,
            description="Update switch_config.yml",
        )
        Path(config_tmp).unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Job polling
    # ------------------------------------------------------------------

    def _wait_for_run(self, run_id: str, timeout_minutes: int = 30, show_url: bool = False) -> bool:
        """Poll a Databricks job run until it completes."""
        host = self.config.get_host()
        token = self.config.get_token()
        if not host or not token:
            console.print("  [yellow]Warning: Could not get auth for polling — assuming success[/yellow]")
            return True

        deadline = time.time() + timeout_minutes * 60
        poll_interval = 15
        url_shown = False

        while time.time() < deadline:
            try:
                resp = requests.get(
                    f"{host}/api/2.1/jobs/runs/get",
                    params={"run_id": run_id},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    state = data.get("state", {})
                    life_cycle = state.get("life_cycle_state", "")
                    result_state = state.get("result_state", "")

                    # Print clickable job URL on first successful poll
                    if show_url and not url_shown:
                        run_url = data.get("run_page_url", "")
                        if run_url:
                            console.print(f"  [bold]Job URL:[/bold] {run_url}")
                        url_shown = True

                    if life_cycle == "TERMINATED":
                        if result_state == "SUCCESS":
                            console.print(f"  Switch job completed successfully")
                            return True
                        else:
                            console.print(f"  [red]Switch job failed: {result_state}[/red]")
                            error_msg = state.get("state_message", "")
                            if error_msg:
                                console.print(f"  [red]{error_msg[:200]}[/red]")
                            return False
                    elif life_cycle in ("INTERNAL_ERROR", "SKIPPED"):
                        console.print(f"  [red]Switch job error: {life_cycle}[/red]")
                        return False

                    if self.verbose:
                        console.print(f"  [dim]Polling... state={life_cycle}[/dim]")
            except Exception as e:
                if self.verbose:
                    console.print(f"  [dim]Poll error: {e}[/dim]")

            time.sleep(poll_interval)

        console.print(f"  [red]Switch job timed out after {timeout_minutes}m[/red]")
        return False

    # ------------------------------------------------------------------
    # Output download
    # ------------------------------------------------------------------

    def download_output(self, ws_folder: str, local_dir: Path) -> list[Path]:
        """Download notebooks from a workspace folder to a local directory."""
        local_dir.mkdir(parents=True, exist_ok=True)

        # List remote files — also check subfolders (Switch sometimes nests output)
        downloaded = self._download_from_folder(ws_folder, local_dir)

        if not downloaded:
            # Try listing subfolders in case Switch put output one level deeper
            list_result = run_command(
                [
                    "databricks", "workspace", "list", ws_folder,
                    "--output", "json",
                    "--profile", self.profile,
                ],
                verbose=self.verbose,
                description="List workspace folder (subfolder check)",
            )
            if list_result.returncode == 0:
                try:
                    entries = json.loads(list_result.stdout)
                    for entry in entries:
                        if entry.get("object_type") == "DIRECTORY":
                            sub_path = entry.get("path", "")
                            sub_downloaded = self._download_from_folder(sub_path, local_dir)
                            downloaded.extend(sub_downloaded)
                except (json.JSONDecodeError, TypeError):
                    pass

        # --- Show estimated output tokens ---
        if downloaded:
            total_chars = sum(
                len(f.read_text(encoding="utf-8", errors="replace"))
                for f in downloaded
            )
            output_tokens = _estimate_tokens(total_chars)
            console.print(
                f"  [bold cyan]LLM output:[/bold cyan] ~{_format_tokens(output_tokens)} tokens "
                f"({len(downloaded)} file(s), {total_chars:,} chars)"
            )

        return downloaded

    def _download_from_folder(self, ws_folder: str, local_dir: Path) -> list[Path]:
        """Download notebook files from a single workspace folder."""
        list_result = run_command(
            [
                "databricks", "workspace", "list", ws_folder,
                "--output", "json",
                "--profile", self.profile,
            ],
            verbose=self.verbose,
            description=f"List {ws_folder}",
        )

        if list_result.returncode != 0:
            return []

        try:
            entries = json.loads(list_result.stdout)
        except (json.JSONDecodeError, TypeError):
            return []

        downloaded: list[Path] = []
        for entry in entries:
            obj_type = entry.get("object_type", "")
            remote_path = entry.get("path", "")
            if not remote_path or obj_type == "DIRECTORY":
                continue

            filename = Path(remote_path).name
            if not filename.endswith(".py"):
                filename = filename + ".py"
            local_path = local_dir / filename

            export_result = run_command(
                [
                    "databricks", "workspace", "export", remote_path,
                    "--file", str(local_path),
                    "--profile", self.profile,
                ],
                verbose=self.verbose,
                description=f"Export {remote_path}",
            )

            if export_result.returncode == 0:
                downloaded.append(local_path)

        return downloaded
