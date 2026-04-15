"""ds2dbx CLI — DataStage to Databricks migration tool."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from ds2dbx import __version__
from ds2dbx.config import Config, load_config, save_config

app = typer.Typer(
    name="ds2dbx",
    help="CLI tool for end-to-end IBM DataStage to Databricks migration using Lakebridge.",
    no_args_is_help=True,
)
console = Console()

# Shared options
ConfigOption = typer.Option(None, "--config", "-c", help="Path to ds2dbx.yml config file")
ProfileOption = typer.Option(None, "--profile", "-p", help="Databricks CLI profile")
CatalogOption = typer.Option(None, "--catalog", help="Unity Catalog catalog name")
SchemaOption = typer.Option(None, "--schema", help="Unity Catalog schema name")
OutputOption = typer.Option(None, "--output", "-o", help="Output directory")
VerboseOption = typer.Option(False, "--verbose", "-v", help="Verbose output")
ForceOption = typer.Option(False, "--force", "-f", help="Force re-run completed passes")


def _load_cfg(config_path, profile, catalog, schema) -> Config:
    cfg = load_config(
        config_path=Path(config_path) if config_path else None,
        cli_overrides={},
    )
    if profile:
        cfg.databricks.profile = profile
    if catalog:
        cfg.catalog = catalog
    if schema:
        cfg.schema = schema
    return cfg


def _get_output_dir(output: Optional[str], usecase_path: Path) -> Path:
    if output:
        return Path(output)
    return Path("_ds2dbx_output") / usecase_path.name


@app.command()
def version():
    """Show version."""
    console.print(f"ds2dbx {__version__}")


@app.command()
def init(
    config_path: str = typer.Option("./ds2dbx.yml", "--output", "-o", help="Config file path"),
):
    """Initialize a ds2dbx.yml config file with all required settings."""
    path = Path(config_path)
    if path.exists():
        overwrite = typer.confirm(f"{path} already exists. Overwrite?")
        if not overwrite:
            raise typer.Abort()

    console.print("\n[bold]ds2dbx configuration wizard[/bold]\n")

    # --- Databricks connection ---
    console.print("[bold]1. Databricks connection[/bold]")
    profile = typer.prompt("  CLI profile", default="DEFAULT")

    # --- Catalog & schemas ---
    console.print("\n[bold]2. Unity Catalog & schemas[/bold]")
    catalog = typer.prompt("  Catalog name", default="migration_pilot")
    source_schema = typer.prompt("  Source schema (where input/source data is loaded)", default="source")
    target_schema = typer.prompt("  Target schema (where converted output tables go)", default="target")

    # --- Lakebridge settings ---
    console.print("\n[bold]3. Lakebridge Switch settings[/bold]")
    switch_catalog = typer.prompt("  Switch staging catalog", default=catalog)
    switch_schema = typer.prompt("  Switch staging schema", default="lakebridge")
    switch_volume = typer.prompt("  Switch staging volume", default="switch_volume")
    data_volume = typer.prompt("  Data upload volume", default="sample_data")
    model = typer.prompt("  Foundation model endpoint", default="databricks-claude-opus-4-6")

    # --- Build config ---
    cfg = Config()
    cfg.databricks.profile = profile
    cfg.catalog = catalog
    cfg.source_schema = source_schema
    cfg.target_schema = target_schema
    cfg.lakebridge.switch_catalog = switch_catalog
    cfg.lakebridge.switch_schema = switch_schema
    cfg.lakebridge.switch_volume = switch_volume
    cfg.lakebridge.data_volume = data_volume
    cfg.lakebridge.foundation_model = model

    save_config(cfg, path)
    console.print(f"\n[green]Config written to {path}[/green]")
    console.print("Run [bold]ds2dbx check[/bold] to verify connectivity.")


@app.command()
def check(
    config: str = ConfigOption,
    profile: str = ProfileOption,
):
    """Verify all prerequisites (CLI, auth, Lakebridge, FMAPI, UC)."""
    from ds2dbx.utils.subprocess_runner import run_command

    cfg = _load_cfg(config, profile, None, None)
    checks = []

    # 1. Config file
    config_found = any(p.exists() for p in [Path("ds2dbx.yml"), Path.home() / ".ds2dbx" / "config.yml"])
    checks.append(("Config file", config_found, "ds2dbx.yml" if config_found else "Not found — run ds2dbx init"))

    # 2. Databricks CLI
    r = run_command(["databricks", "--version"])
    cli_ok = r.returncode == 0
    cli_ver = r.stdout.strip().split()[-1] if cli_ok else "not found"
    cli_ver = cli_ver.lstrip("v")
    checks.append(("Databricks CLI", cli_ok, f"v{cli_ver}" if cli_ok else "Install: https://docs.databricks.com/dev-tools/cli/install.html"))

    # 3. Auth
    r = run_command(["databricks", "auth", "env", "--profile", cfg.databricks.profile])
    auth_ok = r.returncode == 0 and "DATABRICKS_HOST" in r.stdout
    checks.append(("Authentication", auth_ok, f"Profile: {cfg.databricks.profile}" if auth_ok else "Run: databricks auth login"))

    # 4. Host
    host = cfg.get_host()
    checks.append(("Workspace", bool(host), host or "Could not determine host"))

    # 5. Lakebridge
    r = run_command(["databricks", "labs", "lakebridge", "describe-transpile", "--profile", cfg.databricks.profile])
    lb_ok = r.returncode == 0
    checks.append(("Lakebridge", lb_ok, "Installed" if lb_ok else "Run: databricks labs install lakebridge"))

    # 6. BladeBridge
    bb_ok = lb_ok and "Bladebridge" in r.stdout
    bb_ver = ""
    if bb_ok:
        for line in r.stdout.splitlines():
            if "Bladebridge" in line:
                parts = line.split()
                bb_ver = parts[1] if len(parts) > 1 else "unknown"
    checks.append(("BladeBridge plugin", bb_ok, f"v{bb_ver}" if bb_ok else "Run: databricks labs lakebridge install-transpile"))

    # 7. Foundation Model API — check via workspace (best effort)
    fmapi_ok = True  # Can't easily check without REST call, assume OK if auth works
    checks.append(("Foundation Model API", fmapi_ok, f"{cfg.lakebridge.foundation_model} (assumed OK)"))

    # Print results
    console.print("\n[bold]Checking prerequisites...[/bold]\n")
    all_ok = True
    for name, ok, detail in checks:
        icon = "[green]✓[/green]" if ok else "[red]✗[/red]"
        console.print(f"  {icon} {name}: {detail}")
        if not ok:
            all_ok = False

    console.print()
    if all_ok:
        console.print("[green bold]All checks passed. Ready to convert.[/green bold]")
    else:
        console.print("[red bold]Some checks failed. Fix issues above before continuing.[/red bold]")
        raise typer.Exit(1)


@app.command()
def convert(
    path: str = typer.Argument(..., help="Path to use case directory"),
    config: str = ConfigOption,
    profile: str = ProfileOption,
    catalog: str = CatalogOption,
    schema: str = SchemaOption,
    output: str = OutputOption,
    verbose: bool = VerboseOption,
    force: bool = ForceOption,
    passes: str = typer.Option("1,2,3,4,5", "--passes", help="Comma-separated passes to run"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show plan without executing"),
):
    """Convert a single use case (all 5 passes)."""
    from ds2dbx.scanner.folder import scan_usecase
    from ds2dbx.scanner.pattern import detect_pattern
    from ds2dbx.utils.status import init_status

    cfg = _load_cfg(config, profile, catalog, schema)
    uc_path = Path(path).resolve()
    if not uc_path.exists():
        console.print(f"[red]Directory not found: {uc_path}[/red]")
        raise typer.Exit(1)

    out_dir = _get_output_dir(output, uc_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Scan
    console.print(f"\n[bold]Scanning:[/bold] {uc_path.name}")
    manifest = scan_usecase(uc_path, cfg)
    manifest.pattern = detect_pattern(manifest)

    console.print(f"  Pattern: [cyan]{manifest.pattern}[/cyan]")
    console.print(f"  DDL files: {len(manifest.ddl_files)}")
    console.print(f"  Data files: {len(manifest.data_files)} + {len(manifest.source_files)} source")
    console.print(f"  DataStage XML: {len(manifest.datastage_files)}")
    console.print(f"  Shell scripts: {len(manifest.shell_logic_scripts)} logic, {len(manifest.shell_skip_scripts)} wrappers (skipped)")

    if dry_run:
        console.print("\n[yellow]Dry run — no passes executed.[/yellow]")
        console.print("\n[bold]Lakebridge commands that would be executed:[/bold]")
        lb = cfg.lakebridge
        ws_base = cfg.get_workspace_base()
        prompt_base = f"/Workspace/Users/{{username}}/.lakebridge/switch/resources/custom_prompts"
        if manifest.ddl_files and 1 in [int(p.strip()) for p in passes.split(",")]:
            ws_out = f"{ws_base}/{manifest.name}/pass1_ddl"
            console.print(f"  [dim]Pass 1 (DDL → Switch):[/dim]")
            console.print(f"    Custom prompt: {prompt_base}/switch_ddl_prompt.yml")
            console.print(f"    switch_config: source_format=sql, max_fix_attempts={lb.max_fix_attempts}, concurrency={lb.concurrency}")
            console.print(f"    databricks labs lakebridge llm-transpile \\")
            console.print(f"      --input-source <local> --output-ws-folder {ws_out} \\")
            console.print(f"      --foundation-model {lb.foundation_model}")
        if manifest.datastage_files and 3 in [int(p.strip()) for p in passes.split(",")]:
            console.print(f"  [dim]Pass 3a (DataStage XML → BladeBridge):[/dim]")
            console.print(f"    databricks labs lakebridge transpile \\")
            console.print(f"      --input-source <local> --output-folder <local> \\")
            console.print(f"      --source-dialect datastage --target-technology PYSPARK")
            console.print(f"  [dim]Pass 3b (Triage → classify clean vs broken)[/dim]")
            ws_out = f"{ws_base}/{manifest.name}/pass3_switch"
            console.print(f"  [dim]Pass 3c (Broken notebooks → Switch):[/dim]")
            console.print(f"    Custom prompt: ds2dbx/prompts/datastage_fix_prompt.yml (30 bug patterns)")
            console.print(f"    switch_config: source_format=databricks, max_fix_attempts={lb.max_fix_attempts}, concurrency={lb.concurrency}")
            console.print(f"    databricks labs lakebridge llm-transpile \\")
            console.print(f"      --input-source <local> --output-ws-folder {ws_out} \\")
            console.print(f"      --foundation-model {lb.foundation_model}")
            console.print(f"  [dim]Pass 3d (Post-processing → 10 deterministic notebook fixes + 5 workflow fixes)[/dim]")
        if manifest.shell_logic_scripts and 4 in [int(p.strip()) for p in passes.split(",")]:
            ws_out = f"{ws_base}/{manifest.name}/pass4_shell"
            console.print(f"  [dim]Pass 4 (Shell → Switch):[/dim]")
            console.print(f"    Custom prompt: {prompt_base}/switch_shell_prompt.yml")
            console.print(f"    switch_config: source_format=generic, max_fix_attempts={lb.max_fix_attempts}, concurrency={lb.concurrency}")
            console.print(f"    databricks labs lakebridge llm-transpile \\")
            console.print(f"      --input-source <local> --output-ws-folder {ws_out} \\")
            console.print(f"      --foundation-model {lb.foundation_model}")
        return

    # Save manifest
    import dataclasses
    manifest_data = {
        "name": manifest.name,
        "path": str(manifest.path),
        "pattern": manifest.pattern,
        "ddl_files": [str(f) for f in manifest.ddl_files],
        "data_files": [str(f) for f in manifest.data_files],
        "source_files": [str(f) for f in manifest.source_files],
        "datastage_files": [str(f) for f in manifest.datastage_files],
        "shell_logic_scripts": [str(f) for f in manifest.shell_logic_scripts],
        "shell_skip_scripts": [str(f) for f in manifest.shell_skip_scripts],
    }
    with open(out_dir / "_manifest.json", "w") as f:
        json.dump(manifest_data, f, indent=2)

    # Only init if no status exists; otherwise preserve existing pass results
    from ds2dbx.utils.status import read_status
    existing = read_status(out_dir)
    if not existing:
        init_status(out_dir, manifest.name, manifest.pattern)
    else:
        # Update pattern if it changed
        existing["pattern_detected"] = manifest.pattern
        from ds2dbx.utils.status import write_status
        write_status(out_dir, existing)

    pass_list = [int(p.strip()) for p in passes.split(",")]

    # Pass 1: DDL
    if 1 in pass_list and manifest.ddl_files:
        console.print(f"\n[bold]Pass 1 (DDL):[/bold] Converting {len(manifest.ddl_files)} DDL files...")
        from ds2dbx.passes.pass1_ddl import Pass1DDL
        p1 = Pass1DDL(cfg, out_dir, verbose)
        metrics = p1.run(manifest, force)
        console.print(f"  [green]✓ Pass 1 complete[/green] — {metrics.get('output_files', 0)} notebook(s)")
    elif 1 in pass_list:
        console.print("\n[yellow]Pass 1 (DDL): Skipped — no DDL/ folder[/yellow]")

    # Pass 2: Data
    if 2 in pass_list and (manifest.data_files or manifest.source_files):
        total_data = len(manifest.data_files) + len(manifest.source_files)
        console.print(f"\n[bold]Pass 2 (Data):[/bold] Loading {total_data} data files...")
        from ds2dbx.passes.pass2_data import Pass2Data
        p2 = Pass2Data(cfg, out_dir, verbose)
        metrics = p2.run(manifest, force)
        console.print(f"  [green]✓ Pass 2 complete[/green] — {metrics.get('tables_count', 0)} table(s)")
    elif 2 in pass_list:
        console.print("\n[yellow]Pass 2 (Data): Skipped — no Data/ folder[/yellow]")

    # Pass 3: DataStage XML
    if 3 in pass_list and manifest.datastage_files:
        console.print(f"\n[bold]Pass 3 (Transpile):[/bold] Converting {len(manifest.datastage_files)} DataStage XML files...")
        from ds2dbx.passes.pass3_transpile import Pass3Transpile
        p3 = Pass3Transpile(cfg, out_dir, verbose)
        metrics = p3.run(manifest, force)
        clean = metrics.get("triage_clean", 0)
        fixed = metrics.get("switch_fixed", 0)
        failed = metrics.get("switch_failed", 0)
        wf = metrics.get("bladebridge_workflows", 0)
        console.print(f"  [green]✓ Pass 3 complete[/green] — {clean} clean + {fixed} fixed, {failed} manual, {wf} workflow(s)")
    elif 3 in pass_list:
        console.print("\n[yellow]Pass 3 (Transpile): Skipped — no Datastage/ folder[/yellow]")

    # Pass 4: Shell scripts
    if 4 in pass_list and manifest.shell_logic_scripts:
        console.print(f"\n[bold]Pass 4 (Shell):[/bold] Converting {len(manifest.shell_logic_scripts)} shell scripts...")
        from ds2dbx.passes.pass4_shell import Pass4Shell
        p4 = Pass4Shell(cfg, out_dir, verbose)
        metrics = p4.run(manifest, force)
        console.print(f"  [green]✓ Pass 4 complete[/green] — {metrics.get('converted', 0)} converted")
    elif 4 in pass_list:
        console.print("\n[yellow]Pass 4 (Shell): Skipped — no logic scripts[/yellow]")

    # Pass 5: Validation
    if 5 in pass_list:
        console.print(f"\n[bold]Pass 5 (Validate):[/bold] Generating {manifest.pattern} validation notebook...")
        from ds2dbx.passes.pass5_validate import Pass5Validate
        p5 = Pass5Validate(cfg, out_dir, verbose)
        metrics = p5.run(manifest, force)
        console.print(f"  [green]✓ Pass 5 complete[/green] — {metrics.get('checks_defined', 0)} checks defined")

    console.print(f"\n[bold green]Conversion complete.[/bold green] Output: {out_dir}")
    console.print(f"Run [bold]ds2dbx status {path}[/bold] for details.")


@app.command()
def convert_all(
    path: str = typer.Argument(..., help="Path containing use case directories"),
    config: str = ConfigOption,
    profile: str = ProfileOption,
    catalog: str = CatalogOption,
    schema: str = SchemaOption,
    output: str = OutputOption,
    verbose: bool = VerboseOption,
    force: bool = ForceOption,
    passes: str = typer.Option("1,2,3,4,5", "--passes", help="Comma-separated passes to run"),
):
    """Convert all use cases in a directory."""
    from ds2dbx.scanner.folder import discover_usecases

    parent = Path(path).resolve()
    usecases = discover_usecases(parent)

    if not usecases:
        console.print(f"[red]No use case directories found in {parent}[/red]")
        raise typer.Exit(1)

    console.print(f"\n[bold]Found {len(usecases)} use case(s):[/bold]")
    for uc in usecases:
        console.print(f"  • {uc.name}")

    for uc in usecases:
        console.print(f"\n{'='*60}")
        uc_output = (Path(output) if output else Path("_ds2dbx_output")) / uc.name
        convert(
            path=str(uc),
            config=config,
            profile=profile,
            catalog=catalog,
            schema=schema,
            output=str(uc_output),
            verbose=verbose,
            force=force,
            passes=passes,
            dry_run=False,
        )


@app.command()
def ddl(
    path: str = typer.Argument(..., help="Path to use case directory"),
    config: str = ConfigOption,
    profile: str = ProfileOption,
    catalog: str = CatalogOption,
    schema: str = SchemaOption,
    output: str = OutputOption,
    verbose: bool = VerboseOption,
    force: bool = ForceOption,
):
    """Run Pass 1 only — convert DDL files to Delta Lake."""
    convert(path=path, config=config, profile=profile, catalog=catalog,
            schema=schema, output=output, verbose=verbose, force=force,
            passes="1", dry_run=False)


@app.command()
def load_data(
    path: str = typer.Argument(..., help="Path to use case directory"),
    config: str = ConfigOption,
    profile: str = ProfileOption,
    catalog: str = CatalogOption,
    schema: str = SchemaOption,
    output: str = OutputOption,
    verbose: bool = VerboseOption,
    force: bool = ForceOption,
):
    """Run Pass 2 only — upload and load sample data."""
    convert(path=path, config=config, profile=profile, catalog=catalog,
            schema=schema, output=output, verbose=verbose, force=force,
            passes="2", dry_run=False)


@app.command()
def transpile(
    path: str = typer.Argument(..., help="Path to use case directory"),
    config: str = ConfigOption,
    profile: str = ProfileOption,
    catalog: str = CatalogOption,
    schema: str = SchemaOption,
    output: str = OutputOption,
    verbose: bool = VerboseOption,
    force: bool = ForceOption,
):
    """Run Pass 3 only — convert DataStage XML via BladeBridge + Switch."""
    convert(path=path, config=config, profile=profile, catalog=catalog,
            schema=schema, output=output, verbose=verbose, force=force,
            passes="3", dry_run=False)


@app.command()
def convert_shell(
    path: str = typer.Argument(..., help="Path to use case directory"),
    config: str = ConfigOption,
    profile: str = ProfileOption,
    catalog: str = CatalogOption,
    schema: str = SchemaOption,
    output: str = OutputOption,
    verbose: bool = VerboseOption,
    force: bool = ForceOption,
):
    """Run Pass 4 only — convert shell scripts via Switch."""
    convert(path=path, config=config, profile=profile, catalog=catalog,
            schema=schema, output=output, verbose=verbose, force=force,
            passes="4", dry_run=False)


@app.command()
def validate(
    path: str = typer.Argument(..., help="Path to use case directory"),
    config: str = ConfigOption,
    profile: str = ProfileOption,
    catalog: str = CatalogOption,
    schema: str = SchemaOption,
    output: str = OutputOption,
    verbose: bool = VerboseOption,
    force: bool = ForceOption,
):
    """Run Pass 5 only — generate validation notebooks."""
    convert(path=path, config=config, profile=profile, catalog=catalog,
            schema=schema, output=output, verbose=verbose, force=force,
            passes="5", dry_run=False)


@app.command()
def deploy(
    path: str = typer.Argument(..., help="Path to use case directory or parent"),
    config: str = ConfigOption,
    profile: str = ProfileOption,
    verbose: bool = VerboseOption,
    run_prereqs: bool = typer.Option(False, "--run-prereqs", help="Run DDL + data loader on cluster before deploying workflows"),
    cluster_id: str = typer.Option("", "--cluster-id", help="Cluster ID for running prerequisite notebooks"),
):
    """Deploy converted notebooks and workflows to workspace.

    With --run-prereqs, also creates schema/volume, runs DDL and data loader
    notebooks on a cluster, and creates source views for missing tables.
    """
    from ds2dbx.workspace.deploy import deploy_usecase
    from ds2dbx.scanner.folder import discover_usecases

    cfg = _load_cfg(config, profile, None, None)
    target = Path(path).resolve()
    output_base = Path("_ds2dbx_output")

    # Discover use cases
    if (target / "Shell").exists() or (target / "DDL").exists():
        uc_paths = [target]
    else:
        uc_paths = discover_usecases(target)

    if not uc_paths:
        console.print(f"[red]No use cases found at {target}[/red]")
        raise typer.Exit(1)

    # Find cluster if --run-prereqs but no --cluster-id
    if run_prereqs and not cluster_id:
        cluster_id = _find_cluster(cfg)
        if not cluster_id:
            console.print("[red]--run-prereqs requires a running cluster. Provide --cluster-id or start a cluster.[/red]")
            raise typer.Exit(1)
        console.print(f"Using cluster: {cluster_id}")

    total_notebooks = 0
    total_workflows = 0

    for uc_path in uc_paths:
        out_dir = output_base / uc_path.name

        if not out_dir.exists():
            console.print(f"\n[yellow]Skipping {uc_path.name} — not yet converted[/yellow]")
            continue

        ws_base = f"{cfg.get_workspace_base()}/{uc_path.name}"
        console.print(f"\n[bold]Deploying: {uc_path.name}[/bold]")
        console.print(f"  Target: {ws_base}")

        metrics = deploy_usecase(out_dir, ws_base, cfg, verbose)
        total_notebooks += metrics["notebooks_uploaded"]
        total_workflows += metrics["workflows_created"]

        console.print(
            f"  [green]Done:[/green] {metrics['notebooks_uploaded']} notebook(s), "
            f"{metrics['workflows_created']} workflow(s)"
        )

        # --- Run prerequisites if requested ---
        if run_prereqs:
            console.print(f"\n  [bold]Running prerequisites on cluster {cluster_id}...[/bold]")
            from ds2dbx.workspace.setup import run_setup
            setup_metrics = run_setup(out_dir, cfg, cluster_id, verbose)
            console.print(
                f"  [green]Setup:[/green] schema={setup_metrics['schema_created']}, "
                f"volume={setup_metrics['volume_created']}, "
                f"DDL={'OK' if setup_metrics['ddl_notebook_run'] else 'SKIP'}, "
                f"data={'OK' if setup_metrics['data_loader_run'] else 'SKIP'}, "
                f"views={setup_metrics['source_views_created']}"
            )

    console.print(f"\n[bold]Total:[/bold] {total_notebooks} notebooks, {total_workflows} workflows deployed")


def _find_cluster(cfg: Config) -> str:
    """Find a running cluster from the workspace."""
    host = cfg.get_host()
    token = cfg.get_token()
    if not host or not token:
        return ""
    try:
        import requests
        resp = requests.get(
            f"{host}/api/2.0/clusters/list",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if resp.status_code == 200:
            for c in resp.json().get("clusters", []):
                if c.get("state") == "RUNNING":
                    return c["cluster_id"]
    except Exception:
        pass
    return ""


@app.command()
def verify(
    path: str = typer.Argument(..., help="Path to use case directory or parent"),
    config: str = ConfigOption,
    profile: str = ProfileOption,
    catalog: str = CatalogOption,
    schema: str = SchemaOption,
    output: str = OutputOption,
    verbose: bool = VerboseOption,
):
    """Verify conversion output against source files."""
    from ds2dbx.verify.ddl_verify import verify_ddl
    from ds2dbx.verify.shell_verify import verify_shell
    from ds2dbx.scanner.folder import scan_usecase, discover_usecases
    from ds2dbx.scanner.shell_classifier import is_ssh_wrapper

    cfg = _load_cfg(config, profile, catalog, schema)
    target = Path(path).resolve()
    output_base = Path("_ds2dbx_output")

    # Discover use cases
    if (target / "Shell").exists() or (target / "DDL").exists():
        uc_paths = [target]
    else:
        uc_paths = discover_usecases(target)

    if not uc_paths:
        console.print(f"[red]No use cases found at {target}[/red]")
        raise typer.Exit(1)

    total_errors = 0
    total_warnings = 0

    for uc_path in uc_paths:
        out_dir = _get_output_dir(output, uc_path) if output else output_base / uc_path.name
        manifest = scan_usecase(uc_path, cfg)

        console.print(f"\n[bold]Verifying: {uc_path.name}[/bold]")

        # --- DDL verification ---
        ddl_output = out_dir / "pass1_ddl" / "output"
        ddl_notebooks = list(ddl_output.glob("*.py")) if ddl_output.exists() else []

        if manifest.ddl_files and ddl_notebooks:
            console.print(f"  [bold]DDL:[/bold] {len(manifest.ddl_files)} source -> {len(ddl_notebooks)} output")
            for nb in ddl_notebooks:
                ddl_issues = verify_ddl(
                    manifest.ddl_files, nb,
                    catalog=cfg.catalog, schema=cfg.get_target_schema(),
                )
                for issue in ddl_issues:
                    if issue.severity == "error":
                        console.print(f"    [red]ERROR[/red] {issue.message}")
                        total_errors += 1
                    else:
                        console.print(f"    [yellow]WARN[/yellow]  {issue.message}")
                        total_warnings += 1
                if not ddl_issues:
                    console.print(f"    [green]PASS[/green]  All {len(manifest.ddl_files)} tables/views verified")
        elif manifest.ddl_files:
            console.print(f"  [yellow]DDL: {len(manifest.ddl_files)} source files but no output — run ds2dbx ddl first[/yellow]")
        else:
            console.print(f"  [dim]DDL: no source files[/dim]")

        # --- Shell verification ---
        shell_output = out_dir / "pass4_shell" / "output"
        shell_notebooks = list(shell_output.glob("*.py")) if shell_output.exists() else []

        logic_scripts = manifest.shell_logic_scripts
        if logic_scripts and shell_notebooks:
            console.print(f"  [bold]Shell:[/bold] {len(logic_scripts)} source -> {len(shell_notebooks)} output")

            # Match source scripts to output notebooks by stem
            out_map = {nb.stem.lower(): nb for nb in shell_notebooks}
            for script in logic_scripts:
                stem = script.stem.lower()
                if stem not in out_map:
                    # Try without extension variations
                    matched = None
                    for out_stem, out_nb in out_map.items():
                        if stem in out_stem or out_stem in stem:
                            matched = out_nb
                            break
                    if not matched:
                        console.print(f"    [red]ERROR[/red] No output for '{script.name}'")
                        total_errors += 1
                        continue
                    out_nb = matched
                else:
                    out_nb = out_map[stem]

                shell_issues = verify_shell(script, out_nb)
                for issue in shell_issues:
                    if issue.severity == "error":
                        console.print(f"    [red]ERROR[/red] {script.name}: {issue.message}")
                        total_errors += 1
                    else:
                        console.print(f"    [yellow]WARN[/yellow]  {script.name}: {issue.message}")
                        total_warnings += 1
                if not shell_issues:
                    console.print(f"    [green]PASS[/green]  {script.name}")
        elif logic_scripts:
            console.print(f"  [yellow]Shell: {len(logic_scripts)} source scripts but no output — run ds2dbx convert-shell first[/yellow]")
        else:
            console.print(f"  [dim]Shell: no logic scripts[/dim]")

    # Summary
    console.print()
    if total_errors == 0 and total_warnings == 0:
        console.print("[bold green]All checks passed.[/bold green]")
    elif total_errors == 0:
        console.print(f"[bold yellow]{total_warnings} warning(s), 0 errors.[/bold yellow]")
    else:
        console.print(f"[bold red]{total_errors} error(s), {total_warnings} warning(s).[/bold red]")
        raise typer.Exit(1)


@app.command()
def status(
    path: str = typer.Argument(..., help="Path to use case directory or parent"),
    config: str = ConfigOption,
):
    """Show conversion status and results."""
    from ds2dbx.utils.status import read_status
    from ds2dbx.scanner.folder import discover_usecases

    target = Path(path).resolve()
    output_base = Path("_ds2dbx_output")

    # Find use cases
    if (target / "Shell").exists() or (target / "DDL").exists():
        # Single use case
        uc_paths = [target]
    else:
        uc_paths = discover_usecases(target)

    if not uc_paths:
        console.print(f"[red]No use cases found at {target}[/red]")
        raise typer.Exit(1)

    table = Table(title="Conversion Status")
    table.add_column("Use Case", style="bold", max_width=50)
    table.add_column("Pattern", style="cyan")
    table.add_column("P1", justify="center")
    table.add_column("P2", justify="center")
    table.add_column("P3", justify="center")
    table.add_column("P4", justify="center")
    table.add_column("P5", justify="center")

    pass_names = ["pass1_ddl", "pass2_data", "pass3_transpile", "pass4_shell", "pass5_validate"]

    for uc_path in uc_paths:
        out_dir = output_base / uc_path.name
        st = read_status(out_dir)
        pattern = st.get("pattern_detected", "—")

        pass_status = []
        for pn in pass_names:
            ps = st.get("passes", {}).get(pn, {}).get("status", "—")
            if ps == "completed":
                pass_status.append("[green]✓[/green]")
            elif ps == "failed":
                pass_status.append("[red]✗[/red]")
            elif ps == "running":
                pass_status.append("[yellow]…[/yellow]")
            else:
                pass_status.append("—")

        # Truncate name for display
        name = uc_path.name
        if len(name) > 50:
            name = name[:47] + "..."

        table.add_row(name, pattern, *pass_status)

    console.print()
    console.print(table)

    # Show Pass 3 details if available
    for uc_path in uc_paths:
        out_dir = output_base / uc_path.name
        st = read_status(out_dir)
        p3 = st.get("passes", {}).get("pass3_transpile", {})
        if p3.get("status") == "completed":
            rate = p3.get("conversion_rate", 0)
            clean = p3.get("triage_clean", 0)
            fixed = p3.get("switch_fixed", 0)
            failed = p3.get("switch_failed", 0)
            wf = p3.get("bladebridge_workflows", 0)
            total = clean + fixed + failed
            console.print(f"\n  {uc_path.name}:")
            console.print(f"    Notebooks: {clean} clean + {fixed} fixed + {failed} manual ({total} total)")
            console.print(f"    Workflows: {wf}")


if __name__ == "__main__":
    app()
