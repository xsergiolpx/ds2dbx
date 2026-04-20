"""Run prerequisite setup: create schema, volume, tables, load data, create source views."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import requests
from rich.console import Console

from ds2dbx.config import Config

console = Console()


def run_setup(
    output_dir: Path,
    config: Config,
    cluster_id: str,
    verbose: bool = False,
) -> dict:
    """Run all prerequisite setup for a use case before workflow execution.

    1. Create schema if needed
    2. Create volume for source files
    3. Upload source files to volume
    4. Run DDL notebook on cluster (creates target tables)
    5. Run data loader notebook on cluster (loads sample data)
    6. Generate and run source-views notebook (creates views for missing source tables)
    """
    host = config.get_host()
    token = config.get_token()
    catalog = config.catalog
    source_schema = config.get_source_schema()
    target_schema = config.get_target_schema()

    metrics = {
        "schema_created": False,
        "volume_created": False,
        "source_files_uploaded": 0,
        "ddl_notebook_run": False,
        "data_loader_run": False,
        "source_views_created": 0,
    }

    if not host or not token:
        console.print("  [red]Cannot get host/token — skipping setup[/red]")
        return metrics

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # --- Step 1: Create schemas ---
    console.print(f"  Creating schemas {catalog}.{source_schema} + {catalog}.{target_schema}...")
    _run_sql(host, headers, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{source_schema}")
    _run_sql(host, headers, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{target_schema}")
    metrics["schema_created"] = True

    # --- Step 2: Create volume (in source schema for source data) ---
    console.print(f"  Creating volume {catalog}.{source_schema}.data...")
    _run_sql(host, headers, f"CREATE VOLUME IF NOT EXISTS {catalog}.{source_schema}.data")
    metrics["volume_created"] = True

    # --- Step 3: Upload source files to volume ---
    # Find source files from the use case manifest
    manifest_path = output_dir / "_manifest.json"
    if manifest_path.exists():
        with open(manifest_path) as f:
            manifest = json.load(f)
        source_files = [Path(p) for p in manifest.get("source_files", [])]
        for sf in source_files:
            if sf.exists():
                vol_path = f"/Volumes/{catalog}/{source_schema}/data/{sf.name}"
                console.print(f"  Uploading {sf.name} -> {vol_path}")
                _upload_to_volume(host, token, sf, vol_path, verbose)
                metrics["source_files_uploaded"] += 1

    # --- Step 4: Run DDL notebook ---
    ddl_notebooks = list((output_dir / "pass1_ddl" / "output").glob("*.py")) if (output_dir / "pass1_ddl" / "output").exists() else []
    if ddl_notebooks:
        ws_base = config.get_workspace_base()
        uc_name = output_dir.name
        ws_path = f"{ws_base}/{uc_name}/notebooks"

        for nb in ddl_notebooks:
            nb_ws_path = f"{ws_path}/{nb.stem}"
            console.print(f"  Running DDL notebook: {nb.stem}...")
            success = _run_notebook(host, headers, nb_ws_path, cluster_id, verbose)
            if success:
                metrics["ddl_notebook_run"] = True
            else:
                console.print(f"  [yellow]DDL notebook failed — tables may not be created[/yellow]")

    # --- Step 5: Run data loader notebook ---
    data_notebooks = list((output_dir / "pass2_data" / "output").glob("*.py")) if (output_dir / "pass2_data" / "output").exists() else []
    if data_notebooks:
        ws_base = config.get_workspace_base()
        uc_name = output_dir.name
        ws_path = f"{ws_base}/{uc_name}/notebooks"

        for nb in data_notebooks:
            nb_ws_path = f"{ws_path}/{nb.stem}"
            console.print(f"  Running data loader: {nb.stem}...")
            success = _run_notebook(host, headers, nb_ws_path, cluster_id, verbose)
            if success:
                metrics["data_loader_run"] = True
            else:
                console.print(f"  [yellow]Data loader failed — data may not be loaded[/yellow]")

    # --- Step 6: Detect and create source views/tables ---
    merged_dir = output_dir / "pass3_transpile" / "merged"
    if merged_dir.exists():
        source_views = _detect_missing_source_tables(
            merged_dir, output_dir, catalog, source_schema, target_schema
        )
        if source_views:
            console.print(f"  Creating {len(source_views)} source table(s)/view(s)...")
            for view_name, view_sql in source_views.items():
                console.print(f"    Creating {catalog}.{source_schema}.{view_name}")
                _run_sql(host, headers, view_sql)
                metrics["source_views_created"] += 1

    # --- Step 7: Create missing P_* partition tables referenced by BIGDATA_TRG ---
    # BIGDATA_TRG does INSERT OVERWRITE P_table PARTITION(...) SELECT * FROM IN_table
    # The P_* table must pre-exist with partition columns. If DDL didn't define it,
    # create it from the IN_* table schema + partition columns (ptn_yyyy, ptn_mm).
    if merged_dir.exists():
        missing_p_tables = _detect_missing_partition_tables(
            merged_dir, catalog, target_schema, source_schema
        )
        if missing_p_tables:
            console.print(f"  Creating {len(missing_p_tables)} partition table(s)...")
            for tbl_name, sqls in missing_p_tables.items():
                console.print(f"    Creating {catalog}.{target_schema}.{tbl_name}")
                for sql in sqls:
                    _run_sql(host, headers, sql)

    return metrics


def _detect_missing_partition_tables(
    merged_dir: Path,
    catalog: str,
    target_schema: str,
    source_schema: str = "",
) -> dict[str, list[str]]:
    """Detect P_* partition tables referenced by BIGDATA_TRG but not in DDL.

    Scans UserVar notebooks for tbl_nm values, constructs P_tbl_nm table names,
    and creates empty partition tables from IN_tbl_nm schema.
    """
    # Collect tbl_nm values from UserVar notebooks
    tbl_nm_values: set[str] = set()
    for nb in merged_dir.glob("*UserVar*.py"):
        content = nb.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(r'tbl_nm\s*=\s*"(\w+)"', content):
            tbl_nm_values.add(m.group(1))

    if not tbl_nm_values:
        return {}

    # For each tbl_nm, check if P_tbl_nm exists. If not, create from IN_tbl_nm.
    result: dict[str, str] = {}
    for tbl_nm in tbl_nm_values:
        p_table = f"P_{tbl_nm}"
        in_table = f"IN_{tbl_nm}"
        full_p = f"{catalog}.{target_schema}.{p_table}"
        full_in = f"{catalog}.{target_schema}.{in_table}"

        # Create P_* as empty partitioned table from IN_* or source table schema.
        # IN_* may not exist at setup time (created by LoadEBAN at runtime).
        # Try: 1) IN_* in target, 2) same table in source_schema, 3) P_* from DDL
        # Use a SQL that tries each source — first one that works wins.
        sources = [full_in]
        if source_schema:
            sources.append(f"{catalog}.{source_schema}.{in_table}")
            # Also try the base table name without IN_ prefix
            sources.append(f"{catalog}.{source_schema}.{tbl_nm}")

        # Generate SQL for each fallback
        sqls = [f"DROP TABLE IF EXISTS {full_p}"]
        for src in sources:
            sqls.append(
                f"CREATE TABLE IF NOT EXISTS {full_p} "
                f"USING DELTA "
                f"PARTITIONED BY (ptn_yyyy, ptn_mm) "
                f"AS SELECT *, CAST('0000' AS STRING) AS ptn_yyyy, CAST('00' AS STRING) AS ptn_mm "
                f"FROM {src} WHERE 1=0"
            )
        result[p_table] = sqls

    return result


def _run_sql(host: str, headers: dict, sql: str, timeout: int = 60) -> bool:
    """Execute SQL via Statement API."""
    try:
        resp = requests.post(
            f"{host}/api/2.0/sql/statements",
            json={
                "statement": sql,
                "wait_timeout": "50s",
                "warehouse_id": _get_warehouse_id(host, headers),
            },
            headers=headers,
            timeout=timeout,
        )
        if resp.status_code == 200:
            data = resp.json()
            state = data.get("status", {}).get("state", "")
            if state in ("SUCCEEDED", "CLOSED"):
                return True
            # Poll if pending
            stmt_id = data.get("statement_id", "")
            for _ in range(15):
                time.sleep(2)
                poll = requests.get(
                    f"{host}/api/2.0/sql/statements/{stmt_id}",
                    headers=headers,
                    timeout=15,
                )
                if poll.status_code == 200:
                    state = poll.json().get("status", {}).get("state", "")
                    if state in ("SUCCEEDED", "CLOSED"):
                        return True
                    if state == "FAILED":
                        return False
            return False
        return False
    except Exception:
        return False


_warehouse_cache: str = ""


def _get_warehouse_id(host: str, headers: dict) -> str:
    """Find any available SQL warehouse."""
    global _warehouse_cache
    if _warehouse_cache:
        return _warehouse_cache
    try:
        resp = requests.get(f"{host}/api/2.0/sql/warehouses", headers=headers, timeout=15)
        if resp.status_code == 200:
            for wh in resp.json().get("warehouses", []):
                if wh.get("state") in ("RUNNING", "STARTING"):
                    _warehouse_cache = wh["id"]
                    return wh["id"]
            # Just return the first one
            warehouses = resp.json().get("warehouses", [])
            if warehouses:
                _warehouse_cache = warehouses[0]["id"]
                return warehouses[0]["id"]
    except Exception:
        pass
    return ""


def _upload_to_volume(host: str, token: str, local_path: Path, vol_path: str, verbose: bool = False) -> bool:
    """Upload a file to a Unity Catalog Volume."""
    try:
        with open(local_path, "rb") as f:
            resp = requests.put(
                f"{host}/api/2.0/fs/files{vol_path}",
                headers={"Authorization": f"Bearer {token}"},
                data=f,
                timeout=60,
            )
        return resp.status_code in (200, 204)
    except Exception:
        return False


def _run_notebook(host: str, headers: dict, notebook_path: str, cluster_id: str, verbose: bool = False) -> bool:
    """Submit a notebook to run on a cluster and wait for completion."""
    try:
        resp = requests.post(
            f"{host}/api/2.1/jobs/runs/submit",
            json={
                "run_name": f"ds2dbx-setup-{Path(notebook_path).name}",
                "tasks": [{
                    "task_key": "setup",
                    "existing_cluster_id": cluster_id,
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "source": "WORKSPACE",
                    },
                }],
            },
            headers=headers,
            timeout=30,
        )
        if resp.status_code != 200:
            return False

        run_id = resp.json().get("run_id")
        if not run_id:
            return False

        # Poll until complete (max 5 min)
        for _ in range(30):
            time.sleep(10)
            poll = requests.get(
                f"{host}/api/2.1/jobs/runs/get?run_id={run_id}",
                headers=headers,
                timeout=15,
            )
            if poll.status_code == 200:
                state = poll.json().get("state", {})
                lcs = state.get("life_cycle_state", "")
                rs = state.get("result_state", "")
                if lcs == "TERMINATED":
                    if rs == "SUCCESS":
                        return True
                    else:
                        if verbose:
                            console.print(f"    [red]Run {run_id} failed: {rs}[/red]")
                        return False
                elif lcs in ("INTERNAL_ERROR", "SKIPPED"):
                    return False

        return False  # Timeout
    except Exception:
        return False


def _detect_missing_source_tables(
    merged_dir: Path,
    output_dir: Path,
    catalog: str,
    source_schema: str,
    target_schema: str,
) -> dict[str, str]:
    """Detect source tables referenced in notebooks but not created by DDL/data loader.

    Source tables go in source_schema; they reference underlying tables from
    either source_schema (data loader) or target_schema (DDL).

    Returns dict of {table_name: CREATE TABLE/VIEW SQL}.
    """
    # 1. Find all tables created by DDL (in target_schema) and data loader (in source_schema)
    created_tables: set[str] = set()

    # Scan DDL output for CREATE TABLE in any schema
    ddl_output = output_dir / "pass1_ddl" / "output"
    if ddl_output.exists():
        for nb in ddl_output.glob("*.py"):
            content = nb.read_text(encoding="utf-8", errors="replace")
            for m in re.finditer(
                r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+\w+\.\w+\.(\w+)",
                content, re.IGNORECASE,
            ):
                created_tables.add(m.group(1).lower())

    # Scan data loader output for saveAsTable in any schema
    data_output = output_dir / "pass2_data" / "output"
    if data_output.exists():
        for nb in data_output.glob("*.py"):
            content = nb.read_text(encoding="utf-8", errors="replace")
            for m in re.finditer(r'saveAsTable\(\s*"\w+\.\w+\.(\w+)"', content):
                created_tables.add(m.group(1).lower())

    # 1b. Build column alias mappings from BladeBridge RAW output (deterministic).
    # BladeBridge always produces consistent column names from DataStage XML metadata.
    # These are the "ground truth" aliases that don't vary across LLM runs.
    # Pattern: DSLink3.ACTION_CD.alias('ACTN_CD') → {ACTION_CD: ACTN_CD}
    bb_alias_maps: dict[str, dict[str, str]] = {}  # notebook_stem → {oracle_col: target_col}
    bb_output_dir = output_dir / "pass3_transpile" / "bladebridge_output"
    if bb_output_dir.exists():
        for nb in bb_output_dir.glob("*.py"):
            content = nb.read_text(encoding="utf-8", errors="replace")
            alias_map = _extract_column_aliases(content)
            if alias_map:
                bb_alias_maps[nb.stem] = alias_map

    # 2. Find tables referenced in workflow widget defaults AND workflow JSON base_parameters
    referenced_tables: dict[str, dict] = {}

    # 2a. From notebook widget defaults
    for nb in merged_dir.glob("*.py"):
        content = nb.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(
            r"dbutils\.widgets\.text\(name\s*=\s*'(SRC_TBL|TABLE_NAME|TBL_NM)'\s*,\s*defaultValue\s*=\s*'(\w+)'\)",
            content,
        ):
            param_name, default_val = m.group(1), m.group(2)
            if default_val and default_val.lower() not in created_tables:
                columns = _extract_source_columns(content)
                if columns:
                    referenced_tables[default_val] = {
                        "columns": columns,
                        "notebook": nb.name,
                        "notebook_content": content,
                    }

    # 2b. From workflow JSON base_parameters (catches values passed via task interpolation)
    for wf_file in merged_dir.glob("*.json"):
        try:
            with open(wf_file) as f:
                wf_data = json.load(f)
            for task in wf_data.get("tasks", []):
                bp = task.get("notebook_task", {}).get("base_parameters", {})
                for key in ("SRC_TBL", "src_tbl_nm"):
                    val = bp.get(key, "")
                    # Skip interpolation syntax like {{tasks...}}
                    if val and not val.startswith("{{") and val.lower() not in created_tables:
                        if val not in referenced_tables:
                            # Find the notebook this task references
                            nb_path_str = task.get("notebook_task", {}).get("notebook_path", "")
                            nb_stem = Path(nb_path_str).name if nb_path_str else ""
                            nb_file = merged_dir / f"{nb_stem}.py"
                            nb_content = nb_file.read_text(encoding="utf-8", errors="replace") if nb_file.exists() else ""
                            columns = _extract_source_columns(nb_content) if nb_content else []
                            referenced_tables[val] = {
                                "columns": columns,
                                "notebook": nb_stem,
                                "notebook_content": nb_content,
                            }
        except (json.JSONDecodeError, OSError):
            continue

    # 2c. From UserVar notebooks that set src_tbl_nm via taskValues
    for nb in merged_dir.glob("*UserVar*.py"):
        content = nb.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(
            r'src_tbl_nm\s*=\s*"(\w+)"',
            content,
        ):
            val = m.group(1)
            if val and val.lower() not in created_tables and val not in referenced_tables:
                # Find the corresponding LoadEBAN notebook that uses this SRC_TBL
                loadbean_content = ""
                # The UserVar is e.g., SEQ_VP_CNSNT_FORM_UserVar1 -> LoadEBAN_VP_CNSNT_FORM
                base_name = nb.stem.replace("_UserVar1", "").replace("_UserVar", "")
                base_name = base_name.replace("SEQ_", "LoadEBAN_")
                loadbean_nb = merged_dir / f"{base_name}.py"
                if loadbean_nb.exists():
                    loadbean_content = loadbean_nb.read_text(encoding="utf-8", errors="replace")
                columns = _extract_all_source_columns(loadbean_content) if loadbean_content else []
                referenced_tables[val] = {
                    "columns": columns,
                    "notebook": loadbean_nb.name if loadbean_nb.exists() else nb.name,
                    "notebook_content": loadbean_content,
                }

    if not referenced_tables:
        return {}

    # 3. For each missing table, find the best matching existing table
    # Use ACTUAL CSV headers (not DDL) — DDL may rename columns (e.g., 'ip_id' vs CSV 'aa.ip_id')
    table_columns: dict[str, list[str]] = {}

    # First try CSV headers from the original data files (most accurate)
    manifest_path = output_dir / "_manifest.json"
    if manifest_path.exists():
        with open(manifest_path) as f:
            manifest_data = json.load(f)
        for data_file_path in manifest_data.get("data_files", []):
            data_file = Path(data_file_path)
            if data_file.exists():
                raw_stem = data_file.stem.lower()
                for prefix in ("datalake.", "datatank.", "common_layer.", "datatank_view."):
                    if raw_stem.startswith(prefix):
                        raw_stem = raw_stem[len(prefix):]
                        break
                tbl_name = raw_stem.replace("-", "_").replace(" ", "_").replace(".", "_")
                first_line = data_file.read_text(encoding="utf-8", errors="ignore").split("\n")[0]
                cols = [c.strip().lower() for c in first_line.split(",")]
                table_columns[tbl_name] = cols

    # Fallback: DDL columns (if no CSV headers found)
    if not table_columns and ddl_output.exists():
        for nb in ddl_output.glob("*.py"):
            content = nb.read_text(encoding="utf-8", errors="replace")
            for tm in re.finditer(
                r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+\w+\.\w+\.(\w+)\s*\((.*?)\)",
                content, re.DOTALL | re.IGNORECASE,
            ):
                tbl = tm.group(1).lower()
                cols = [cm.group(1).lower() for cm in re.finditer(r'^\s*(\w+)\s+\w+', tm.group(2), re.MULTILINE)]
                table_columns[tbl] = cols

    # 4. Generate CREATE OR REPLACE TABLE statements in source_schema
    views: dict[str, str] = {}
    for missing_table, info in referenced_tables.items():
        source_cols = info["columns"]
        notebook_content = info.get("notebook_content", "")

        # Extract column alias mapping from notebook transformation.
        # Merge BladeBridge (deterministic) + Switch (LLM) aliases.
        # BladeBridge aliases are the ground truth — they come from DataStage XML
        # metadata and don't vary across LLM runs.
        alias_map = {}
        # First: BladeBridge aliases (deterministic, preferred)
        for bb_stem, bb_map in bb_alias_maps.items():
            if missing_table.lower() in bb_stem.lower() or bb_stem.lower() in missing_table.lower():
                alias_map.update(bb_map)
                break
        # Then: Switch aliases (may override, but BladeBridge provides the base)
        switch_aliases = _extract_column_aliases(notebook_content)
        if switch_aliases:
            # Only add Switch aliases not already in BB map
            for k, v in switch_aliases.items():
                if k not in alias_map:
                    alias_map[k] = v

        # Use alias map to find matching table: check if alias TARGET columns match DDL columns
        best_match = None
        best_overlap = 0
        if alias_map:
            target_col_names = {v.lower() for v in alias_map.values()}
            for tbl_name, tbl_cols in table_columns.items():
                tbl_col_set = {c.lower() for c in tbl_cols}
                overlap = len(target_col_names & tbl_col_set)
                # Use overlap ratio relative to BOTH sets (prefer tight matches)
                ratio = overlap / max(len(target_col_names), 1)
                if ratio > best_overlap:
                    best_overlap = ratio
                    best_match = tbl_name

        # Fallback: try direct column/name matching
        if not best_match:
            best_match = _find_best_matching_table(missing_table, source_cols, table_columns, created_tables)

        if best_match and table_columns.get(best_match):
            target_cols = table_columns[best_match]
            src_table = f"{catalog}.{source_schema}.{best_match}"
            tgt_table = f"{catalog}.{source_schema}.{missing_table}"

            if alias_map and len(source_cols) > 0:
                # Build SELECT with reverse aliases + NULL for missing columns.
                # The UNION ALL pattern in BladeBridge notebooks has a second SELECT
                # that reads actual columns from the source. We alias existing columns
                # and add NULL AS col_name for columns that don't exist in the source
                # (the first SELECT in the UNION uses cast(NULL) for these anyway).
                reverse_map = {v.lower(): k for k, v in alias_map.items()}
                # Also index by base name (after dot) for CSV columns like 'aa.ip_id'
                target_base = {}
                for tc in target_cols:
                    base = tc.split(".")[-1].lower() if "." in tc else tc.lower()
                    target_base[base] = tc

                select_parts = []
                covered = set()
                for tcol in target_cols:
                    tcol_ref = f"`{tcol}`" if "." in tcol else tcol
                    base = tcol.split(".")[-1].lower() if "." in tcol else tcol.lower()
                    source_name = reverse_map.get(base, base)
                    if source_name.lower() != base:
                        select_parts.append(f"{tcol_ref} AS {source_name}")
                        covered.add(source_name.upper())
                    else:
                        select_parts.append(tcol_ref)
                        covered.add(base.upper())
                        covered.add(tcol.upper())

                # Add CAST(NULL AS STRING) for columns needed by UNION ALL but missing from source.
                # Must cast to STRING — bare NULL creates void type which Spark can't query.
                # Scan BOTH BladeBridge raw (deterministic) and Switch (LLM) for all source columns.
                all_needed = _extract_all_source_columns(notebook_content)
                # Also scan the BladeBridge raw output for the same notebook
                for bb_stem in bb_alias_maps:
                    if missing_table.lower() in bb_stem.lower() or bb_stem.lower() in missing_table.lower():
                        bb_nb = bb_output_dir / f"{bb_stem}.py"
                        if bb_nb.exists():
                            bb_cols = _extract_all_source_columns(bb_nb.read_text(encoding="utf-8", errors="replace"))
                            all_needed = all_needed | bb_cols
                        break
                for needed_col in all_needed:
                    if needed_col not in covered:
                        select_parts.append(f"CAST(NULL AS STRING) AS {needed_col}")
                        covered.add(needed_col)

                select_clause = ", ".join(select_parts)
                views[missing_table] = (
                    f"CREATE OR REPLACE TABLE {tgt_table} AS "
                    f"SELECT {select_clause} FROM {src_table}"
                )
            else:
                # No alias mapping — use SELECT * but still add NULL for missing columns
                all_needed = _extract_all_source_columns(notebook_content)
                target_cols_upper = {c.upper() for c in target_cols}
                # Also include base names (after dot)
                for tc in target_cols:
                    target_cols_upper.add(tc.split(".")[-1].upper() if "." in tc else tc.upper())
                missing_cols = [c for c in all_needed if c not in target_cols_upper]
                if missing_cols:
                    null_parts = ", ".join(f"CAST(NULL AS STRING) AS {c}" for c in missing_cols)
                    views[missing_table] = (
                        f"CREATE OR REPLACE TABLE {tgt_table} AS "
                        f"SELECT *, {null_parts} FROM {src_table}"
                    )
                else:
                    views[missing_table] = (
                        f"CREATE OR REPLACE TABLE {tgt_table} AS "
                        f"SELECT * FROM {src_table}"
                    )
        else:
            # No alias match via column overlap — find by name similarity and apply
            # aliases if available. The UNION ALL pattern in notebooks means the second
            # SELECT reads actual columns from the source, so aliases matter.
            fallback = _find_best_matching_table(missing_table, source_cols, table_columns, created_tables)
            if fallback:
                src_table = f"{catalog}.{source_schema}.{fallback}"
                tgt_table = f"{catalog}.{source_schema}.{missing_table}"
                fallback_cols = table_columns.get(fallback, [])

                # Build SELECT with aliases + NULL columns for missing source columns.
                # BladeBridge UNION ALL pattern: second SELECT reads actual columns.
                # We alias existing columns AND add NULL for columns that don't exist
                # in the source table (the notebook's first SELECT casts them to NULL anyway).
                fallback_cols = table_columns.get(fallback, [])
                notebook_content = info.get("notebook_content", "")
                all_needed = _extract_all_source_columns(notebook_content) if notebook_content else []
                reverse_map = {v.lower(): k for k, v in alias_map.items()} if alias_map else {}

                # Map actual source columns: rename if alias exists
                fallback_cols_lower = {c.lower() for c in fallback_cols}
                # Also index by last part after dot (e.g., 'aa.ip_id' -> 'ip_id')
                fallback_base = {}
                for c in fallback_cols:
                    base = c.split(".")[-1] if "." in c else c
                    fallback_base[base.lower()] = c

                select_parts = []
                covered_needed = set()
                for tcol in fallback_cols:
                    tcol_ref = f"`{tcol}`" if "." in tcol or " " in tcol else tcol
                    base = tcol.split(".")[-1].lower() if "." in tcol else tcol.lower()
                    source_name = reverse_map.get(base, base)
                    if source_name.lower() != base:
                        select_parts.append(f"{tcol_ref} AS {source_name}")
                        covered_needed.add(source_name.upper())
                    else:
                        select_parts.append(tcol_ref)
                        covered_needed.add(tcol.upper())
                        covered_needed.add(base.upper())

                # Add NULL for columns needed by the notebook but missing from source
                for needed_col in all_needed:
                    if needed_col not in covered_needed:
                        select_parts.append(f"CAST(NULL AS STRING) AS {needed_col}")
                        covered_needed.add(needed_col)

                select_clause = ", ".join(select_parts)
                views[missing_table] = (
                    f"CREATE OR REPLACE TABLE {tgt_table} AS "
                    f"SELECT {select_clause} FROM {src_table}"
                )
            else:
                # Last resort: use the largest available table (most rows for COUNT/SUM)
                if created_tables:
                    largest = max(created_tables, key=lambda t: len(table_columns.get(t, [])))
                    src_table = f"{catalog}.{source_schema}.{largest}"
                    tgt_table = f"{catalog}.{source_schema}.{missing_table}"
                    views[missing_table] = (
                        f"CREATE OR REPLACE TABLE {tgt_table} AS "
                        f"SELECT * FROM {src_table}"
                    )
                else:
                    views[missing_table] = _generate_empty_table_sql(
                        catalog, source_schema, missing_table, source_cols
                    )

    return views


def _extract_all_source_columns(content: str) -> list[str]:
    """Extract ALL column names referenced in SELECT ... FROM source table patterns.

    Handles two patterns:
    1. UNION ALL second SELECT: reads actual columns from source table
    2. Straight SELECT ... FROM {source_schema}: reads columns directly

    Returns column names that MUST exist in the source table.
    """
    columns = []
    skip_words = {"NULL", "AS", "CAST", "SELECT", "COUNT", "SUM", "TRIM", "OVER",
                  "0", "1", "SUBSTR", "TO_DATE", "CONCAT", "CURRENT_TIMESTAMP"}

    def _parse_select_columns(select_clause: str) -> list[str]:
        cols = []
        for line in select_clause.split("\n"):
            line = line.strip().rstrip(",")
            if not line or line.upper().startswith("CAST(") or line.startswith("--") or line.startswith("#"):
                continue
            # Skip lines that are just numbers or expressions like "0 AS SUMAMT"
            if re.match(r'^\d+\s', line):
                continue
            col_m = re.match(r'(?:TRIM\s*\(\s*)?(\w+)', line, re.IGNORECASE)
            if col_m:
                col_name = col_m.group(1).upper()
                if col_name not in skip_words:
                    cols.append(col_name)
        return cols

    # Pattern 1: UNION ALL second SELECT
    m = re.search(r'UNION\s+ALL\s*\n\s*SELECT\s+(.*?)FROM\s', content, re.DOTALL | re.IGNORECASE)
    if m:
        columns = _parse_select_columns(m.group(1))
        if columns:
            return columns

    # Pattern 2: Straight SELECT ... FROM {source_schema} or {catalog}.{source_schema}
    m = re.search(
        r'spark\.sql\(f"""\s*SELECT\s+(.*?)FROM\s+\{',
        content, re.DOTALL | re.IGNORECASE,
    )
    if m:
        columns = _parse_select_columns(m.group(1))

    return columns


def _extract_column_aliases(content: str) -> dict[str, str]:
    """Extract column alias mappings from notebook transformation code.

    Looks for patterns like:
        col('FORMAPPR_CD').alias('FORM_DTL_ID')
    Returns: {SOURCE_COLUMN: TARGET_COLUMN} e.g. {'FORMAPPR_CD': 'FORM_DTL_ID'}
    """
    alias_map: dict[str, str] = {}
    # Pattern: col('SOURCE_COL').alias('TARGET_COL') or col("SOURCE_COL").alias("TARGET_COL")
    for m in re.finditer(
        r"""col\(\s*['"](\w+)['"]\s*\)\s*\.alias\(\s*['"](\w+)['"]\s*\)""",
        content,
    ):
        source_col = m.group(1)
        target_col = m.group(2)
        if source_col.upper() != target_col.upper():  # Only real renames
            alias_map[source_col.upper()] = target_col.upper()
    return alias_map


def _extract_source_columns(content: str) -> list[str]:
    """Extract column names from SELECT ... FROM in a notebook."""
    columns = []
    # Find the first SELECT block in spark.sql
    m = re.search(r'spark\.sql\(f?"{3}?\s*SELECT\s+(.*?)FROM\s', content, re.DOTALL | re.IGNORECASE)
    if not m:
        return columns

    select_clause = m.group(1)
    # Parse column names from SELECT clause
    for col_match in re.finditer(r'(?:AS\s+)?(\w+)\s*(?:,|$)', select_clause):
        col_name = col_match.group(1).upper()
        # Skip SQL keywords and functions
        if col_name not in {"NULL", "INTEGER", "SMALLINT", "STRING", "TIMESTAMP", "DECIMAL", "AS", "CAST", "COUNT", "SUM", "TRIM", "SELECT"}:
            columns.append(col_name)

    return columns


def _parse_ddl_columns(content: str, catalog: str, schema: str, table_columns: dict) -> None:
    """Parse CREATE TABLE statements and extract column names."""
    for m in re.finditer(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{re.escape(catalog)}\.{re.escape(schema)}\.(\w+)\s*\((.*?)\)",
        content, re.DOTALL | re.IGNORECASE,
    ):
        table_name = m.group(1).lower()
        cols_block = m.group(2)
        cols = []
        for cm in re.finditer(r'^\s*(\w+)\s+\w+', cols_block, re.MULTILINE):
            cols.append(cm.group(1).lower())
        table_columns[table_name] = cols


def _find_best_matching_table(
    missing_table: str,
    source_cols: list[str],
    table_columns: dict[str, list[str]],
    created_tables: set[str],
) -> str | None:
    """Find the created table that best matches the missing source table."""
    best_match = None
    best_score = 0

    source_cols_lower = {c.lower() for c in source_cols}

    for created_name in created_tables:
        created_cols = {c.lower() for c in table_columns.get(created_name, [])}

        if not created_cols:
            # Name similarity fallback
            score = _name_similarity(missing_table.lower(), created_name.lower())
        else:
            # Column overlap score
            overlap = len(source_cols_lower & created_cols)
            score = overlap / max(len(source_cols_lower), 1) * 100

        if score > best_score:
            best_score = score
            best_match = created_name

    return best_match if best_score > 0 else None


def _name_similarity(a: str, b: str) -> float:
    """Simple name similarity (shared substrings)."""
    # Check if one name contains parts of the other
    a_parts = set(a.lower().replace("_", " ").split())
    b_parts = set(b.lower().replace("_", " ").split())
    overlap = len(a_parts & b_parts)
    return overlap / max(len(a_parts | b_parts), 1) * 100


def _generate_empty_table_sql(
    catalog: str,
    schema: str,
    table_name: str,
    columns: list[str],
) -> str:
    """Generate CREATE TABLE for a missing source table with STRING columns."""
    col_defs = ", ".join(f"{col} STRING" for col in columns)
    return f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} ({col_defs})"
