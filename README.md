# ds2dbx

**End-to-end IBM DataStage to Databricks migration CLI using Lakebridge.**

Convert an entire DataStage project — DDL, sample data, DataStage XML, shell scripts, and validation — with a single command.

```bash
ds2dbx convert ./my_usecase/
```

---

## How it works

DataStage projects typically contain more than just XML exports. The real ETL logic lives across multiple artifact types:

```
UseCase/
├── DDL/          Hive/Kudu table definitions
├── Data/         Sample CSV files
├── Datastage/    DataStage XML job exports
├── Shell/        Impala SQL scripts, SCD2 logic, file validation
└── source/       Raw source files (mainframe extracts, etc.)
```

**ds2dbx** runs 5 conversion passes, one per artifact type:

| Pass | Input | Tool | Output |
|---|---|---|---|
| **1. DDL** | `DDL/*.sql` | Lakebridge Switch (LLM) | Delta Lake CREATE TABLE notebook |
| **2. Data** | `Data/*.csv` | Template (no LLM) | Data loader notebook + UC Volume upload |
| **3. Transpile** | `Datastage/*.xml` | BladeBridge + Switch | PySpark notebooks + Workflow JSON |
| **4. Shell** | `Shell/*.sh` | Lakebridge Switch (LLM) | PySpark transformation notebooks |
| **5. Validate** | *(auto-generated)* | Jinja2 template | Pattern-specific validation notebook |

The tool auto-detects the folder structure, classifies shell scripts (skips SSH wrappers), and identifies the ETL pattern (multi-table JOIN, SCD Type 2, file ingestion) to generate the right validation checks.

For LLM passes (1 and 4), the tool shows:
- **Estimated input/output tokens** before and after each Switch call (for cost tracking)
- **Clickable job URL** to monitor the Switch job in the Databricks workspace UI

---

## Prerequisites

1. **Python 3.10+**

2. **Databricks CLI** — authenticated to your workspace:
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   databricks auth login --host https://your-workspace.azuredatabricks.net
   ```

3. **Lakebridge** with BladeBridge and Switch plugins:
   ```bash
   databricks labs install lakebridge
   databricks labs lakebridge install-transpile --interactive false
   databricks labs lakebridge install-transpile --include-llm-transpiler true
   ```

4. **Workspace resources:**
   - Unity Catalog enabled
   - Foundation Model API with `databricks-claude-opus-4-6` endpoint in READY state
   - A catalog and schemas for the migration (created in setup below)

---

## Installation

```bash
git clone https://github.com/xsergiolpx/ds2dbx.git
cd ds2dbx
```

Check if dependencies are already available:

```bash
python3 -c "import typer, rich, yaml, jinja2, requests; print('All dependencies found')"
```

If all found — fast install (no network needed):

```bash
pip install -e . --no-deps --no-build-isolation
```

If any missing — full install:

```bash
pip install -e .
```

> **Corporate proxy / VPN issues?** If `pip install` hangs, try:
> ```bash
> pip install -e . --index-url https://pypi.org/simple/
> ```
> Or disconnect from VPN, install, then reconnect.

Verify:

```bash
ds2dbx version
```

---

## Quick Start

### 1. Create a config file

```bash
cd my_project/
ds2dbx init
```

The interactive wizard asks for 7 settings:

| Prompt | Description | Example |
|---|---|---|
| CLI profile | Databricks CLI profile name | `DEFAULT` |
| Unity Catalog name | Target catalog for converted tables | `migration_pilot` |
| Schema name | Target schema for converted tables | `converted` |
| Switch staging catalog | Catalog for Lakebridge staging | `migration_pilot` |
| Switch staging schema | Schema for Lakebridge staging | `lakebridge` |
| Switch staging volume | UC Volume for Switch file staging | `switch_volume` |
| Data upload volume | UC Volume for sample data upload | `sample_data` |
| Foundation model endpoint | LLM model for Switch | `databricks-claude-opus-4-6` |

This generates a complete `ds2dbx.yml` with all required settings.

### 2. Create workspace resources

Run these SQL statements on your workspace (SQL editor or notebook):

```sql
-- Replace with your catalog/schema names from ds2dbx init
CREATE CATALOG IF NOT EXISTS migration_pilot;
CREATE SCHEMA IF NOT EXISTS migration_pilot.converted;
CREATE SCHEMA IF NOT EXISTS migration_pilot.lakebridge;
CREATE VOLUME IF NOT EXISTS migration_pilot.lakebridge.switch_volume;
CREATE VOLUME IF NOT EXISTS migration_pilot.lakebridge.sample_data;
```

### 3. Verify everything is connected

```bash
ds2dbx check
```

All 7 checks should show green checkmarks.

### 4. Convert a use case

```bash
# See what will happen (no execution)
ds2dbx convert ./my_usecase/ --dry-run

# Run all 5 passes
ds2dbx convert ./my_usecase/
```

Example output:
```
Scanning: my_usecase
  Pattern: multi_join
  DDL files: 9
  Data files: 8 + 0 source
  Shell scripts: 2 logic, 2 wrappers (skipped)

Pass 1 (DDL): Converting 9 DDL files...
  LLM input: ~2.4K tokens (source: 1.3K, prompt: 1.1K)
  Switch job triggered (run_id=12345), waiting for completion...
  Job URL: https://your-workspace.azuredatabricks.net/?o=...#job/.../run/12345
  Switch job completed successfully
  LLM output: ~1.5K tokens (1 file(s), 5,821 chars)
  ✓ Pass 1 complete — 1 notebook(s)

Pass 2 (Data): Loading 8 data files...
  ✓ Pass 2 complete — 8 table(s)

Pass 4 (Shell): Converting 2 shell scripts...
  LLM input: ~8.1K tokens (source: 6.9K, prompt: 1.2K)
  Switch job triggered (run_id=12346), waiting for completion...
  Job URL: https://your-workspace.azuredatabricks.net/?o=...#job/.../run/12346
  Switch job completed successfully
  LLM output: ~3.2K tokens (2 file(s), 12,834 chars)
  ✓ Pass 4 complete — 2 converted

Pass 5 (Validate): Generating multi_join validation notebook...
  ✓ Pass 5 complete — 8 checks defined
```

### 5. Verify conversion correctness

```bash
ds2dbx verify ./my_usecase/
```

This compares source files against output — checks all tables present, columns match, DML preserved, infrastructure commands removed.

### 6. Deploy to workspace

```bash
ds2dbx deploy ./my_usecase/

# Or deploy all use cases at once
ds2dbx deploy ./
```

This uploads all converted notebooks to the workspace and creates Databricks Serverless Workflows from BladeBridge-generated job definitions (Pass 3). Each workflow preserves the original DataStage task dependencies — sequential jobs become task chains, parallel jobs fan into sequencer tasks.

### 6b. Deploy with prerequisites (recommended for first run)

```bash
# Deploy + create schema/tables/data + source views
ds2dbx deploy ./my_usecase/ --run-prereqs --cluster-id <YOUR_CLUSTER_ID>

# Auto-detect a running cluster
ds2dbx deploy ./my_usecase/ --run-prereqs
```

With `--run-prereqs`, the tool also:
- Creates the target schema (`CREATE SCHEMA IF NOT EXISTS`)
- Creates a volume for source files (`CREATE VOLUME IF NOT EXISTS`)
- Uploads source files (mainframe extracts, etc.) to the volume
- Runs the DDL notebook on the cluster (creates target tables)
- Runs the data loader notebook on the cluster (loads sample CSV data)
- Auto-detects missing source tables and creates views pointing to loaded data

This makes workflow execution self-contained — all tables, data, and views are ready before workflows run.

Example output:
```
Deploying: my_usecase
  Uploading Pass 1 (DDL): 1 notebook(s)
  Uploading Pass 3 (Transpile): 31 notebook(s)
  Creating 10 workflow(s) + 1 orchestrator(s)
    Created: SEQ_pipeline_A -> https://your-workspace.azuredatabricks.net/#job/12345
    Created orchestrator: SEQ_MASTER -> https://your-workspace.azuredatabricks.net/#job/12347
  Done: 36 notebook(s), 11 workflow(s)

  Running prerequisites on cluster 0714-073622-xr9f5da2...
  Creating schema catalog.schema...
  Creating volume catalog.schema.data...
  Running DDL notebook: all_ddl...
  Running data loader: my_usecase_data_loader...
  Creating 1 source view(s)...
    Creating catalog.schema.V_SOURCE_VIEW
  Setup: schema=True, volume=True, DDL=OK, data=OK, views=1
```

### 7. Check status

```bash
ds2dbx status ./
```

```
┃ Use Case              ┃ Pattern        ┃ P1 ┃ P2 ┃ P3 ┃ P4 ┃ P5 ┃
│ UC1 - pipeline_A      │ multi_join     │ ✓  │ ✓  │ ✓  │ ✓  │ ✓  │
│ UC2 - pipeline_B      │ scd2           │ ✓  │ ✓  │ ✓  │ ✓  │ ✓  │
│ UC3 - pipeline_C      │ file_ingestion │ ✓  │ ✓  │ ✓  │ ✓  │ ✓  │
```

---

## Commands

| Command | Description |
|---|---|
| `ds2dbx init` | Create `ds2dbx.yml` config interactively |
| `ds2dbx check` | Verify prerequisites (CLI, auth, Lakebridge, FMAPI) |
| `ds2dbx convert <path>` | Run all 5 passes on a use case |
| `ds2dbx convert-all <path>` | Convert all use cases in a directory |
| `ds2dbx ddl <path>` | Pass 1 only — DDL to Delta Lake |
| `ds2dbx load-data <path>` | Pass 2 only — upload data, generate loader |
| `ds2dbx transpile <path>` | Pass 3 only — BladeBridge + Switch |
| `ds2dbx convert-shell <path>` | Pass 4 only — shell scripts to PySpark |
| `ds2dbx validate <path>` | Pass 5 only — generate validation notebook |
| `ds2dbx verify <path>` | Verify conversion output against source files |
| `ds2dbx deploy <path>` | Upload notebooks + create Serverless Workflows |
| `ds2dbx deploy <path> --run-prereqs` | Deploy + create schema/tables/data/views on cluster |
| `ds2dbx status <path>` | Show conversion summary table |

### Common flags

| Flag | Description |
|---|---|
| `--config`, `-c` | Path to config file (default: `./ds2dbx.yml`) |
| `--profile`, `-p` | Databricks CLI profile override |
| `--catalog` | Unity Catalog catalog override |
| `--schema` | Unity Catalog schema override |
| `--output`, `-o` | Output directory (default: `./_ds2dbx_output/`) |
| `--verbose`, `-v` | Show detailed output including token counts and job URLs |
| `--force`, `-f` | Re-run completed passes |
| `--passes` | Comma-separated list of passes to run (e.g., `--passes 1,4`) |
| `--dry-run` | Show plan without executing |

---

## Preparing Your Data

### Directory layout

Organize each use case (one ETL pipeline or logical unit) into its own folder with up to 5 subfolders:

```
my_project/
├── ds2dbx.yml                          ← generated by ds2dbx init
│
├── UC_customer_master/                 ← one use case = one folder
│   ├── DDL/                            ← Pass 1: table definitions
│   ├── Shell/                          ← Pass 4: ETL shell scripts
│   ├── Data/                           ← Pass 2: sample data files
│   ├── Datastage/                      ← Pass 3: DataStage XML exports
│   └── source/                         ← Pass 2: raw source files
│
├── UC_transaction_load/                ← another use case
│   ├── DDL/
│   ├── Shell/
│   └── Data/
│
└── UC_scd2_dimension/
    ├── DDL/
    ├── Shell/
    └── Data/
```

**All subfolders are optional.** The tool skips missing ones and runs only the applicable passes. At minimum, you need either `DDL/` or `Shell/` to have something to convert.

### DDL/ — Table definitions (Pass 1)

Place your Hive, Impala, or Kudu `CREATE TABLE` / `CREATE VIEW` statements here.

**Expected format:**
- One `.sql` file per table or view
- Standard Hive/Impala DDL syntax
- Any naming convention works (e.g., `DDL_schema_tablename.sql`, `create_my_table.sql`)

**Supported syntax** (automatically converted or removed):

| Source syntax | What happens |
|---|---|
| `CREATE EXTERNAL TABLE schema.table (...)` | → `CREATE TABLE IF NOT EXISTS catalog.schema.table (...)` |
| `CREATE VIEW schema.view AS SELECT ...` | → `CREATE OR REPLACE VIEW catalog.schema.view AS SELECT ...` |
| `STORED AS PARQUET / KUDU / ORC / TEXTFILE` | Removed (Delta is the default) |
| `ROW FORMAT DELIMITED FIELDS TERMINATED BY ...` | Removed |
| `WITH SERDEPROPERTIES (...)` | Removed |
| `LOCATION 'hdfs://...'` | Removed |
| `TBLPROPERTIES ('kudu.master_addresses'=...)` | Removed |
| `PRIMARY KEY (col1, col2)` | Removed (Delta doesn't support DDL constraints) |
| `PARTITION BY HASH (...) PARTITIONS N` | Removed (Kudu-specific) |
| `ENCODING ... COMPRESSION ...` | Removed (Kudu column attributes) |
| `NOT NULL / NULL` column constraints | Removed |
| `PARTITIONED BY (col TYPE, ...)` | Preserved (Delta supports partition columns) |
| `COMMENT 'description'` on columns/tables | Preserved |
| Column names and data types | Preserved exactly |

**Example DDL file:**
```sql
CREATE EXTERNAL TABLE datalake.customer_dim (
  customer_id BIGINT,
  name STRING,
  segment STRING,
  created_dt TIMESTAMP
)
PARTITIONED BY (ptn_yyyy STRING, ptn_mm STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/customer_dim';
```

### Shell/ — ETL scripts (Pass 4)

Place your shell scripts that contain the actual ETL business logic — SQL executed via `impala-shell`, `beeline`, or `hive` commands.

**Expected format:**
- `.sh`, `.ksh`, or `.bash` files
- Scripts that execute SQL via `impala-shell -q "..."`, `beeline -e "..."`, or `hive -e "..."`
- Any mix of SQL types: INSERT, UPDATE, DELETE, MERGE, SELECT, CREATE

**What gets converted:**

| Source pattern | Converted to |
|---|---|
| `impala-shell -q "SQL"` | `spark.sql("""SQL""")` |
| `beeline -e "SQL"` | `spark.sql("""SQL""")` |
| `hive -e "SQL"` | `spark.sql("""SQL""")` |
| Shell variables `$1`, `$2`, `$VAR` | `dbutils.widgets.text()` / `dbutils.widgets.get()` |
| `UPDATE t SET ... FROM ... JOIN` | `MERGE INTO t USING ... ON ... WHEN MATCHED` |
| `UPSERT INTO target SELECT ...` | `MERGE INTO target USING (...) ON ... WHEN MATCHED / NOT MATCHED` |
| `datalake.sha256(col)` | `sha2(col, 256)` |
| `datalake.md5(col)` | `md5(col)` |
| `from_unixtime(unix_timestamp())` | `current_timestamp()` |
| `strleft(col, n)` / `strright(col, n)` | `left(col, n)` / `right(col, n)` |
| Date variables `$(date +%Y-%m-%d)` | `datetime.now().strftime("%Y-%m-%d")` |

**What gets removed:**

| Source pattern | Reason |
|---|---|
| `kinit -kt /etc/keytab ...` | Kerberos auth — not needed in Databricks |
| `ssh user@host "..."` | Remote execution — not needed |
| `source /path/to/config` | Config file loading — replaced by widgets |
| `hadoop fs` / `hdfs dfs` commands | HDFS file ops — not needed with UC Volumes |
| `INVALIDATE METADATA table` | Impala-specific — not needed |
| `REFRESH table` | Impala-specific — not needed |
| `COMPUTE STATS table` | Impala-specific — Databricks handles stats automatically |
| `SET SYNC_DDL=1` | Impala-specific |
| `SET KUDU_READ_MODE=...` | Kudu-specific |
| `SET hive.exec.dynamic.partition.mode=...` | Hive-specific |

**SSH wrapper scripts** — Scripts that only invoke other scripts via SSH (e.g., `Insert_data.sh`) are automatically detected and skipped. Detection uses:
1. Filename matching against `skip_patterns` in config (default: `Insert_data.sh`, `Insert_data_common.sh`)
2. Heuristic: file is shorter than 20 lines and contains `ssh`

### Data/ — Sample data files (Pass 2)

Place sample data files here for upload to a Unity Catalog Volume. The tool generates a data loader notebook that reads these files into Delta tables.

**Supported formats:**
- CSV (`.csv`)
- TSV (`.tsv`)
- Text files with any delimiter (`.txt`, `.dat`)

**Delimiter auto-detection:** The tool reads the first 4KB of each file and detects the delimiter:
- SOH (`\x01`) — common in Hive/Hadoop exports
- Pipe (`|`)
- Tab (`\t`)
- Comma (`,`) — default fallback

**Naming convention:** Any filename works. The table name is derived from the file stem (e.g., `schema.my_table.csv` → table `schema_my_table`).

**Headers:** The loader notebook uses `option("header", "true")` by default. If your files don't have headers, adjust the generated notebook after conversion.

### Datastage/ — DataStage XML exports (Pass 3)

Place DataStage job exports here for conversion via BladeBridge (deterministic) + Switch (LLM fallback).

**Expected format:**
- `.dsx` files — DataStage XML export format
- `.xml` files — alternative export format
- `.json` files — DataStage JSON exports (newer versions)

**How to export from DataStage Designer:**
1. Right-click the job in the Repository tree
2. Select **Export** → **DataStage Components**
3. Choose XML format and save as `.dsx`

Multiple jobs can be exported into separate files or as a single combined export.

### source/ — Raw source files (Pass 2)

Place raw source files here (mainframe extracts, fixed-width files, etc.) that should be uploaded to the UC Volume alongside the `Data/` files.

These files are uploaded as-is — no parsing or transformation. The data loader notebook does not reference them by default; add manual loading logic if needed.

### Multiple use cases

To convert multiple use cases at once, organize them as sibling directories:

```
my_project/
├── ds2dbx.yml
├── UC_pipeline_A/
│   ├── DDL/
│   ├── Shell/
│   └── Data/
├── UC_pipeline_B/
│   ├── DDL/
│   └── Shell/
└── UC_pipeline_C/
    ├── DDL/
    ├── Shell/
    ├── Data/
    └── Datastage/
```

The tool auto-discovers use case folders when you point it at the parent:

```bash
# Convert all use cases
ds2dbx convert-all my_project/

# Dry-run scan all
ds2dbx convert my_project/UC_pipeline_A --dry-run
ds2dbx convert my_project/UC_pipeline_B --dry-run

# Status for all
ds2dbx status my_project/

# Verify all
ds2dbx verify my_project/
```

Discovery looks for subdirectories containing `Shell/`, `DDL/`, `Datastage/`, or `Data/` subfolders, or directories whose names start with `UC`.

### ETL pattern detection

The tool inspects shell script content to auto-detect the ETL pattern, which determines the validation template used in Pass 5:

| Pattern | Detection criteria | Validation checks generated |
|---|---|---|
| `scd2` | Shell contains SCD2 keywords (`LAST_VRSN_F`, `VLD_FM_DT`, `VLD_TO_DT`) | Version integrity, one current record per key, date continuity |
| `file_ingestion` | Shell contains file validation keywords (`HEADER`, `FOOTER`, `PIPE_COUNT`, `HEAD_FOOT`) | Record count vs footer, partition correctness, no header/footer rows in data |
| `multi_join` | DDL has >4 files AND shell contains `JOIN` | Row count per table, lookup completeness, NULL keys, audit columns |
| `generic` | None of the above | Row count, schema match, sample data spot check |

---

## Output Structure

```
_ds2dbx_output/
└── my_usecase/
    ├── _manifest.json            ← Scan results, file classification
    ├── _status.json              ← Pass completion tracking
    │
    ├── pass1_ddl/
    │   ├── input/all_ddl.py      ← Concatenated DDL (sent to Switch)
    │   └── output/all_ddl.py     ← Delta Lake DDL notebook
    │
    ├── pass2_data/
    │   └── output/..._data_loader.py   ← Generated loader notebook
    │
    ├── pass3_transpile/
    │   ├── bladebridge_output/   ← Raw BladeBridge notebooks + workflows
    │   ├── triage_report.json    ← Clean vs broken classification
    │   ├── switch_output/        ← Fixed notebooks
    │   └── merged/               ← Final: clean + fixed + workflows
    │
    ├── pass4_shell/
    │   ├── input/                ← Shell scripts with DDL context
    │   └── output/               ← Converted PySpark notebooks
    │
    └── pass5_validate/
        └── output/..._validation.py  ← Pattern-specific validation notebook
```

---

## Configuration

### Generated by `ds2dbx init`

```yaml
databricks:
  profile: DEFAULT
catalog: migration_pilot
schema: converted
workspace:
  base_path: /Workspace/Users/{username}/ds2dbx_output
lakebridge:
  switch_catalog: migration_pilot
  switch_schema: lakebridge
  switch_volume: switch_volume
  data_volume: sample_data
  foundation_model: databricks-claude-opus-4-6
  target_technology: PYSPARK
  concurrency: 4
  max_fix_attempts: 5
prompts:
  strategy: custom
shell_scripts:
  skip_patterns:
  - Insert_data.sh
  - Insert_data_common.sh
  skip_heuristic: true
data_loading:
  default_delimiter: auto
  infer_schema: true
  encoding: utf-8
```

### Config resolution order

1. CLI flags (highest priority)
2. `ds2dbx.yml` in current directory
3. `~/.ds2dbx/config.yml`
4. Built-in defaults

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `pip install` hangs on corporate network | Use `pip install -e . --no-deps --no-build-isolation` if deps already installed, or `--index-url https://pypi.org/simple/` |
| `ds2dbx check` fails on auth | Run `databricks auth login --profile DEFAULT` |
| `ds2dbx check` fails on Lakebridge | Run `databricks labs install lakebridge` |
| Switch job completes but no output | Ensure `max_fix_attempts` is 5+ in config and `prompts.strategy` is `custom` |
| BladeBridge crashes on large XML | Apply the large file patch (see migration guide) |
| UC Volume upload fails | Verify the volume exists: `databricks volumes list <catalog> <schema>` |
| Pass skipped (already completed) | Use `--force` to re-run: `ds2dbx convert ./uc --force` |
| Wrong pattern detected | Override with the right pass: `ds2dbx validate ./uc --pattern scd2` |

---

## License

Apache 2.0
