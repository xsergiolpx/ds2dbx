# ds2dbx

**End-to-end IBM DataStage to Databricks migration CLI using Lakebridge.**

Convert an entire DataStage project — DDL, sample data, DataStage XML jobs, shell scripts, and validation — then deploy and run as Databricks Serverless Workflows.

```bash
# Convert all use cases
ds2dbx convert . --passes 1,2,3,4

# Deploy + setup environment + run workflows
ds2dbx deploy . --run-prereqs
```

**Proven results:** 23/23 tasks SUCCESS across 3 real-world patterns (multi-table JOIN, SCD Type 2, mainframe file ingestion).

---

## How it works

DataStage projects contain multiple artifact types beyond XML exports:

```
UseCase/
├── DDL/          Hive/Kudu table definitions
├── Data/         Sample CSV files
├── Datastage/    DataStage XML job exports
├── Shell/        Impala SQL scripts, SCD2 logic, file validation
└── Source/       Raw source files (mainframe extracts)
```

**ds2dbx** runs a 5-pass conversion pipeline:

| Pass | Input | Engine | Output |
|---|---|---|---|
| **1. DDL** | `DDL/*.sql` | Lakebridge Switch (LLM) | Delta Lake CREATE TABLE notebook |
| **2. Data** | `Data/*.csv` + `Source/*` | Template | Data loader notebook + UC Volume upload |
| **3. Transpile** | `Datastage/*.xml` | BladeBridge (rules) → Triage → Switch (LLM) → Post-processing | PySpark notebooks + Workflow JSON |
| **4. Shell** | `Shell/*.sh` | Lakebridge Switch (LLM) | PySpark transformation notebooks |
| **5. Validate** | *(auto-generated)* | Jinja2 template | Pattern-specific validation notebook |

Pass 3 is the most sophisticated — BladeBridge does rule-based XML parsing, triage classifies output as clean or broken (30 known bug patterns), Switch fixes broken notebooks with a custom prompt, and 10 deterministic post-processors fix patterns the LLM handles inconsistently.

### Post-Processing Pipeline

After LLM conversion, ds2dbx applies deterministic fixes:

**Notebook fixes (10):** UserVar conversion, widget injection/defaults, JOB_RCNCL STRING casts/retry/rename, no-op copy notebook replacement, mainframe file delimiter+schema+header fix, RCNCL schema correction.

**Workflow fixes (5):** Sequencer/Abort removal, shell script task wiring, parallel orchestrator, base_parameters syntax conversion, POS_DT from source file.

### Dual Schema Architecture

Source data (CSV-loaded tables) → `source_schema`. Pipeline output → `target_schema`. Prevents collisions when DataStage reads and writes the same table name from different connections.

---

## Prerequisites

1. **Python 3.10+**
2. **Databricks CLI** — authenticated to your workspace
3. **Lakebridge** with BladeBridge and Switch plugins:
   ```bash
   databricks labs install lakebridge
   ```
4. **Workspace:** Unity Catalog enabled + Foundation Model API with `databricks-claude-opus-4-6`

---

## Installation

```bash
git clone https://github.com/xsergiolpx/ds2dbx.git
cd ds2dbx
pip install -e .
ds2dbx version   # Expected: ds2dbx 0.7.0
```

---

## Quick Start

### 1. Configure

```bash
cd my_project/
ds2dbx init
```

Generates `ds2dbx.yml`:

```yaml
databricks:
  profile: DEFAULT
catalog: vn
source_schema: ds2dbx_source
target_schema: ds2dbx_target
workspace:
  base_path: /Workspace/Users/{username}/ds2dbx_output
lakebridge:
  switch_catalog: vn
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

### 2. Verify connectivity

```bash
ds2dbx check
```

### 3. Dry-run scan

```bash
ds2dbx convert "UC1 - my_usecase" --dry-run
```

Shows the folder scan results, detected pattern, and the exact Lakebridge commands that will execute (including custom prompts and switch_config settings).

### 4. Convert

```bash
# Single use case
ds2dbx convert "UC1 - my_usecase" --passes 1,2,3,4

# All use cases with force re-run
ds2dbx convert . --force --passes 1,2,3,4
```

### 5. Verify conversion

```bash
ds2dbx verify "UC1 - my_usecase"
```

Checks: all DDL tables present, column names/types match, DML statements preserved, infrastructure commands removed, Databricks API usage present.

### 6. Deploy + run

```bash
# Deploy all use cases with auto-setup
ds2dbx deploy . --run-prereqs

# Then run workflows via UI or CLI
databricks jobs run-now --job-id <JOB_ID>
```

`--run-prereqs` creates schemas, volumes, runs DDL, loads data, creates source views, and creates partition tables — everything needed for workflows to execute.

### 7. Check status

```bash
ds2dbx status .
```

```
┃ Use Case                             ┃ Pattern        ┃ P1 ┃ P2 ┃ P3 ┃ P4 ┃ P5 ┃
│ UC1 - common_layer.v_cnsnt_kbnk_mstr │ multi_join     │ ✓  │ ✓  │ ✓  │ ✓  │ ✓  │
│ UC2 - datatank_view.vl_ip_x_doc_...  │ scd2           │ ✓  │ ✓  │ ✓  │ ✓  │ ✓  │
│ UC3 - datalake.P_CC_UBILL_TXN        │ file_ingestion │ ✓  │ ✓  │ ✓  │ ✓  │ ✓  │
```

---

## Commands

| Command | Description |
|---|---|
| `ds2dbx init` | Create `ds2dbx.yml` config interactively |
| `ds2dbx check` | Verify prerequisites (CLI, auth, Lakebridge, FMAPI) |
| `ds2dbx convert <path>` | Run conversion passes on a use case |
| `ds2dbx ddl <path>` | Pass 1 only — DDL to Delta Lake |
| `ds2dbx load-data <path>` | Pass 2 only — upload data, generate loader |
| `ds2dbx transpile <path>` | Pass 3 only — BladeBridge + Switch |
| `ds2dbx convert-shell <path>` | Pass 4 only — shell scripts to PySpark |
| `ds2dbx validate <path>` | Pass 5 only — generate validation notebook |
| `ds2dbx verify <path>` | Verify conversion output against source files |
| `ds2dbx deploy <path>` | Upload notebooks + create Serverless Workflows |
| `ds2dbx deploy <path> --run-prereqs` | Deploy + create schema/tables/data/views |
| `ds2dbx status <path>` | Show conversion summary table |

### Common flags

| Flag | Description |
|---|---|
| `--verbose`, `-v` | Detailed output with token counts and job URLs |
| `--force`, `-f` | Re-run completed passes |
| `--passes` | Comma-separated passes to run (e.g., `--passes 1,2,3,4`) |
| `--dry-run` | Show plan with Lakebridge commands without executing |
| `--config`, `-c` | Path to config file (default: `./ds2dbx.yml`) |
| `--profile`, `-p` | Databricks CLI profile override |
| `--run-prereqs` | Auto-setup schema/data/views on deploy |
| `--cluster-id` | Cluster for running prerequisite notebooks |

---

## Supported Conversions

### DDL (Pass 1)

| Source syntax | Action |
|---|---|
| `CREATE EXTERNAL TABLE` | → `CREATE TABLE IF NOT EXISTS` |
| `STORED AS PARQUET / KUDU / ORC` | Removed (Delta default) |
| `ROW FORMAT DELIMITED` / `SERDEPROPERTIES` | Removed |
| `LOCATION 'hdfs://...'` | Removed |
| `PRIMARY KEY (...)` | Removed (preserved as comment) |
| `PARTITION BY HASH (...) PARTITIONS N` | Removed (Kudu-specific) |
| `ENCODING` / `COMPRESSION` | Removed (Kudu column attributes) |
| `PARTITIONED BY (col TYPE)` | Preserved |
| `COMMENT 'desc'` | Preserved |

### Shell Scripts (Pass 4)

| Source pattern | Converted to |
|---|---|
| `impala-shell -q "SQL"` | `spark.sql(f"""SQL""")` |
| Shell variables `$1`, `$VAR` | `dbutils.widgets.text()` / `.get()` |
| `UPDATE ... FROM ... JOIN` | `MERGE INTO ... USING ... ON ...` |
| `datalake.sha256(col)` | `sha2(col, 256)` |
| `kinit`, `ssh`, `INVALIDATE METADATA`, `COMPUTE STATS` | Removed |

### DataStage XML (Pass 3)

BladeBridge + Switch with 30 custom bug fixes:
- `import oracledb` / `SparkContext` / `SparkSession` → removed
- `TEMP_TABLE_*#` placeholders → widget variables
- `CurrentTimestamp()` → `current_timestamp()`
- Connection attributes `lit(CON).ATTR` → widget variables
- `.csv('')` → `.saveAsTable()` with widget variable
- `JOB_RCNCL` reconciliation → STRING casts + retry loop

---

## Output Structure

```
_ds2dbx_output/
└── my_usecase/
    ├── _manifest.json              ← Scan results, file classification
    ├── _status.json                ← Pass completion tracking
    ├── pass1_ddl/output/           ← Delta Lake DDL notebook
    ├── pass2_data/output/          ← Data loader notebook
    ├── pass3_transpile/merged/     ← PySpark notebooks + Workflow JSON
    ├── pass4_shell/output/         ← Shell-converted notebooks
    └── pass5_validate/output/      ← Validation notebook
```

To inspect deployed workflow definitions:

```bash
# View local JSON (pre-deploy)
cat "_ds2dbx_output/my_usecase/pass3_transpile/merged/SEQ_*.json" | python3 -m json.tool

# View deployed definition (post-deploy)
databricks jobs get --job-id <JOB_ID> --output json
```

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `pip install` hangs | `pip install -e . --no-deps --no-build-isolation` or `--index-url https://pypi.org/simple/` |
| `ds2dbx check` fails auth | `databricks auth login --profile DEFAULT` |
| Switch timeout / empty output | Increase `max_fix_attempts` in config; check workspace compute limits |
| Pass skipped ("already completed") | Use `--force` to re-run |
| `TABLE_OR_VIEW_NOT_FOUND` at runtime | Deploy with `--run-prereqs` to auto-create source views |
| `DELTA_FAILED_TO_MERGE_FIELDS` | Stale table — drop it: `DROP TABLE IF EXISTS catalog.schema.table` |
| `ConcurrentAppendException` on JOB_RCNCL | Expected — the retry wrapper handles it automatically |
| LLM forgets imports (`NameError`) | Add missing import manually; post-processor catches most cases |
| Verify error on file validation scripts | Fixed in v0.7.0 — accepts `dbutils` as valid API usage |

---

## License

Apache 2.0
