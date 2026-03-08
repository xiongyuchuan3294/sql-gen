---
name: sql_workflow
description: SQL Workflow Orchestrator - YAML driven multi-step workflow SQL generation
---

# SQL Workflow Skill

## Trigger Rules
Use this skill when the request clearly asks for multi-step SQL generation, for example:
- `run a SQL workflow`
- `reconciliation workflow`
- `validation workflow`
- any request that needs multiple SQL steps orchestrated in one result

Chinese-language workflow trigger phrases remain supported in addition to English phrasing.

## Core Design
1. The model converts natural language into semantic YAML.
2. Python scripts convert that YAML into deterministic workflow SQL.
3. Once YAML exists, SQL generation should not depend on additional LLM reasoning.
4. Reuse helpers from `../intelligent_sql_generation/scripts/generate.py` for metadata discovery, partition handling, join-key validation, and template rendering through the configured shared Hive MCP server session.

## Source of Truth
When files disagree, follow this order:
1. `assets/scenarios/*.yaml`
2. `scripts/config_loader.py`
3. `scripts/orchestrator.py`
4. `../intelligent_sql_generation/assets/templates/sql/*.sql`

## Resource Layout
- Deterministic scripts: `scripts/`
- Workflow definitions: `assets/scenarios/`
- Example semantic YAML: `assets/examples/`
- Runtime outputs: `output/` (generated locally, not committed)

## Semantic YAML Contract
Preferred runtime input:

```yaml
scenario: data_compare
env: local_hs2
params:
  table_name: t_local_hs2_aml300_ads_safe_p_ds_dt
  partition: ds=2026-02-01
  join_keys:
    - cust_id
  temp_suffix: -temp
```

Notes:
- `table_name` supports `db.table` or table-only. Table-only input is preferred if you want the script to discover the best matching database.
- `partition` supports `ds=2026-02-01` or `2026-02-01`.
- `join_keys` supports either a string or a list.

## Execution Workflow
### Step 1: Natural language -> YAML
Extract the workflow type and parameters from user text.

### Step 2: YAML -> SQL
Run:

```bash
python .claude/skills/sql_workflow/scripts/orchestrator.py --yaml <semantic_yaml_path>
```

Two common modes:

```bash
# 1) Generate SQL from natural language and save into output/
python .claude/skills/sql_workflow/scripts/orchestrator.py "Reconciliation workflow for t_xxx on ds=2026-02-01 with primary key id"

# 2) Generate SQL from YAML
python .claude/skills/sql_workflow/scripts/orchestrator.py --yaml .claude/skills/sql_workflow/assets/examples/input_example_data_compare.yaml
python .claude/skills/sql_workflow/scripts/orchestrator.py --yaml .claude/skills/sql_workflow/assets/examples/input_example_data_validation.yaml
```

`--yaml` path support:
- recommended: `.claude/skills/sql_workflow/assets/examples/*.yaml`
- also supports any accessible YAML file path

Optional flags:
- `--scenario data_compare`
- `--env local_hs2`
- `--output <path>`
- `--no-save`

Default save behavior:
- without `--output`, the workflow writes a deterministic SQL filename
- with `--output`, only the SQL file is written to the specified path

### Step 3: Return SQL
Return multi-step SQL in fenced code blocks.
Do not execute SQL unless the user explicitly asks for execution.

## Supported Workflows
### data_compare
Purpose: compare a source partition with its temp partition.

Generated steps:
1. `metadata_probe`
2. `move_partition`
3. `data_num` (source)
4. `data_num` (target)
5. `data_diff`

Scenario definition: `assets/scenarios/data_compare.yaml`

### data_validation
Purpose: validate partition data quality for one table.

Generated steps:
1. `metadata_probe`
2. `data_num`
3. `null_checks`
4. `null_rate`
5. `repeat_check`

Scenario definition: `assets/scenarios/data_validation.yaml`

## Fallback Mode
If YAML is not provided, the script can still parse legacy text input:

```bash
python .claude/skills/sql_workflow/scripts/orchestrator.py "Validation workflow for t_xxx on ds=2026-02-01 with primary key id"
```

For reproducibility, YAML mode is preferred.
