---
name: sql_workflow
description: SQL Workflow Orchestrator - YAML driven multi-step workflow SQL generation
version: 2.0.0
---

# SQL Workflow Skill

## Trigger Rules
Use this skill when user intent includes:
- `执行sql工作流` / `sql流程`
- `对账工作流` / `对账流程`
- `校验工作流` / `校验流程`
- Any request requiring multi-step SQL orchestration

## Core Design (Aligned with intelligent_sql_generation)
1. AI parses natural language into a **semantic YAML input**.
2. Python scripts consume that YAML and deterministically generate final workflow SQL.
3. SQL generation should not depend on extra LLM reasoning after YAML is produced.
4. Reuse helpers from `../intelligent_sql_generation/scripts/generate.py` for:
   - db discovery
   - partition extraction/validation
   - join key validation
   - template rendering

## Source of Truth
When files disagree, use this order:
1. `config/scenarios/*.yaml` (workflow definitions)
2. `config_loader.py` (deterministic engine)
3. `orchestrator.py` (compatibility wrapper)
4. `../intelligent_sql_generation/templates/sql/*.sql` (single-step templates)

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
- `table_name` supports `db.table` or table-only. 推荐只传表名，脚本会自动发现并选择最匹配的库名。
- `partition` supports `ds=2026-02-01` or `2026-02-01` (auto-normalized).
- `join_keys` supports string or list; final engine normalizes to list.

## Execution Workflow

### Step 1: NL -> YAML (AI)
Extract workflow type and parameters from user text.

### Step 2: YAML -> SQL (Python)
Run:

```bash
python .claude/skills/sql_workflow/orchestrator.py --yaml <semantic_yaml_path>
```

Optional:
- `--scenario data_compare`
- `--env local_hs2`
- `--output <path>`
- `--no-save`

Default save behavior:
- If `--output` is not provided, workflow saves to a single deterministic SQL file:
  - `data_compare_<table>_<partition>.sql`

### Step 3: Return SQL
Return generated multi-step SQL in code blocks.
Do not execute SQL unless user explicitly asks to execute.

## Current Supported Workflow

### data_compare
Purpose: compare source partition vs temp partition for one table.

Generated steps:
1. `move_partition`
2. `data_num` (source)
3. `data_num` (temp)
4. `data_diff`

Scenario definition: `config/scenarios/data_compare.yaml`

## Fallback Mode
If YAML is not provided, script supports legacy text input:

```bash
python .claude/skills/sql_workflow/orchestrator.py "对账工作流：t_xxx 的 2026-02-01 分区 主键 id"
```

But YAML mode is preferred for stability and reproducibility.
