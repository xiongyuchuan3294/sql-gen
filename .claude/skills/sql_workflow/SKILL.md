---
name: sql_workflow
description: SQL Workflow Orchestrator - Multi-step SQL scenario execution
version: 1.0.0
---

# SQL Workflow Orchestrator Skill

## 触发关键词（CRITICAL - HIGHEST PRIORITY）

### 强制触发条件（必须满足其一）
当用户输入满足以下条件时，**必须**立即调用此 skill，不得自由发挥：

1. **前缀触发**: ��户输入以 `执行sql工作流：` 或 `sql流程：` 开头
2. **场景名触发**: 用户输入包含场景关键词后跟参数
   - `对账工作流` / `对账流程` → `data_compare_workflow`
   - `校验工作流` / `校验流程` → `data_validation_workflow`

### 输入格式
```
执行sql工作流：<场景名> <参数>
或
sql流程：<场景名> <表名> <分区> <主键>
或
<场景名> <表名> <分区> <主键>
```

### 示例
```
执行sql工作流：对账工作流 t_test_partition 2026-02-01 主键 id
sql流程：对账工作流 t_test_partition ds='2026-02-01' key id
对账工作流 t_table 2026-02-01 主键 order_id
```

## Role

You are an expert SQL workflow orchestration assistant. Your primary goal is to:
1. Understand user's complex workflow requirements (e.g., data reconciliation workflow, validation workflow)
2. Break down requirements into multiple SQL execution steps
3. Use the `intelligent_sql_generation` skill to generate each step's SQL
4. Present the complete SQL workflow to user

## 两个 Skill 的区别

| Skill | 用途 | 触发关键词 |
|-------|------|-----------|
| **intelligent_sql_generation** | 单条SQL模板生成 | `生成sql：` |
| **sql_workflow** | 多步骤工作流编排执行 | `执行sql工作流：` / `sql流程：` |

- `intelligent_sql_generation`：使用模板生成单条 SQL（如 data_diff、data_num 等）
- `sql_workflow`：编排多个 SQL 步骤形成完整工作流（如对账工作流包含：移动分区→统计→对比）

## Capabilities

### 1. Workflow Recognition
从用户输入中识别工作流类型，支持以下关键词映射：

| 关键词 | 工作流类型 | 说明 |
|--------|-----------|------|
| 对账工作流、对账流程、reconcile | `data_compare_workflow` | 新旧分区对账完整流程 |
| 校验工作流、校验流程、validate workflow | `data_validation_workflow` | 数据质量校验完整流程 |

**优先级**: 关键词匹配优先于意图推断

### 2. Intelligent Parameter Extraction
**Automatically understand and extract parameters from natural language input:**

When user provides input like:
- "对账工作流：imd_aml_safe.t_test_partition 的 2026-02-01 分区 主键 id"

You should automatically understand and extract:
- `table_name`: `t_test_partition` (extract from `imd_aml_safe.t_test_partition`)
- `db`: `imd_aml_safe` (the database)
- `partition`: `ds='2026-02-01'` (from "2026-02-01 分区")
- `join_keys`: `["id"]` (from "主键 id")

**Supported formats:**
- Table: `table_name` or `db.table_name`
- Partition: `ds=2026-01-01`, `dt=2026-01-01`, or just `2026-01-01 分区`
- Join keys: `主键 id`, `key id,name`, `主键 id 和 case_date`

### 3. Metadata Awareness
- **CRITICAL: Automatically discover database by table name**
  - When user provides only table name (without db), use `discover_db_names_by_table()` from `intelligent_sql_generation/scripts/generate.py`
  - Import and call: `from scripts.generate import discover_db_names_by_table`
  - Usage: `dbs = discover_db_names_by_table("t_test_partition", env="local")`
  - If found, use the first database in the list
  - If not found, prompt user to specify the database
- Automatically query partition fields via MCP
- Validate partition parameters against table metadata
- If table is partitioned but user doesn't specify partition, prompt user

### 4. Step Orchestration
Execute workflows by calling templates from `intelligent_sql_generation`:
- Each step calls appropriate template
- Parameters are automatically mapped
- Results are collected and presented

## Workflow

### Step 1: Workflow Recognition
Identify workflow type from user input keywords.

### Step 2: Intelligent Parameter Extraction
Automatically understand user input and extract:
- table_name (required)
- db (optional, auto-discovered)
- partition (required for partitioned tables)
- join_keys (required for data comparison)
- other workflow-specific parameters

### Step 3: Metadata Validation
- **Database Discovery** (when user provides only table name):
  1. Import `discover_db_names_by_table` from `intelligent_sql_generation/scripts/generate.py`
  2. Call: `dbs = discover_db_names_by_table(table_name, env="local")`
  3. If found: use `dbs[0]` as the database
  4. If not found: prompt user "表 {table_name} 未找到，请指定数据库 (如 imd_aml_safe.t_test_partition)"
- Discover table metadata via MCP (describe table, show partitions)
- Validate partition parameters
- If validation fails, prompt user with specific guidance

### Step 4: SQL Generation
For each step in workflow:
1. Map parameters to template
2. Call intelligent_sql_generation to generate SQL
3. Collect generated SQL

### Step 5: Output
Present complete SQL workflow to user and save to output directory.

## Supported Workflows

### 1. Data Compare Workflow (data_compare_workflow)

**Purpose**: 新旧分区对账完整流程

**User Input Examples**:
```
"对账工作流：imd_aml_safe.t_test_partition 的 2026-02-01 分区 主键 id"
"对账工作流：t_table ds=2026-02-01 主键 id 和 user_id"
"sql流程：reconcile table t1 partition 2026-02-01 key order_id"
```

**Steps**:
1. Step1 - 移动分区到临时表 (move_partition)
2. Step2 - 统计原分区数据量 (data_num)
3. Step3 - 统计临时分区数据量 (data_num)
4. Step4 - 对比差异 (data_diff)

### 2. Data Validation Workflow (data_validation_workflow)

**Purpose**: 数据质量校验完整流程

**User Input Examples**:
```
"校验工作流：imd_aml_safe.t_table ds=2026-02-01 数据质量"
"校验流程：检查 t_table 在 2026-02-01 的数据"
```

**Steps** (selectable):
1. Data count (data_num)
2. Null rate check (null_rate)
3. Duplicate check (repeat_check)

## Constraints

### 触发约束（最高优先级）
1. **强制触发**: 用户输入包含 `执行sql工作流`、`sql流程`、`对账工作流`、`校验工作流` 等关键词时，**必须**使用此 skill
2. **不得自由发挥**: 触发后不得改用其他方式生成 SQL，必须按此 skill 的 workflow 执行
3. **参数映射**: 必须按工作流模板的要求映射参数，不得随意更改

### 执行约束
- Always validate partition parameters before generating SQL
- If partition is required but not specified, prompt user with specific guidance
- Use `intelligent_sql_generation` for actual SQL generation
- Do not execute SQL without user confirmation
- Save generated SQL to output directory
