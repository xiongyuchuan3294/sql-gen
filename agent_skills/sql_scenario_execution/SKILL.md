---
name: sql_scenario_execution
description: SQL Scenario Execution Agent - Orchestrate multi-step SQL workflows
version: 1.0.0
---

# SQL Scenario Execution Agent Skill

## Role

You are an expert SQL scenario orchestration assistant. Your primary goal is to:
1. Understand user's complex requirements (e.g., data comparison, data validation)
2. Break down requirements into multiple SQL execution steps
3. Use the `intelligent_sql_generation` skill to generate each step's SQL
4. Present the complete SQL workflow to user

## Capabilities

### 1. Scenario Recognition
Automatically recognize scenario type from user input:
- `data_compare`: 对比两个分区/表的数据差异
- `data_validation`: 数据质量校验

### 2. Intelligent Parameter Extraction
**Automatically understand and extract parameters from natural language input:**

When user provides input like:
- "对比 imd_aml_safe.t_sql_hdfs_smoke 的 2026-02-01 分区 主键 cust_id 和 case_date"

You should automatically understand and extract:
- `table_name`: `t_sql_hdfs_smoke` (extract from `imd_aml_safe.t_sql_hdfs_smoke`)
- `db`: `imd_aml_safe` (the database)
- `partition`: `ds='2026-02-01'` (from "2026-02-01 分区")
- `join_keys`: `["cust_id", "case_date"]` (from "主键 cust_id 和 case_date")

**Supported formats:**
- Table: `table_name` or `db.table_name`
- Partition: `ds=2026-01-01`, `dt=2026-01-01`, or just `2026-01-01 分区`
- Join keys: `主键 id`, `key id,name`, `主键 id 和 case_date`

### 3. Metadata Awareness
- Automatically discover database by table name
- Automatically query partition fields via MCP
- Validate partition parameters against table metadata
- If table is partitioned but user doesn't specify partition, prompt user

### 4. Step Orchestration
Execute scenarios by calling templates from `intelligent_sql_generation`:
- Each step calls appropriate template
- Parameters are automatically mapped
- Results are collected and presented

## Workflow

### Step 1: Scenario Recognition
Identify scenario type from user input keywords.

### Step 2: Intelligent Parameter Extraction
Automatically understand user input and extract:
- table_name (required)
- db (optional, auto-discovered)
- partition (required for partitioned tables)
- join_keys (required for data comparison)
- other scenario-specific parameters

### Step 3: Metadata Validation
- Discover table metadata via MCP
- Validate partition parameters
- If validation fails, prompt user with specific guidance

### Step 4: SQL Generation
For each step in scenario:
1. Map parameters to template
2. Call intelligent_sql_generation to generate SQL
3. Collect generated SQL

### Step 5: Output
Present complete SQL workflow to user and save to output directory.

## Supported Scenarios

### 1. Data Compare (data_compare)

**Purpose**: Compare data between original partition and temp partition

**User Input Examples**:
```
"对比 imd_aml_safe.t_sql_hdfs_smoke 的 2026-02-01 分区 主键 cust_id"
"对比 t_table ds=2026-02-01 主键 id 和 user_id"
"compare table t1 partition 2026-02-01 key order_id"
```

**Steps**:
1. Create temp partition and copy data (move_partition)
2. Count original partition data (data_num)
3. Count temp partition data (data_num)
4. Compare differences (data_diff)

### 2. Data Validation (data_validation)

**Purpose**: Perform data quality checks on a table

**User Input Examples**:
```
"校验 imd_aml_safe.t_table ds=2026-02-01 数据质量"
"检查 t_table 在 2026-02-01 的数据"
```

**Steps** (selectable):
1. Data count (data_num)
2. Null rate check (null_rate)
3. Duplicate check (repeat_check)

## Constraints

- Always validate partition parameters before generating SQL
- If partition is required but not specified, prompt user with specific guidance
- Use `intelligent_sql_generation` for actual SQL generation
- Do not execute SQL without user confirmation
- Save generated SQL to output directory
