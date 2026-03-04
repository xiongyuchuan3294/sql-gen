# SQL 生成 Skill 增强设计方案

## 1. 背景

当前 `intelligent_sql_generation` skill 具备的能力：
- 根据模板生成 SQL（INSERT、SELECT、DDL 等）
- 支持 HDFS 命令生成
- 通过 MCP 可选调用 Hive 执行

新需求：
- 用户输入表名时，自动发现数据库
- 用户未指定分区时，自动提醒用户指定
- 支持场景化的多步骤 SQL 编排（如数据对比）

---

## 2. 总体架构

```text
┌─────────────────────────────────────────────────────────────┐
│                      用户请求                                │
│  "对比 rrs_aml_risk_rate_current 的 2026-02-01 分区数据"  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              sql_scenario_execution (新 Skill)              │
│  - 场景识别: 数据对比                                        │
│  - 步骤拆分: [移动分区, 统计原分区, 统计temp, 对比差异]    │
│  - 流程编排: 串联每一步                                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│           intelligent_sql_generation (增强)                │
│  - 元数据发现: 表→库、分区字段                              │
│  - 参数校验: 分区值是否完整                                 │
│  - SQL 生成: 调用模板生成每步 SQL                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Hive MCP Server                         │
│  - hive_show_tables: 搜索表                                 │
│  - hive_describe_table: 获取表结构/分区                    │
│  - hive_execute_query: 执行 SQL                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. intelligent_sql_generation 增强设计

### 3.1 能力增强概览

| 能力 | 描述 | 实现方式 |
|------|------|----------|
| 自动发现数据库 | 用户只给表名时，自动查找所在数据库 | 遍历数据库 + 模糊匹配 |
| 自动发现分区字段 | 查询表的分区字段（一级/二级） | DESCRIBE FORMATTED 解析 |
| 分区参数校验 | 分区表未指定分区/缺少二级分区时提醒 | 参数校验逻辑 |
| 非分区表放行 | 非分区表直接生成 SQL，无需分区 | 校验后放行 |

### 3.2 自动发现数据库

#### 3.2.1 工作流程

```
用户输入: "查 rrs_aml_risk_rate_current 的数据量"

Step 1: 检查用户是否已指定数据库
        - 用户已指定 (如 imd_aml_safe.table) → 跳过发现
        - 用户未指定 → 进入发现流程

Step 2: 遍历已知数据库搜索表名
        调用 hive_show_tables 遍历:
        - imd_aml_safe
        - imd_dm_safe
        - imd_rdfs_dm_safe
        - ...

Step 3: 模糊匹配
        - SHOW TABLES LIKE 'rrs_aml_risk_rate_current'
        - 或 SHOW TABLES LIKE '*rrs_aml_risk_rate_current*'

Step 4: 结果处理
        - 唯一匹配 → 使用该数据库
        - 多个匹配 → 返回列表让用户确认
        - 无匹配 → 放弃库名，使用裸表名
```

#### 3.2.2 已知数据库列表

```python
KNOWN_DATABASES = [
    "imd_aml_safe",
    "imd_aml_dm_safe",
    "imd_dm_safe",
    "imd_rdfs_dm_safe",
    "imd_aml300_ads_safe",
    "imd_amlai_ads_safe",
]
# 可从 hive_show_databases 动态获取
```

### 3.3 自动发现分区字段

#### 3.3.1 调用方式

```python
# 调用 MCP
result = hive_describe_table(schema="imd_aml_safe", table_name="rrs_aml_risk_rate_current")

# Hive DESCRIBE FORMATTED 返回格式（分区信息部分）
"""
# Partition Information
# col_name             data_type               comment
ds                    string                  分区日期
hour                   string                  分区小时
```

#### 3.3.2 分区字段解析逻辑

```python
def parse_partition_fields(desc_formatted_output: str) -> list[str]:
    """
    解析 DESCRIBE FORMATTED 输出，提取分区字段

    返回示例: ['ds'] 或 ['ds', 'hour']
    """
    partitions = []
    in_partition_section = False

    for line in output.split('\n'):
        if '# Partition Information' in line:
            in_partition_section = True
            continue
        if in_partition_section:
            if '# ' in line:  # 新的 # 开头表示下一个section
                break
            if line.strip() and not line.startswith('#'):
                cols = line.split()
                if cols:
                    partitions.append(cols[0])  # 第一列是字段名

    return partitions
```

### 3.4 分区参数校验

#### 3.4.1 校验规则

```python
def validate_partition_params(table_metadata: dict, user_partitions: dict) -> ValidationResult:
    """
    校验分区参数

    table_metadata: {
        "is_partitioned": True/False,
        "partition_fields": ["ds", "hour"]
    }

    user_partitions: {
        "ds": "2026-02-01",
        # "hour" 可能缺失
    }
    """
    if not table_metadata["is_partitioned"]:
        # 非分区表，直接通过
        return ValidationResult(valid=True, message=None)

    required_fields = table_metadata["partition_fields"]
    provided_fields = list(user_partitions.keys())

    # 1. 检查是否指定了任何分区
    if not provided_fields:
        return ValidationResult(
            valid=False,
            message=f"该表是分区表，分区字段为: {required_fields}，请指定分区值，如 {required_fields[0]}='2026-02-01'"
        )

    # 2. 检查是否缺少必需的分区字段
    missing = set(required_fields) - set(provided_fields)
    if missing:
        return ValidationResult(
            valid=False,
            message=f"该表有分区字段 {required_fields}，请补充指定: {list(missing)}"
        )

    return ValidationResult(valid=True, message=None)
```

#### 3.4.2 校验结果处理

| 校验结果 | 处理方式 |
|----------|----------|
| valid=True | 正常生成 SQL |
| valid=False | 返回提示信息，要求用户补充参数 |

### 3.5 交互示例

#### 示例1: 分区表 - 用户未指定分区

```
用户: "查 rrs_aml_risk_rate_current 的数据量"

intelligent_sql_generation:
  → 自动发现数据库: imd_aml_safe
  → 查询分区字段: ['ds']
  → 用户未指定分区
  → 返回提示:
    "【参数缺失】该表是分区表，分区字段为 ds，请指定分区值，如 ds='2026-02-01'"
```

#### 示例2: 分区表 - 用户只指定了一级分区

```
用户: "查 rrs_aml_risk_rate_current 在 ds='2026-02-01' 的数据量"

intelligent_sql_generation:
  → 自动发现数据库: imd_aml_safe
  → 查询分区字段: ['ds', 'hour']
  → 用户只指定了 ds
  → 返回提示:
    "【参数缺失】该表有二级分区 hour，请补充指定，如 hour='23'"
```

#### 示例3: 非分区表

```
用户: "查 some_non_partitioned_table 的数据量"

intelligent_sql_generation:
  → 自动发现数据库: imd_aml_safe
  → 查询分区字段: []
  → 非分区表，直接生成 SQL:
    SELECT COUNT(*) FROM imd_aml_safe.some_non_partitioned_table;
```

### 3.6 新增模板

需要补充以下模板以支持数据对比场景：

#### 3.6.1 move_partition (移动分区)

```yaml
# templates/yaml/move_partition.yaml
type: "move_partition"
description: "将分区数据移动到另一个分区（如备份到 temp 分区）"
params:
  table_name: "example_table"          # 表名
  db: "imd_aml_safe"                   # 数据库（可选）
  source_partition: "ds='2026-02-01'"   # 源分区
  target_partition: "ds='2026-02-01-temp'"  # 目标分区
```

```sql
-- templates/sql/move_partition.sql
INSERT OVERWRITE TABLE {{table_name}} PARTITION ({{target_partition}})
SELECT * FROM {{table_name}} PARTITION ({{source_partition}});
```

#### 3.6.2 create_temp_partition (创建 temp 分区)

```yaml
# templates/yaml/create_temp_partition.yaml
type: "create_temp_partition"
description: "为分区表创建空白的 temp 分区"
params:
  table_name: "example_table"
  partition: "ds='2026-02-01-temp'"
```

---

## 4. sql_scenario_execution 设计

### 4.1 定位

- **职责**: 场景化 SQL 编排，将复杂需求拆解为多个 SQL 步骤
- **复用**: 调用 `intelligent_sql_generation` 的模板生成能力
- **不涉及**: 具体的 SQL 语法生成（交给已有 skill）

### 4.2 场景定义

#### 4.2.1 数据对比场景 (data_compare)

```yaml
# scenarios/data_compare.yaml
scenario:
  name: "数据对比场景"
  description: "对比原分区和 temp 分区的数据差异"

  # 需要的参数
  required_params:
    - name: "table_name"
      description: "表名"
    - name: "db"
      description: "数据库名（可选，默认自动发现）"
    - name: "partition"
      description: "分区值，如 ds='2026-02-01'"
    - name: "temp_suffix"
      description: "temp 分区后缀，默认 '-temp'"

  # 执行步骤
  steps:
    - name: "创建 temp 分区并复制数据"
      template: "move_partition"
      params:
        source_partition: "{{partition}}"
        target_partition: "{{partition}}{{temp_suffix}}"

    - name: "统计原分区数据量"
      template: "data_num"
      params:
        partition: "{{partition}}"

    - name: "统计 temp 分区数据量"
      template: "data_num"
      params:
        partition: "{{partition}}{{temp_suffix}}"

    - name: "对比原分区和 temp 分区的差异"
      template: "data_diff"
      params:
        source_partition: "{{partition}}"
        target_partition: "{{partition}}{{temp_suffix}}"
```

#### 4.2.2 数据校验场景 (data_validation)

```yaml
# scenarios/data_validation.yaml
scenario:
  name: "数据校验场景"
  description: "对表进行数据质量校验"

  required_params:
    - name: "table_name"
    - name: "partition"
    - name: "check_types"
      description: "校验类型列表: data_num, null_rate, repeat_check"

  steps:
    - name: "数据量统计"
      condition: "'data_num' in check_types"
      template: "data_num"

    - name: "空值率检查"
      condition: "'null_rate' in check_types"
      template: "null_rate"

    - name: "重复值检查"
      condition: "'repeat_check' in check_types"
      template: "repeat_check"
```

### 4.3 交互流程

```
用户输入:
  "对比 imd_aml_safe.rrs_aml_risk_rate_current 的 2026-02-01 分区数据"

Step 1: 场景识别
        → 识别为: data_compare

Step 2: 参数提取
        - table_name: rrs_aml_risk_rate_current
        - db: imd_aml_safe
        - partition: ds='2026-02-01'
        - temp_suffix: -temp

Step 3: 调用 intelligent_sql_generation 生成各步骤 SQL
        → 步骤1: move_partition
        → 步骤2: data_num (原分区)
        → 步骤3: data_num (temp 分区)
        → 步骤4: data_diff

Step 4: 输出结果
        逐个返回生成的 SQL，用户可选择执行
```

### 4.4 Skill 定义

```yaml
# agent_skills/sql_scenario_execution/SKILL.md
---
name: sql_scenario_execution
description: SQL Scenario Execution Agent for Hive
version: 1.0.0
---

# Role
You are an expert SQL scenario orchestration assistant.
Your role is to:
1. Understand user's complex requirements (e.g., data comparison, data validation)
2. Break down requirements into multiple SQL execution steps
3. Use intelligent_sql_generation skill to generate each step's SQL
4. Present the complete SQL workflow to user

# Capabilities
1. Scenario Recognition: Identify the scenario type from user input
2. Parameter Extraction: Extract table name, partition, db, etc.
3. Step Orchestration: Define steps for each scenario
4. SQL Generation: Delegate to intelligent_sql_generation for SQL generation

# Workflow
1. Parse user input to identify scenario type
2. Extract required parameters from user input
3. Call intelligent_sql_generation to generate SQL for each step
4. Present the complete SQL workflow

# Scenarios
- data_compare: 对比两个分区/表的数据差异
- data_validation: 数据质量校验
```

---

## 5. 实现计划

### 5.1 Phase 1: intelligent_sql_generation 增强

| 序号 | 任务 | 优先级 | 预估工作量 |
|------|------|--------|------------|
| 1.1 | 添加自动发现数据库逻辑 | P0 | 0.5d |
| 1.2 | 添加分区字段发现逻辑 | P0 | 0.5d |
| 1.3 | 添加分区参数校验逻辑 | P0 | 0.5d |
| 1.4 | 新增 move_partition 模板 | P1 | 0.5d |
| 1.5 | 新增 create_temp_partition 模板 | P1 | 0.5d |

### 5.2 Phase 2: sql_scenario_execution 开发

| 序号 | 任务 | 优先级 | 预估工作量 |
|------|------|--------|------------|
| 2.1 | 创建 skill 目录结构 | P0 | 0.5d |
| 2.2 | 实现场景识别逻辑 | P0 | 1d |
| 2.3 | 实现 data_compare 场景 | P0 | 1d |
| 2.4 | 实现 data_validation 场景 | P1 | 1d |

---

## 6. 文件变更清单

### 6.1 intelligent_sql_generation 变更

```
agent_skills/sql_generation/
├── SKILL.md                    # [修改] 增加元数据感知能力说明
├── templates/
│   ├── yaml/
│   │   ├── move_partition.yaml    # [新增]
│   │   └── create_temp_partition.yaml  # [新增]
│   └── sql/
│       ├── move_partition.sql     # [新增]
│       └── create_temp_partition.sql  # [新增]
└── templates/
    └── scenario_orchestrator.py   # [新增] 元数据发现 + 校验逻辑
```

### 6.2 sql_scenario_execution 新建

```
agent_skills/sql_scenario_execution/
├── SKILL.md                    # [新建] Skill 定义
├── scenarios/
│   ├── __init__.py
│   ├── base.py                 # [新建] 场景基类
│   ├── data_compare.py         # [新建] 数据对比场景
│   └── data_validation.py      # [新建] 数据校验场景
└── orchestrator.py             # [新建] 场景编排器
```

---

## 7. 风险与限制

1. **数据库遍历性能**: 遍历所有数据库搜索表可能较慢
   - 缓解: 可配置已知数据库列表，或缓存结果

2. **分区解析兼容性**: 不同 Hive 版本的 DESCRIBE FORMATTED 格式可能有差异
   - 缓解: 需要在多个环境测试兼容性

3. **场景识别准确性**: 场景识别依赖关键词匹配
   - 缓解: 提供场景列表供用户确认
