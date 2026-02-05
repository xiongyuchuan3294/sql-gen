# 智能SQL生成Agent方案设计

## 一、现状分析

### 1.1 当前问题

根据项目调研，现有 `generate_sql` 文件夹中的SQL生成脚本存在以下问题：

| 问题 | 描述 | 影响 |
|------|------|------|
| 硬编码严重 | 每个SQL操作需要编写专门的Python脚本 | 开发成本高，维护困难 |
| 缺乏灵活性 | 新增表或修改表结构需要修改代码 | 适应性差 |
| 无统一规范 | 不同脚本风格不统一 | 代码质量参差不齐 |
| 可复用性差 | 相同逻辑在不同脚本中重复实现 | 效率低下 |

### 1.2 参考资源

- **SQL语法文档**: `ai_gen_data/SQL语法和hdfs命令.md` (1.1MB)
- **支持数据库**: Hive (主要)、MySQL
- **现有工具**: `generate_hdfs_commands.py`、数据生成器、表信息获取工具

## 二、方案选型

### 2.1 技术选型对比

| 方案 | 优点 | 缺点 | 推荐度 |
|------|------|------|--------|
| **纯Agent Skill** | 智能程度高，理解自然语言 | 成本高、速度慢、可控性差 | ⭐⭐⭐ |
| **模板引擎** | 速度快、可控性强、成本低 | 灵活性受限，需预定义模板 | ⭐⭐⭐⭐ |
| **混合方案(推荐)** | 兼具灵活性和可控性，成本适中 | 架构相对复杂 | ⭐⭐⭐⭐⭐ |
| **代码生成器** | 生成可执行代码，功能强大 | 需要编译执行步骤 | ⭐⭐⭐ |

### 2.2 最终方案：配置驱动的混合SQL生成器

采用 **模板引擎 + 语义理解 + 规则验证** 的混合架构：

```
用户输入(自然语言/配置)
       ↓
   意图识别层
       ↓
   参数提取层
       ↓
   SQL模板引擎
       ↓
   语法验证层
       ↓
   安全检查层
       ↓
   输出SQL
```

## 三、系统架构设计

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                     SQL智能生成系统                          │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  CLI入口     │    │  API接口     │    │  Web UI      │  │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘  │
│         │                    │                    │          │
│         └────────────────────┼────────────────────┘          │
│                              ↓                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              输入解析与意图识别层                     │    │
│  │  - 自然语言解析 (可选AI模块)                          │    │
│  │  - 结构化配置解析 (JSON/YAML)                        │    │
│  │  - 命令行参数解析                                     │    │
│  └──────────────────────┬──────────────────────────────┘    │
│                         ↓                                   │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                  核心SQL生成引擎                      │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │    │
│  │  │  模板管理器  │  │  参数构建器  │  │  SQL组装器  │ │    │
│  │  └──────────────┘  └──────────────┘  └────────────┘ │    │
│  └──────────────────────┬──────────────────────────────┘    │
│                         ↓                                   │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                    验证与优化层                       │    │
│  │  - SQL语法验证 (基于Hive/MySQL语法规则)              │    │
│  │  - 安全检查 (敏感字段、操作范围)                      │    │
│  │  - 性能优化建议 (分区、LIMIT)                         │    │
│  └──────────────────────┬──────────────────────────────┘    │
│                         ↓                                   │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                    执行与输出层                       │    │
│  │  - 直接输出SQL文件                                   │    │
│  │  - 执行SQL (可选)                                    │    │
│  │  - 生成HDFS命令 (联动)                               │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                               │
└───────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                      支撑模块                                 │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ 配置管理     │  │ 模板库       │  │ 表结构缓存   │      │
│  │ - 数据源配置 │  │ - INSERT模板 │  │ - 字段信息   │      │
│  │ - 环境配置   │  │ - UPDATE模板 │  │ - 分区信息   │      │
│  │ - 规则配置   │  │ - ALTER模板  │  │ - 数据类型   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ 日志系统     │  │ 工具集成     │  │ 测试数据生成 │      │
│  │ - 操作日志   │  │ - HDFS命令   │  │ (现有模块)   │      │
│  │ - SQL历史    │  │ - 表差异检查 │  │              │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└───────────────────────────────────────────────────────────────┘
```

### 3.2 目录结构设计

```
sql-gen/
├── core/                          # 核心引擎模块
│   ├── __init__.py
│   ├── sql_generator.py          # SQL生成器主类
│   ├── template_manager.py       # 模板管理器
│   ├── param_builder.py          # 参数构建器
│   ├── sql_assembler.py          # SQL组装器
│   └── base_template.py          # 模板基类
│
├── validators/                    # 验证模块
│   ├── __init__.py
│   ├── syntax_validator.py       # 语法验证器
│   ├── security_validator.py     # 安全检查器
│   └── performance_validator.py  # 性能检查器
│
├── templates/                     # SQL模板库
│   ├── hive/                     # Hive SQL模板
│   │   ├── insert/
│   │   │   ├── insert_into.yaml
│   │   │   ├── insert_overwrite.yaml
│   │   │   └── insert_select.yaml
│   │   ├── update/
│   │   │   ├── update_basic.yaml
│   │   │   └── update_conditional.yaml
│   │   ├── alter/
│   │   │   ├── add_partition.yaml
│   │   │   ├── drop_partition.yaml
│   │   │   └── modify_columns.yaml
│   │   └── select/
│   │       ├── select_basic.yaml
│   │       └── select_join.yaml
│   └── mysql/                    # MySQL SQL模板 (可选)
│       └── ...
│
├── config/                        # 配置文件
│   ├── database.yaml             # 数据库配置
│   ├── rules.yaml                # 生成规则配置
│   └── security.yaml             # 安全规则配置
│
├── schema/                        # 表结构定义
│   ├── table_schemas.json        # 表结构JSON
│   └── schema_loader.py          # 结构加载器
│
├── parsers/                       # 输入解析器
│   ├── __init__.py
│   ├── config_parser.py          # 配置文件解析器
│   ├── natural_parser.py         # 自然语言解析器 (可选AI)
│   └── cli_parser.py             # 命令行解析器
│
├── outputs/                       # 输出模块
│   ├── __init__.py
│   ├── sql_writer.py             # SQL文件写入器
│   └── executor.py               # SQL执行器 (可选)
│
├── utils/                         # 工具模块
│   ├── __init__.py
│   ├── logger.py                 # 日志工具
│   ├── file_utils.py             # 文件工具
│   └── date_utils.py             # 日期工具
│
├── tests/                         # 测试模块
│   ├── test_generator.py
│   ├── test_validators.py
│   └── templates/                # 测试用模板
│
├── cli.py                         # CLI入口
└── api.py                         # API入口 (可选)
```

## 四、核心功能设计

### 4.1 SQL模板系统

模板采用YAML格式定义，结构化且易于维护：

#### 示例：INSERT INTO模板

```yaml
# templates/hive/insert/insert_into.yaml
template:
  name: "insert_into"
  description: "向表中插入数据"
  category: "DML"

  # SQL模板
  sql: |
    INSERT INTO {table}
    PARTITION ({partition_clause})
    {values_or_select}

  # 参数定义
  parameters:
    table:
      type: "string"
      required: true
      description: "目标表名"
      validation: "^[a-z][a-z0-9_]*$"

    partition_clause:
      type: "object"
      required: true
      description: "分区字段键值对"
      example: "ds='2025-01-01', dt='lqt'"

    values_or_select:
      type: "choice"
      required: true
      options:
        - "values"
        - "select"
      description: "VALUES子句或SELECT子句"

    values:
      type: "array"
      required_when: "values_or_select == 'values'"
      description: "值列表"

    select:
      type: "string"
      required_when: "values_or_select == 'select'"
      description: "SELECT查询语句"

  # 自动添加的规则
  auto_rules:
    - "add_limit_if_missing"
    - "check_partition_exists"
    - "verify_columns"

  # 示例
  examples:
    - description: "插入单条数据"
      input:
        table: "my_table"
        partition: {ds: "2025-01-01"}
        values: [[1, "data1"]]
      output: |
        INSERT INTO my_table
        PARTITION (ds='2025-01-01')
        VALUES (1, 'data1')
```

#### 示例：ALTER TABLE模板

```yaml
# templates/hive/alter/add_partition.yaml
template:
  name: "add_partition"
  description: "为表添加新分区"
  category: "DDL"

  sql: |
    ALTER TABLE {table}
    ADD PARTITION ({partition_spec});

  parameters:
    table:
      type: "string"
      required: true

    partition_spec:
      type: "object"
      required: true
      description: "分区规格"
```

### 4.2 配置驱动设计

用户通过配置文件描述需求，无需编写代码：

#### 示例配置文件

```yaml
# config/jobs/daily_test_data.yaml
job:
  name: "daily_test_data_generation"
  description: "每日测试数据生成任务"

  # 数据库连接
  database:
    type: "hive"
    connection: "default"

  # 操作列表
  operations:
    # 操作1: 添加分区
    - type: "alter.add_partition"
      table: "rrs_aml_base_outer_trans_fact"
      partition:
        ds: "2025-01-01"
        dt: "lqt"

    # 操作2: 插入数据
    - type: "insert.insert_select"
      table: "my_table"
      partition:
        ds: "2025-01-01"
      select:
        columns:
          - name: "id"
            expression: "CASE WHEN id = 4 THEN 5 ELSE 4 END"
          - name: "name"
            source_column: "name"
          - name: "class"
            source_column: "class"
        from: "imd_aml_safe.my_table"
        where:
          ds: "2024-04-30"
        limit: 1000
```

### 4.3 语法验证器设计

基于SQL语法规则进行验证：

```python
# validators/syntax_validator.py (概念设计)

class HiveSyntaxValidator:
    """Hive SQL语法验证器"""

    # 语法规则库
    SYNTAX_RULES = {
        'insert_into': {
            'required_keywords': ['INSERT', 'INTO'],
            'optional_keywords': ['PARTITION', 'OVERWRITE'],
            'structure': r'INSERT\s+(INTO|OVERWRITE)\s+TABLE?\s+\w+'
        },
        'alter_table': {
            'required_keywords': ['ALTER', 'TABLE'],
            'structure': r'ALTER\s+TABLE\s+\w+\s+(ADD|DROP)\s+PARTITION'
        }
    }

    def validate(self, sql: str, operation_type: str) -> ValidationResult:
        """验证SQL语法"""
        # 1. 基础结构检查
        # 2. 关键字检查
        # 3. 括号匹配检查
        # 4. 分区语法检查
        # 5. 返回验证结果
```

### 4.4 安全检查设计

```yaml
# config/security.yaml
security_rules:
  # 敏感字段检查
  sensitive_fields:
    - pattern: ".*(webank|微众).*(user|customer|account).*"
      action: "block"
      message: "禁止包含敏感字段"

  # 操作范围限制
  operation_limits:
    - type: "INSERT"
      max_rows: 100000
      require_partition: true
      require_limit: true

    - type: "UPDATE"
      require_where: true
      require_limit: true

    - type: "DELETE"
      require_confirmation: true

  # 生产环境保护
  production_protection:
    enabled: true
    protected_databases:
      - "prod_*"
      - "production_*"
    require_additional_auth: true
```

## 五、工作流程设计

### 5.1 标准工作流程

```
1. 用户输入
   ↓
   ├─→ 配置文件 (config/jobs/*.yaml)
   ├─→ 命令行参数
   └─→ 自然语言描述 (可选AI解析)
   ↓
2. 意图识别
   - 确定操作类型 (INSERT/UPDATE/ALTER/...)
   - 确定目标表
   - 确定操作参数
   ↓
3. 模板匹配
   - 根据操作类型选择对应模板
   - 加载模板定义
   ↓
4. 参数构建
   - 合并用户参数和默认值
   - 类型转换和格式化
   - 处理条件参数
   ↓
5. SQL组装
   - 应用模板
   - 填充参数
   - 添加自动规则
   ↓
6. 验证
   - 语法验证
   - 安全检查
   - 性能检查
   ↓
7. 输出
   - 生成SQL文件
   - 可选执行
   - 生成报告
```

### 5.2 使用示例

#### 方式1: 配置文件 (推荐)

```bash
# 1. 创建配置文件
cat > my_job.yaml << EOF
job:
  name: "test_data_insert"
  operations:
    - type: "insert.insert_select"
      table: "test_table"
      partition:
        ds: "$TODAY"
      select:
        columns: "*"
        from: "source_table"
        where:
          status: "active"
        limit: 1000
EOF

# 2. 生成SQL
python cli.py generate -c my_job.yaml -o output.sql

# 3. 预览生成的SQL
cat output.sql
```

#### 方式2: 命令行 (快速操作)

```bash
# 添加分区
python cli.py alter add-partition \
  --table my_table \
  --partition ds=2025-01-01 \
  --partition dt=lqt \
  --output output.sql

# 插入数据
python cli.py insert \
  --table my_table \
  --partition ds=2025-01-01 \
  --select "SELECT * FROM source WHERE ds='2025-01-01' LIMIT 1000" \
  --output output.sql
```

#### 方式3: Python API (程序集成)

```python
from core.sql_generator import SQLGenerator

# 创建生成器
gen = SQLGenerator(database_type='hive')

# 生成SQL
sql = gen.generate(
    operation='insert_into',
    table='my_table',
    partition={'ds': '2025-01-01'},
    source_table='source_table',
    where_clause="status = 'active'",
    limit=1000
)

# 保存或执行
gen.save(sql, 'output.sql')
# 或 gen.execute(sql)  # 可选执行
```

## 六、技术实现要点

### 6.1 模板引擎选择

| 方案 | 优点 | 缺点 | 推荐度 |
|------|------|------|--------|
| Jinja2 | 功能强大、生态成熟 | 语法相对复杂 | ⭐⭐⭐⭐⭐ |
| string.Template | 简单轻量 | 功能有限 | ⭐⭐⭐ |
| 自研 | 完全可控 | 开发成本高 | ⭐⭐ |

**推荐**: 使用Jinja2作为模板引擎

### 6.2 配置文件格式

推荐使用YAML格式：
- 层次清晰
- 注释友好
- 类型支持好
- Python生态支持良好 (PyYAML)

### 6.3 数据源集成

```yaml
# config/database.yaml
databases:
  hive_test:
    type: "hive"
    host: "localhost"
    port: 10000
    auth: "kerberos"

  mysql_test:
    type: "mysql"
    host: "localhost"
    port: 3306
    user: "${MYSQL_USER}"
    password: "${MYSQL_PASSWORD}"
```

### 6.4 表结构获取

```python
# schema/schema_loader.py (概念设计)

class SchemaLoader:
    """表结构加载器"""

    def get_table_schema(self, table: str, database: str = None) -> TableSchema:
        """获取表结构"""
        # 1. 尝试从缓存获取
        # 2. 查询数据库元数据
        # 3. 解析并返回

    def get_partition_info(self, table: str) -> PartitionInfo:
        """获取分区信息"""

    def refresh_cache(self):
        """刷新缓存"""
```

## 七、自然语言到模板转换机制设计

### 7.1 设计目标

将用户的自然语言描述自动转换为SQL模板配置，实现：

| 目标 | 描述 | 示例 |
|------|------|------|
| 意图识别 | 识别用户想要执行的SQL操作类型 | "添加分区" → ALTER TABLE ADD PARTITION |
| 实体提取 | 从自然语言中提取关键参数 | "my_table表" → table: "my_table" |
| 槽位填充 | 将提取的实体映射到模板参数 | ds='2025-01-01' → partition: {ds: "2025-01-01"} |
| 歧义处理 | 处理不确定或多义的情况 | 询问用户确认或提供选项 |

### 7.2 转换架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    自然语言转换引擎                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  用户输入                                                          │
│  "给my_table表添加ds=2025-01-01的分区"                            │
│    ↓                                                              │
│  ┌─────────────────────────────────────────────────────┐        │
│  │              1. 预处理层                              │        │
│  │  - 文本标准化 (繁简转换、全角半角)                   │        │
│  │  - 分词与词性标注                                    │        │
│  │  - 去除停用词                                        │        │
│  └────────────────────┬────────────────────────────────┘        │
│                       ↓                                          │
│  ┌─────────────────────────────────────────────────────┐        │
│  │              2. 意图识别层                            │        │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │        │
│  │  │  关键词匹配  │  │  模式匹配    │  │  语义分析  │ │        │
│  │  │  (规则引擎)  │  │  (正则/模板) │  │  (可选AI)  │ │        │
│  │  └──────────────┘  └──────────────┘  └────────────┘ │        │
│  │                      ↓                                  │        │
│  │              操作类型: alter.add_partition            │        │
│  └────────────────────┬────────────────────────────────┘        │
│                       ↓                                          │
│  ┌─────────────────────────────────────────────────────┐        │
│  │              3. 实体提取层                            │        │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │        │
│  │  │  表名识别    │  │  分区识别    │  │  条件识别  │ │        │
│  │  │  my_table    │  │  ds=...      │  │  WHERE/... │ │        │
│  │  └──────────────┘  └──────────────┘  └────────────┘ │        │
│  └────────────────────┬────────────────────────────────┘        │
│                       ↓                                          │
│  ┌─────────────────────────────────────────────────────┐        │
│  │              4. 槽位填充层                            │        │
│  │  - 类型转换与验证                                    │        │
│  │  - 默认值填充                                        │        │
│  │  - 引用表结构获取字段信息                            │        │
│  └────────────────────┬────────────────────────────────┘        │
│                       ↓                                          │
│  ┌─────────────────────────────────────────────────────┐        │
│  │              5. 歧义处理层                            │        │
│  │  - 缺失参数: 询问用户或使用默认值                    │        │
│  │  - 多义情况: 提供选项供用户选择                      │        │
│  │  - 交互式确认                                        │        │
│  └────────────────────┬────────────────────────────────┘        │
│                       ↓                                          │
│              结构化配置 (YAML格式)                               │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

### 7.3 意图识别设计

#### 7.3.1 意图分类体系

```yaml
# config/intent_mapping.yaml
intents:
  # DDL操作
  alter:
    keywords: ["添加", "新增", "创建", "删除", "删除分区", "drop", "add", "alter"]
    sub_intents:
      add_partition:
        keywords: ["分区", "partition", "加分区"]
        template: "alter/add_partition.yaml"
      drop_partition:
        keywords: ["删除分区", "drop", "清空分区"]
        template: "alter/drop_partition.yaml"
      modify_columns:
        keywords: ["修改列", "增加列", "删除列", "改字段"]
        template: "alter/modify_columns.yaml"

  # DML操作
  insert:
    keywords: ["插入", "导入", "insert", "写入", "添加数据"]
    sub_intents:
      insert_into:
        keywords: ["insert into", "插入到"]
        template: "insert/insert_into.yaml"
      insert_overwrite:
        keywords: ["覆盖", "overwrite", "重写"]
        template: "insert/insert_overwrite.yaml"
      insert_select:
        keywords: ["从...查询", "select", "查询插入"]
        template: "insert/insert_select.yaml"

  update:
    keywords: ["更新", "修改", "update", "改数据"]
    template: "update/update_basic.yaml"

  delete:
    keywords: ["删除", "delete", "清除"]
    template: "delete/delete_basic.yaml"

  select:
    keywords: ["查询", "select", "查看", "获取", "检索"]
    template: "select/select_basic.yaml"
```

#### 7.3.2 意图识别算法

```python
# parsers/intent_recognizer.py

from typing import Optional, List, Tuple
import re

class IntentRecognizer:
    """意图识别器"""

    def __init__(self, intent_config_path: str):
        self.intent_config = self._load_config(intent_config_path)

    def recognize(self, text: str) -> Tuple[str, Optional[str], float]:
        """
        识别用户意图

        返回: (主意图, 子意图, 置信度)
        例如: ("alter", "add_partition", 0.95)
        """
        text_lower = text.lower().strip()

        # 1. 关键词匹配 (快速路径)
        keyword_result = self._keyword_match(text_lower)
        if keyword_result[2] > 0.8:  # 置信度阈值
            return keyword_result

        # 2. 模式匹配 (正则表达式)
        pattern_result = self._pattern_match(text_lower)
        if pattern_result[2] > keyword_result[2]:
            return pattern_result

        # 3. 语义分析 (可选AI模块)
        # semantic_result = self._semantic_analysis(text)
        # if semantic_result[2] > threshold:
        #     return semantic_result

        # 返回最佳匹配
        return max([keyword_result, pattern_result], key=lambda x: x[2])

    def _keyword_match(self, text: str) -> Tuple[str, Optional[str], float]:
        """基于关键词的意图匹配"""
        scores = []

        for intent, intent_config in self.intent_config['intents'].items():
            # 检查主意图关键词
            main_keywords = intent_config.get('keywords', [])
            main_score = sum(1 for kw in main_keywords if kw in text)

            # 检查子意图
            if 'sub_intents' in intent_config:
                for sub_intent, sub_config in intent_config['sub_intents'].items():
                    sub_keywords = sub_config.get('keywords', [])
                    sub_score = sum(1 for kw in sub_keywords if kw in text)

                    if sub_score > 0:
                        total_score = (main_score + sub_score * 2) / (len(main_keywords) + len(sub_keywords) * 2)
                        scores.append((intent, sub_intent, min(total_score, 1.0)))

        return max(scores) if scores else ("unknown", None, 0.0)

    def _pattern_match(self, text: str) -> Tuple[str, Optional[str], float]:
        """基于模式匹配的意图识别"""
        patterns = {
            'alter.add_partition': [
                r'(添加|新增|add).*分区.*表\s*(\w+)',
                r'表\s*(\w+).*添加.*分区',
                r'alter\s+table\s+(\w+).*add\s+partition'
            ],
            'insert.insert_select': [
                r'从\s*(\w+).*插入.*到\s*(\w+)',
                r'insert\s+into\s+(\w+).*select',
                r'(查询|select).*插入到\s*(\w+)'
            ],
            # 更多模式...
        }

        for intent, pattern_list in patterns.items():
            for pattern in pattern_list:
                if re.search(pattern, text):
                    main_intent, sub_intent = intent.split('.')
                    return (main_intent, sub_intent, 0.9)

        return ("unknown", None, 0.0)
```

### 7.4 实体提取设计

#### 7.4.1 实体类型定义

```yaml
# config/entity_types.yaml
entity_types:
  table_name:
    patterns:
      - r'表\s*([a-z][a-z0-9_]*)'
      - r'into\s+([a-z][a-z0-9_]*)'
      - r'from\s+([a-z][a-z0-9_]*)'
      - r'表名[:：]\s*([a-z][a-z0-9_]*)'
    validators:
      - type: "regex"
        pattern: "^[a-z][a-z0-9_]{2,50}$"
        message: "表名必须以小写字母开头，只能包含小写字母、数字和下划线"

  partition:
    patterns:
      - r'分区\s*[:：]?\s*([a-z]+)=([\'"]?[^,\'"\s]+[\'"]?)'
      - r'partition\s*\(\s*([a-z]+)=([\'"]?[^,\'"\s)]+[\'"]?)'
      - r'([a-z]+)=([\'"]?\d{4}-\d{2}-\d{2}[\'"]?)'  # 日期格式
    validators:
      - type: "date_format"
        formats: ["yyyy-MM-dd", "yyyy/MM/dd", "yyyyMMdd"]

  column_name:
    patterns:
      - r'列\s*([a-z][a-z0-9_]*)'
      - r'字段\s*([a-z][a-z0-9_]*)'
      - r'column\s+([a-z][a-z0-9_]*)'

  limit:
    patterns:
      - r'(限制|limit|前)\s*(\d+)\s*条'
      - r'top\s+(\d+)'
      - r'只取\s*(\d+)'
    validators:
      - type: "range"
        min: 1
        max: 100000
        message: "LIMIT必须在1-100000之间"

  date_value:
    patterns:
      - r'(\d{4}[-/]\d{2}[-/]\d{2})'
      - r'(今天|昨天|明天|今天-1|今天+1)'
    normalizers:
      - type: "date_expr"
        mappings:
          "今天": "today"
          "昨天": "today-1"
          "明天": "today+1"

  condition:
    patterns:
      - r'where\s+([^;]+)'
      - r'条件[:：]\s*([^;]+)'
      - r'满足\s+([^;]+)'
```

#### 7.4.2 实体提取器实现

```python
# parsers/entity_extractor.py

import re
from typing import Dict, List, Any, Optional
from datetime import datetime

class EntityExtractor:
    """实体提取器"""

    def __init__(self, entity_config_path: str):
        self.entity_config = self._load_config(entity_config_path)
        self._compile_patterns()

    def extract(self, text: str, intent: str) -> Dict[str, Any]:
        """
        从文本中提取实体

        返回结构化参数字典，例如:
        {
            "table": "my_table",
            "partition": {"ds": "2025-01-01"},
            "limit": 1000
        }
        """
        entities = {}

        # 根据意图决定提取哪些实体
        required_entities = self._get_required_entities(intent)

        for entity_type in required_entities:
            if entity_type in self.entity_config['entity_types']:
                extracted = self._extract_entity(text, entity_type)
                if extracted is not None:
                    entities[entity_type] = extracted

        return entities

    def _extract_entity(self, text: str, entity_type: str) -> Any:
        """提取单个类型的实体"""
        config = self.entity_config['entity_types'][entity_type]
        patterns = config.get('patterns', [])
        validators = config.get('validators', [])
        normalizers = config.get('normalizers', [])

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # 提取原始值
                raw_value = match.group(1) if match.lastindex == 1 else match.groups()

                # 归一化处理
                normalized = self._normalize(raw_value, normalizers)

                # 验证
                if self._validate(normalized, validators):
                    return self._format_value(normalized, entity_type)

        return None

    def _normalize(self, value: Any, normalizers: List[Dict]) -> Any:
        """归一化处理"""
        for normalizer in normalizers:
            if normalizer['type'] == 'date_expr':
                mappings = normalizer.get('mappings', {})
                if isinstance(value, str) and value in mappings:
                    value = mappings[value]
        return value

    def _validate(self, value: Any, validators: List[Dict]) -> bool:
        """验证实体值"""
        for validator in validators:
            if validator['type'] == 'regex':
                if not re.match(validator['pattern'], str(value)):
                    return False
            elif validator['type'] == 'range':
                if not (validator['min'] <= int(value) <= validator['max']):
                    return False
        return True

    def _format_value(self, value: Any, entity_type: str) -> Any:
        """格式化实体值"""
        if entity_type == 'partition':
            # 解析分区键值对
            if isinstance(value, tuple) and len(value) == 2:
                return {value[0]: value[1]}
        elif entity_type == 'limit':
            return int(value)
        return value
```

### 7.5 槽位填充设计

#### 7.5.1 槽位定义与映射

```yaml
# config/slot_mapping.yaml
slot_mappings:
  alter.add_partition:
    slots:
      - name: "table"
        entity_type: "table_name"
        required: true
        prompts: ["请输入目标表名", "要给哪个表添加分区?"]

      - name: "partition"
        entity_type: "partition"
        required: true
        prompts: ["请输入分区信息，格式: ds='2025-01-01'", "分区值是多少?"]

      - name: "if_not_exists"
        entity_type: "boolean"
        default: true
        prompts: ["如果分区已存在是否忽略?"]

  insert.insert_select:
    slots:
      - name: "table"
        entity_type: "table_name"
        required: true
        prompts: ["要插入到哪个表?"]

      - name: "partition"
        entity_type: "partition"
        required: true
        prompts: ["目标分区是什么?"]

      - name: "source_table"
        entity_type: "table_name"
        required: true
        prompts: ["从哪个表查询数据?"]

      - name: "where_clause"
        entity_type: "condition"
        required: false
        default: null

      - name: "limit"
        entity_type: "limit"
        required: false
        default: 1000
        prompts: ["需要插入多少条数据?"]
```

#### 7.5.2 槽位填充器实现

```python
# parsers/slot_filler.py

from typing import Dict, Any, Optional, List

class SlotFiller:
    """槽位填充器"""

    def __init__(self, slot_config_path: str, schema_loader=None):
        self.slot_config = self._load_config(slot_config_path)
        self.schema_loader = schema_loader

    def fill(self,
             intent: str,
             sub_intent: str,
             entities: Dict[str, Any],
             interactive: bool = True) -> Dict[str, Any]:
        """
        填充模板槽位

        Args:
            intent: 主意图
            sub_intent: 子意图
            entities: 已提取的实体
            interactive: 是否交互式询问缺失参数

        Returns:
            完整的参数字典
        """
        template_key = f"{intent}.{sub_intent}"
        slot_config = self.slot_config.get('slot_mappings', {}).get(template_key, {})

        filled_params = {}
        missing_required = []

        # 处理每个槽位
        for slot in slot_config.get('slots', []):
            slot_name = slot['name']

            # 1. 检查实体中是否已有
            if slot_name in entities:
                filled_params[slot_name] = entities[slot_name]
                continue

            # 2. 尝试从表结构推断
            if self.schema_loader:
                inferred = self._infer_from_schema(slot_name, filled_params)
                if inferred is not None:
                    filled_params[slot_name] = inferred
                    continue

            # 3. 使用默认值
            if 'default' in slot:
                filled_params[slot_name] = slot['default']
                continue

            # 4. 处理缺失的必填参数
            if slot.get('required', False):
                missing_required.append(slot)
            else:
                filled_params[slot_name] = None

        # 处理缺失参数
        if missing_required:
            if interactive:
                filled_params.update(self._prompt_for_missing(missing_required, filled_params))
            else:
                raise ValueError(f"缺少必填参数: {[s['name'] for s in missing_required]}")

        return filled_params

    def _infer_from_schema(self, slot_name: str, current_params: Dict) -> Optional[Any]:
        """从表结构推断参数"""
        if slot_name == 'partition' and 'table' in current_params:
            # 从表结构获取分区信息
            table = current_params['table']
            partition_info = self.schema_loader.get_partition_info(table)
            if partition_info and partition_info.columns:
                # 返回默认分区值
                return {col: self._get_default_partition_value(col)
                        for col in partition_info.columns}

        return None

    def _get_default_partition_value(self, partition_column: str) -> str:
        """获取默认分区值"""
        if partition_column in ['ds', 'dt', 'date', 'day']:
            return datetime.now().strftime('%Y-%m-%d')
        return ''

    def _prompt_for_missing(self, missing_slots: List[Dict], current: Dict) -> Dict:
        """交互式询问缺失参数"""
        filled = {}
        print("需要补充以下参数:")
        for slot in missing_slots:
            prompts = slot.get('prompts', [f"请输入{slot['name']}"])
            for prompt in prompts:
                print(f"  {prompt}")
            value = input(f"{slot['name']}: ").strip()
            filled[slot['name']] = value
        return filled
```

### 7.6 歧义处理与交互

#### 7.6.1 歧义场景分类

| 歧义类型 | 示例 | 处理策略 |
|---------|------|---------|
| 表名模糊 | "给用户表添加分区" | 列出候选表，请用户选择 |
| 分区值缺失 | "添加分区" | 询问分区值，或使用今天日期 |
| 多个意图匹配 | "操作my_table" | 询问具体操作类型 |
| 参数冲突 | "LIMIT=10 但又说要全部" | 提示冲突，要求确认 |
| 条件不完整 | "状态为活跃的数据" | 询问活跃的定义 |

#### 7.6.2 交互式处理器

```python
# parsers/interactive_handler.py

from typing import Dict, List, Any, Optional

class DisambiguationHandler:
    """歧义处理器"""

    def handle_ambiguity(self,
                        ambiguity_type: str,
                        context: Dict[str, Any],
                        candidates: Optional[List[Any]] = None) -> Any:
        """
        处理歧义情况

        Args:
            ambiguity_type: 歧义类型
            context: 当前上下文
            candidates: 候选选项

        Returns:
            用户选择的结果
        """
        handlers = {
            'table_ambiguous': self._handle_table_ambiguity,
            'partition_missing': self._handle_partition_missing,
            'intent_multiple': self._handle_intent_multiple,
            'param_conflict': self._handle_param_conflict,
        }

        handler = handlers.get(ambiguity_type, self._handle_generic)
        return handler(context, candidates)

    def _handle_table_ambiguity(self, context: Dict, candidates: List[str]) -> str:
        """处理表名模糊"""
        print(f"找到多个匹配的表，请选择:")
        for i, table in enumerate(candidates, 1):
            print(f"  {i}. {table}")

        choice = input("请输入序号: ").strip()
        try:
            index = int(choice) - 1
            if 0 <= index < len(candidates):
                return candidates[index]
        except ValueError:
            pass

        return input("请输入完整表名: ").strip()

    def _handle_partition_missing(self, context: Dict, candidates: None) -> Dict[str, str]:
        """处理分区值缺失"""
        print("未指定分区值")
        use_default = input(f"使用今天日期 ({datetime.now().strftime('%Y-%m-%d')})? (y/n): ").strip().lower()

        if use_default == 'y':
            table = context.get('table', '')
            # 获取表的分区列
            partition_cols = self._get_table_partition_columns(table)
            return {col: datetime.now().strftime('%Y-%m-%d') for col in partition_cols}

        return self._prompt_partition_value()

    def _handle_intent_multiple(self, context: Dict, candidates: List[Dict]) -> str:
        """处理多个意图匹配"""
        print("您的请求可能对应以下操作:")
        for i, intent in enumerate(candidates, 1):
            print(f"  {i}. {intent['description']}")

        choice = input("请选择操作类型: ").strip()
        return candidates[int(choice) - 1]['intent']
```

### 7.7 完整转换流程示例

#### 示例1: 添加分区

**用户输入:**
```
给my_table表添加ds=2025-01-01的分区
```

**转换过程:**

```
步骤1: 预处理
  输入: "给my_table表添加ds=2025-01-01的分区"
  标准化: "给 my_table 表 添加 ds=2025-01-01 的 分区"

步骤2: 意图识别
  匹配关键词: ["添加", "分区"]
  结果: (intent="alter", sub_intent="add_partition", confidence=0.95)

步骤3: 实体提取
  - table_name: "my_table" (通过 "表\s*(\w+)" 模式)
  - partition: {"ds": "2025-01-01"} (通过 "(\w+)=(\S+)" 模式)

步骤4: 槽位填充
  模板: alter/add_partition.yaml
  必填槽位:
    ✓ table: "my_table" (已提取)
    ✓ partition: {"ds": "2025-01-01"} (已提取)
    ○ if_not_exists: true (使用默认值)

步骤5: 歧义处理
  无歧义，跳过

步骤6: 输出配置
  生成YAML配置 → SQL生成器 → 输出SQL
```

**最终生成的SQL:**
```sql
ALTER TABLE my_table ADD IF NOT EXISTS PARTITION (ds='2025-01-01');
```

#### 示例2: 插入数据 (带交互)

**用户输入:**
```
从source表插入1000条数据到target表
```

**转换过程:**

```
步骤1: 预处理
  输入: "从source表插入1000条数据到target表"

步骤2: 意图识别
  匹配关键词: ["从", "插入", "到"]
  结果: (intent="insert", sub_intent="insert_select", confidence=0.9)

步骤3: 实体提取
  - source_table: "source"
  - table: "target"
  - limit: 1000

步骤4: 槽位填充
  模板: insert/insert_select.yaml
  必填槽位:
    ✓ table: "target"
    ✓ source_table: "source"
    ✓ limit: 1000
    ✗ partition: 缺失! (必填)

步骤5: 交互式询问
  系统: "未指定分区值"
  系统: "使用今天日期 (2025-01-31)? (y/n): "
  用户: "y"
  partition: {"ds": "2025-01-31"}

步骤6: 输出SQL
```

**最终生成的SQL:**
```sql
INSERT INTO target
PARTITION (ds='2025-01-31')
SELECT * FROM source
LIMIT 1000;
```

### 7.8 配置文件示例

```yaml
# parsers/nlp_config.yaml
# 自然语言解析配置

preprocessing:
  # 繁简转换
  traditional_to_simplified: true
  # 全角转半角
  fullwidth_to_halfwidth: true
  # 停用词
  stop_words: ["的", "了", "和", "与", "或者"]

intent_recognition:
  # 关键词匹配权重
  keyword_weight: 0.6
  # 模式匹配权重
  pattern_weight: 0.4
  # 最低置信度阈值
  confidence_threshold: 0.5
  # 是否启用AI语义分析
  enable_semantic_analysis: false
  # AI模型配置 (可选)
  semantic_model:
    provider: "openai"
    model: "gpt-4"
    api_key_env: "OPENAI_API_KEY"

entity_extraction:
  # 实体提取顺序 (按优先级)
  priority_order:
    - "table_name"
    - "partition"
    - "limit"
    - "date_value"
    - "condition"
    - "column_name"
  # 是否验证实体值
  validate_entities: true
  # 是否使用表结构推断
  enable_schema_inference: true

slot_filling:
  # 交互式填充
  interactive: true
  # 默认值策略
  default_strategy: "smart"  # none, smart, prompt
  # 最多重试次数
  max_retries: 3

disambiguation:
  # 是否自动选择最高置信度结果
  auto_select_threshold: 0.95
  # 候选选项最大数量
  max_candidates: 5
  # 显示候选详情
  show_details: true
```

## 八、扩展性设计

### 8.1 模板扩展

用户可以自定义模板：

```yaml
# templates/custom/my_operation.yaml
template:
  name: "my_custom_operation"
  sql: |
    -- 自定义SQL逻辑
    {custom_sql}
```

### 8.2 验证规则扩展

```yaml
# config/custom_rules.yaml
custom_validations:
  - name: "check_business_rule"
    condition: "amount > 0"
    message: "金额必须大于0"
```

### 8.3 AI辅助扩展 (可选)

```python
# parsers/natural_parser.py (可选模块)

class NaturalLanguageParser:
    """自然语言解析器 - 可选AI模块"""

    def parse_to_config(self, text: str) -> dict:
        """将自然语言转换为配置"""
        # 调用AI模型解析
        # 返回结构化配置
```

## 九、与现有模块集成

### 9.1 现有模块复用

| 现有模块 | 用途 | 集成方式 |
|---------|------|---------|
| `generate_hdfs_commands.py` | 生成HDFS命令 | 作为输出后端 |
| `util/data_generator.py` | 生成测试数据 | 数据源集成 |
| `client/` | 数据库连接 | 连接管理 |
| `common/` | 公共函数 | 工具函数引用 |

### 9.2 迁移策略

1. **第一阶段**: 实现核心引擎，支持基本操作
2. **第二阶段**: 迁移现有脚本到新模板
3. **第三阶段**: 添加高级功能和AI辅助
4. **第四阶段**: 废弃旧脚本

## 十、安全与合规

### 10.1 安全措施

1. **敏感字段检查**: 自动检测并阻止敏感字段操作
2. **操作范围限制**: 限制行数、要求WHERE条件
3. **生产环境保护**: 额外确认生产环境操作
4. **审计日志**: 记录所有SQL生成和执行

### 10.2 合规要求

- 遵守公司数据安全规范
- 不包含微众/webank敏感字段
- 操作需加分区和LIMIT
- SQL执行前需人工确认

## 十一、开发计划

### 11.1 里程碑

| 阶段 | 内容 | 交付物 |
|------|------|--------|
| 第一阶段 | 核心框架搭建 | 基础架构、模板系统 |
| 第二阶段 | 基本功能实现 | INSERT/ALTER等常用操作 |
| 第三阶段 | 验证与优化 | 语法验证、性能优化 |
| 第四阶段 | 集成与测试 | 与现有模块集成、测试 |
| 第五阶段 | 文档与培训 | 用户文档、培训材料 |

### 11.2 优先级

**P0 (必须)**:
- 基础架构
- INSERT/SELECT模板
- 语法验证
- 安全检查

**P1 (重要)**:
- UPDATE/DELETE模板
- ALTER模板
- HDFS命令集成
- 表结构缓存

**P2 (可选)**:
- 自然语言解析
- Web UI
- AI辅助优化

## 十二、风险评估与应对

| 风险 | 影响 | 概率 | 应对措施 |
|------|------|------|---------|
| SQL语法复杂度高 | 高 | 中 | 分阶段实现，先支持常用场景 |
| 性能问题 | 中 | 低 | 优化验证逻辑，添加缓存 |
| 兼容性问题 | 中 | 中 | 充分测试，支持版本配置 |
| 用户接受度 | 高 | 低 | 渐进式迁移，保留旧系统 |

## 十三、总结


### 13.1 方案优势

1. **灵活性**: 配置驱动，无需编写代码
2. **可控性**: 基于模板，输出可预测
3. **安全性**: 多层验证，保护数据安全
4. **可维护性**: 模块化设计，易于维护
5. **可扩展性**: 支持自定义模板和规则
6. **成本适中**: 不依赖昂贵的AI服务

### 13.2 关键成功因素

1. **模板设计质量**: 模板覆盖率和易用性
2. **验证准确性**: 语法和安全验证的准确性
3. **用户体验**: 简单易用的接口
4. **文档完善度**: 清晰的使用文档
5. **渐进式迁移**: 平滑从旧系统迁移

---

**文档版本**: v1.0
**创建日期**: 2025-01-31
**作者**: Claude AI
**状态**: 待审核
