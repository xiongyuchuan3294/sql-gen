# 智能 SQL 生成 Agent Skill 设计方案

## 1. 背景与目标
本方案旨在创建一个“智能 SQL 生成 Agent Skill”，以辅助资深数据测试工程师。
**当前痛点**：`generate_sql` 文件夹中现有的脚本不够灵活，新需求往往需要修改代码。
**解决方案**：一个能够接受自然语言需求，理解 `ai_gen_data/SQL语法和hdfs命令.md` 中提供的特定 Hive SQL 语法/规则，检查表结构，并生成有效 SQL 的 Agent Skill。

## 2. 架构

### 2.1 核心组件
1.  **输入接口**：
    *   **自然语言**：接受描述（例如，“向 `user_info` 表插入5条用户 'test_01' 的测试数据”）。
    *   **结构化 YAML**：接受用于定义结构化参数的配置文件。
2.  **模板引擎 (Jinja2)**：
    *   支持使用 Jinja2 模板动态生成 SQL（例如，`SELECT * FROM {{ table }} WHERE dt='{{ date }}'`）。
    *   与 YAML 配置集成以注入参数值。
3.  **知识库**：
    *   **语法规则**：从 `ai_gen_data/SQL语法和hdfs命令.md` 加载。
    *   **Schema 信息**：通过 `check_hive_tables.py` 或 `hive_util.py` 动态获取（如果离线则模拟）。
4.  **推理引擎 (LLM)**：
    *   **思维链 (Chain of Thought)**：分析需求 -> 匹配 Schema -> 应用语法规则 -> 生成代码。
    *   *自修正 (Self-Correction)*：根据“高优注意点”（例如，“必须添加分区和 LIMIT”）验证生成的 SQL。

### 2.2 支持的 SQL 模式
Agent 将被设计为识别并生成以下特定模式：
1.  **DDL (表管理)**:
    *   `CREATE TABLE`: 支持复杂类型 (Array, Map, Struct)、分区 (`PARTITIONED BY`)、分桶 (`CLUSTERED BY`) 和存储格式 (`STORED AS`).
    *   `ADD PARTITION`: `ALTER TABLE table_name ADD PARTITION (...)`.
2.  **DML (数据写入与修改)**:
    *   **INSERT OVERWRITE**: 全量覆盖，常配合 `PARTITION` 和 `CASE WHEN` (用于修改特定字段数据)。
    *   **INSERT INTO ... SELECT**: 追加数据，支持带条件的来源选择。
    *   **INSERT INTO ... VALUES**: 直接插入常量值 (e.g., mock data)。
    *   **特定列插入**: `INSERT INTO table (col1, col2) ...`。
3.  **DQL (数据探查)**:
    *   `SHOW TABLES`, `SHOW PARTITIONS`.
    *   `DESCRIBE FORMATTED` (查看表结构).
4.  **复杂逻辑**:
    *   **数据修正**: 使用 `CASE WHEN` 逻辑在 `SELECT` 子句中动态修改值 (e.g., `CASE WHEN id=4 THEN 5 ELSE id END`).
    *   **复杂类型构造**: 正确使用 `map()`, `array()`, `struct()` 构造函数.
    *   **正则匹配**: 使用 `RLIKE`, `REGEXP` 进行复杂过滤.

### 2.3 扩展能力
Agent 将内置以下标准化处理能力，以替代原有的硬编码脚本：
1.  **数据质量检查**: 
    *   **行数统计 (`data_num`)**: 生成 `SELECT COUNT(1) ...` 语句，支持按分区统计。
    *   **空值检查 (`null_num`)**: 生成 `SELECT COUNT(1) FROM ... WHERE col IS NULL`。
    *   **重复检查 (`repeat_check`)**: 生成 `SELECT col, COUNT(1) ... GROUP BY col HAVING COUNT(1) > 1`。
2.  **数据比对**:
    *   **数据差异 (`data_diff`)**: 生成 SQL 比较两个表/分区的数据差异（例如使用 `FULL OUTER JOIN` 或 `EXCEPT`）。
    *   **结构差异 (`tablediff`)**: 比较两个表的 Schema 定义。
3.  **表结构变更**:
    *   **修改列 (`alter_columns`)**: 生成 `ALTER TABLE ... CHANGE COLUMN` 或 `ADD BOULUMN` 语句。
4.  **数据清理与转换**:
    *   **特定删除 (`delete_use_id`)**: 生成 `INSERT OVERWRITE ... SELECT ... WHERE id NOT IN (...)` (Hive 不支持直接 DELETE 时) 或 `DELETE FROM ...` (如果支持 ACID)。
    *   **格式转换 (`convertfomart`)**: 处理数据格式转换逻辑 (e.g., String to Date, CSV to Parquet via table properties)。

### 2.4 Agent Skill 工作流
1.  **接收任务**：用户提供需求（自然语言）。
2.  **Schema 查询**：Agent 获取表结构信息。
3.  **中间层生成 (NL -> YAML)**：
    *   Agent 分析自然语言需求。
    *   Agent 将需求映射为符合预定义 Schema 的 **YAML 配置**。
    *   *优势*：将非结构化需求转化为结构化数据，便于校验和模版填充。
4.  **SQL 生成 (YAML -> Jinja -> SQL)**：
    *   Agent 选择或生成合适的 Jinja2 模板。
    *   使用生成的 YAML 配置渲染模板，得到 SQL 初稿。
5.  **语法与约束检查**：
    *   应用 `SQL语法和hdfs命令.md` 中的规则（如 PARTITION, LIMIT）。
    *   检查生成的 YAML 是否遗漏了关键参数。
6.  **最终输出**：返回生成的 SQL 及对应的 YAML 配置（可选，供用户复用）。

## 3. 详细设计

### 3.1 提示词策略 (Skill 系统提示词)
Agent Skill 将使用结构化的系统提示词：

> **角色**：资深数据测试工程师助理（SQL 专家）。
> **目标**：根据用户需求生成有效的 Hive SQL。
> **约束**：
> 1.  **严格遵守语法**：必须遵守 `SQL语法和hdfs命令.md` 中的规则。
>     *   *高重要性*：操作必须包含 PARTITION 和 LIMIT (除非是 SHOW/DESCRIBE)。
>     *   *数据隐私*：不得包含敏感字段（微众/Webank）。
> 2.  **模式支持**：能够处理 `INSERT OVERWRITE`, `INSERT INTO`, `CREATE TABLE`, `ADD PARTITION`。
> 3.  **复杂类型**：根据参考文档正确处理 Map/Array/Struct。
> 4.  **逻辑处理**：能够使用 `CASE WHEN` 进行数据清洗/逻辑转换。

### 3.2 动态 Schema 处理
Agent 不应硬编码，而应使用工具（如 `get_table_schema(table_name)`）来获取列名和类型。
*   *设计说明*：如果工具不可用，Agent 将要求用户以 `CREATE TABLE` 格式提供 Schema。

### 3.3 自修正机制（"检查"步骤）
Agent 在输出前将执行专门的“验证步骤”，覆盖以下业界最佳实践与语法规则：

**1. 语法与结构完整性** (Hive Syntax Check)
*   [ ] SQL 关键字拼写和位置是否正确（e.g., `SELECT`, `FROM`, `WHERE` 顺序）？
*   [ ] 括号是否闭合？函数调用语法是否正确？
*   [ ] 分号结尾是否遗漏？

**2. 强制性约束与性能优化规则** (High-Priority Constraints)
*   [ ] **查询限制 (Mandatory LIMIT)**: 对于 SELECT 查询，是否包含 `LIMIT` 子句？（防止拉取过多数据导致 Client OOM）。
*   [ ] **分区过滤 (Partition Pruning)**: 是否针对分区表使用了分区字段进行过滤？（防止全表扫描）。
*   [ ] **排序优化**: 使用 `ORDER BY` 时是否配合了 `LIMIT`？建议优先使用 `SORT BY` 以规避全局排序的单一 Reducer 瓶颈。
*   [ ] **笛卡尔积检测**: 检查 `JOIN` 是否缺少关联条件（ON/USING），是否导致笛卡尔积（Cross Join）。

**3. 数据类型与逻辑正确性** (Data Type & Logic)
*   [ ] **复杂类型构造**: Map/Array/Struct 的构造函数 `map()`, `array()`, `struct()` 参数格式是否正确？
*   [ ] **类型兼容性**: 避免将字符串隐式转换为数字进行数值比较（可能导致 Index 失效或结果错误）。
*   [ ] **NULL 处理**: 检查是否错误使用了 `= NULL` (应为 `IS NULL`)。
*   [ ] **数据修正逻辑**: `CASE WHEN` 语句是否包含 `ELSE` 分支（防止意外的 NULL 返回）？

**4. 安全与合规** (Security & Compliance)
*   [ ] **敏感数据**: 是否包含微众/Webank 敏感字段关键词？
*   [ ] **非保留字命名**: 别名或新列名是否使用了 Hive 保留字？

**5. 最佳实践反模式检测** (Anti-Patterns)
*   [ ] **SELECT ***: 是否使用了 `SELECT *`？（建议明确列出字段，减少 IO）。
*   [ ] **Join 顺序**: 大表 Join 小表时，小表是否在左侧（或使用 MapJoin Hint）？（虽然现代 Hive CBO 会优化，但作为检查项仍有价值）。
*   [ ] **Distinct 滥用**: 是否在大宽表上滥用 `COUNT(DISTINCT ...)`？（可能导致数据倾斜，建议 `GROUP BY`）。

### 3.4 结构化输入 (YAML) 与 Jinja 模板
为了增强灵活性，Agent 将支持 YAML 输入结合 Jinja2 模板。
**模板 (`insert_data.sql.j2`)**：
```sql
INSERT INTO TABLE {{ table }} PARTITION ({{ partition.key }}='{{ partition.value }}')
SELECT
{% for field, value in data.fields.items() -%}
  {{ value }} AS {{ field }}{{ ", " if not loop.last else "" }}
{%- endfor %}
LIMIT {{ options.limit }};
```

**配置 (`request.yaml`)**：
```yaml
table: "page_view"
partition:
  key: "dt"
  value: "2023-01-01"
data:
  fields:
    user_id: "random_id()"
    properties: "map('device', 'iphone')"
options:
  limit: 10
```
**处理流程**：
1.  **加载模板**：Agent 加载相应的 Jinja2 模板（或动态生成一个）。
2.  **加载上下文**：将 `request.yaml` 解析为 Python 字典。
3.  **渲染**：使用配置数据递归渲染模板。
4.  **验证**：根据语法规则检查渲染后的 SQL。

## 4. 场景示例

### 场景 A：完整流程 (Natural Language -> YAML -> SQL)
**用户输入**：“为 `page_view` 表构造测试数据，日期为 '2023-01-01'，共 10 条。properties map 应该包含 'device' -> 'iphone'。”

**Agent 处理步骤**：

**1. 中间层生成 (NL -> YAML)**
Agent 分析需求并输出 YAML：
```yaml
table: "page_view"
partition:
  key: "dt"
  value: "2023-01-01"
data:
  fields:
    user_id: "random_id()"
    properties: "map('device', 'iphone')"
options:
  limit: 10
```

**2. 模板选择与渲染 (YAML -> Jinja)**
Agent 选择通用插入模板 `insert_data.sql.j2` 并注入 YAML 数据：
*   `{{ table }}` -> `page_view`
*   `{{ partition.key }}='{{ partition.value }}'` -> `dt='2023-01-01'`

**3. SQL 生成与验证**
生成的 SQL 初稿：
```sql
INSERT INTO TABLE page_view PARTITION (dt='2023-01-01')
SELECT random_id() AS user_id, map('device', 'iphone') AS properties
LIMIT 10;
```
```sql
INSERT INTO TABLE page_view PARTITION (dt='2023-01-01')
SELECT random_id() AS user_id, map('device', 'iphone') AS properties
LIMIT 10;
```
**验证**：
*   语法检查：PASS
*   强制约束：包含 `PARTITION (dt=...)` 和 `LIMIT 10` -> PASS
*   最佳实践：未使用 `SELECT *` -> PASS
*   结果：验证通过。

### 场景 B：直接 YAML 输入
**用户输入**：（直接提供上述 YAML）
**Agent 处理**：直接跳过步骤 1，从步骤 2 开始执行。

### 场景 C：执行专用脚本功能 (e.g., 数据行数统计)
**用户输入**：“统计 `user_info` 表在 2023-01-01 分区的数据量。”
**Agent 处理**：
1.  **识别意图**：匹配到 `data_num` 能力。
2.  **生成 SQL**：
    ```sql
    SELECT COUNT(1) as total_count 
    FROM user_info 
    WHERE dt='2023-01-01';
    ```
**Agent 处理**：
1.  **识别意图**：匹配到 `data_num` 能力。
2.  **生成 SQL**：
    ```sql
    SELECT COUNT(1) as total_count 
    FROM user_info 
    WHERE dt='2023-01-01';
    ```
3.  **验证**：
    *   分区过滤：包含了 `WHERE dt='...'` -> PASS
    *   类型检查：`COUNT(1)` 返回 BIGINT -> PASS
    *   结果：验证通过。

## 5. 下一步
1.  实现 `SKILL.md` 文件。
2.  根据 `SQL语法和hdfs命令.md` 文件验证提示词。
3.  使用示例输入进行测试。
