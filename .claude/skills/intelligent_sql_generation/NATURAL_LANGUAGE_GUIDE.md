# 自然语言输入指南

本文档定义了用户输入自然语言时，AI如何解析参数并映射到SQL模板。

---

## 1. 整体架构

```
用户自然语言 → AI参数提取 → YAML参数组装 → Jinja2模板渲染 → SQL输出
```

AI在解析过程中会：
1. **自动发现数据库**：通过表名在Hive环境中搜索
2. **自动获取分区字段**：通过DESCRIBE查询表结构
3. **自动获取非分区字段**：用于数据对比

---

## 2. 模板分类与自然语言格式

### 2.1 数据查询类

| 模板类型 | 用途 | 必填参数 | 自然语言示例 |
|---------|------|----------|-------------|
| `data_num` | 统计行数 | table_name, partition | "查询 t_table ds=2026-01-01 的数据量" |
| `null_rate` | 空值率统计 | table_name, partition, columns | "查询 t_table 表的 user_id 和 email 字段空值率 ds=2026-01-01" |
| `field_dist` | 字段值分布 | table_name, partition, columns | "查询 t_table 表的 status 字段值分布 ds=2026-01-01" |
| `group_top_n` | 分组TopN | table_name, partition, partition_by, order_by, limit_n | "查询 t_table 每个班级成绩前三的学生 ds=2026-01-01" |
| `null_checks` | 空值检查 | table_name, partition, columns | "检查 t_table 表的 user_id 和 order_id 是否为空 ds=2026-01-01" |
| `repeat_check` | 重复值检查 | table_name, partition, group_by_columns | "检查 t_table 表的 user_id 是否有重复 ds=2026-01-01" |
| `batch_data_num` | 批量统计行数 | tables: [{name, partition}, ...] | "统计 table_A ds=2026-01-01 和 table_B ds=2026-01-02 的数据量" |

### 2.2 数据对比类

| 模板类型 | 用途 | 必填参数 | 自然语言示例 |
|---------|------|----------|-------------|
| `data_diff` | 对比两个分区/表的数据差异 | source_table, target_table, source_partition, target_partition, join_keys | "对比 imd_aml_safe.t_a 和 imd_amlai_ads_safe.t_b 在 ds=2026-02-01 的数据差异，主键 cust_id" |
| `anti_join` | 反向连接查找 | source_table, target_table, source_partition, target_partition, join_keys | "查找 imd_aml_safe.t_a 有但 imd_amlai_ads_safe.t_b 没有的记录 ds=2026-01-01 主键 cust_id" |

### 2.3 数据操作类

| 模板类型 | 用途 | 必填参数 | 自然语言示例 |
|---------|------|----------|-------------|
| `data_clean` | 数据清洗过滤 | table_name, partition, filter_condition | "过滤掉 user_id 为空的记录 ds=2026-01-01" |
| `drop_partition` | 删除分区 | table_name, partition, if_exists | "删除 t_table ds=2026-01-01 分区" |
| `move_partition` | 移动分区(备份) | table_name, source_partition, target_partition | "将 ds=2026-01-01 备份到 ds=2026-01-01-temp" |
| `create_temp_partition` | 创建临时分区 | table_name, partition | "创建 ds=2026-01-01-temp 临时分区" |
| `insert_values` | 插入测试数据 | table_name, partition, data_rows | "插入10条测试数据到 t_table" |
| `union_merge` | 合并多个查询 | queries: [{table_name, columns, partition, condition}, ...] | "合并 table_a 和 table_b 的 active 记录" |

### 2.4 表结构操作类

| 模板类型 | 用途 | 必填参数 | 自然语言示例 |
|---------|------|----------|-------------|
| `alter_table` | 修改表结构 | table_name, operations | "给 t_table 添加新列 col_1" |
| `check_field_len` | 检查字段长度 | table_name, partition, column, limit | "查看 note 字段最长的5条记录 ds=2026-01-01" |

### 2.5 系统操作类

| 模板类型 | 用途 | 必填参数 | 自然语言示例 |
|---------|------|----------|-------------|
| `hdfs_du` | 查询HDFS存储大小 | targets: [{db, table, partition}, ...] | "查询 imd_aml_safe.t_table 的 HDFS 存储大小" |

---

## 3. 参数提取规则

### 3.1 表名 (table_name / source_table / target_table)

**支持格式：**
- 完整格式：`db.table_name`（如 `imd_aml_safe.t_table`）
- 简略格式：`table_name`（AI会自动发现数据库）

**提取关键词：**
- 直接写表名
- "的 table" 格式

**示例：**
```
"对比 imd_aml_safe.t_a 和 imd_amlai_ads_safe.t_b"  → source_table="imd_aml_safe.t_a", target_table="imd_amlai_ads_safe.t_b"
"查询 t_table 的数据量"  → table_name="t_table" (AI会自动发现数据库)
```

### 3.2 分区 (partition / source_partition / target_partition)

**支持格式：**
- 标准格式：`ds='2026-01-01'` 或 `ds=2026-01-01`
- 多级分区：`ds='2026-01-01',hour='23'`
- 自然语言：`2026-01-01 分区`、`的 2026-02-01`

**提取关键词：**
- `ds=`, `dt=`, `partition=`
- 日期+分区：`2026-01-01 分区`

**示例：**
```
"ds=2026-02-01"  → partition="ds='2026-02-01'"
"在 ds='2026-02-01' 分区"  → partition="ds='2026-02-01'"
"2026-02-01 和 2026-02-02 分区"  → partition="ds='2026-02-01',ds='2026-02-02'" (IN查询)
```

### 3.3 主键/关联字段 (join_keys / group_by_columns)

**支持格式：**
- 单字段：`cust_id`
- 多字段：`cust_id,account_no` 或 `cust_id 和 account_no`

**提取关键词：**
- `主键`、`key`、`on`、`关联`
- `按 xxx 分组`

**示例：**
```
"主键 cust_id"  → join_keys=["cust_id"]
"key cust_id,account_no"  → join_keys=["cust_id", "account_no"]
"按 user_id 分组检查重复"  → group_by_columns=["user_id"]
```

### 3.4 字段列表 (columns)

**支持格式：**
- 单字段：`user_id`
- 多字段：`user_id,email` 或 `user_id 和 email`

**提取关键词：**
- 直接写字段名

**示例：**
```
"查询 t_table 表的 user_id 和 email 的空值率"  → columns=["user_id", "email"]
```

---

## 4. 自然语言到YAML的参数映射示例

### 示例1：数据对比
**用户输入：**
```
对比 imd_aml_safe.t_a 和 imd_amlai_ads_safe.t_b 在 ds=2026-02-01 和 ds=2026-02-02 分区的数据差异，主键 cust_id
```

**AI提取的YAML参数：**
```yaml
type: data_diff
params:
  source_table: imd_aml_safe.t_a
  target_table: imd_amlai_ads_safe.t_b
  source_partition: "ds IN ('2026-02-01', '2026-02-02')"
  target_partition: "ds IN ('2026-02-01', '2026-02-02')"
  join_keys:
    - cust_id
```

### 示例2：数据统计
**用户输入：**
```
查询 imd_aml_safe.t_table 在 2026-01-01 的数据量
```

**AI提取的YAML参数：**
```yaml
type: data_num
params:
  table_name: imd_aml_safe.t_table
  partition: "ds='2026-01-01'"
```

### 示例3：空值检查
**用户输入：**
```
检查 t_table 表的 user_id、email、phone 字段空值情况 ds=2026-01-01
```

**AI提取的YAML参数：**
```yaml
type: null_rate
params:
  table_name: t_table
  partition: "ds='2026-01-01'"
  columns:
    - user_id
    - email
    - phone
```

### 示例4：HDFS存储查询
**用户输入：**
```
查询 imd_aml_safe.t_table 的 HDFS 存储大小
```

**AI提取的YAML参数：**
```yaml
type: hdfs_du
params:
  targets:
    - db: imd_aml_safe
      table: t_table
```

---

## 5. 模板关键词识别表

AI通过以下关键词自动识别用户意图并选择对应模板：

| 用户意图关键词 | 模板类型 | 说明 |
|--------------|----------|------|
| 对比、差异、diff、区别 | data_diff | 数据对比/差异分析 |
| 没有、不存在、missing、在A不在B | anti_join | 反向连接查找 |
| 重复、去重、duplicate | repeat_check | 重复值检查 |
| 数据量、行数、count、记录数 | data_num | 数据计数 |
| 空值、为空、null | null_rate / null_checks | 空值检查 |
| 分布、分布情况 | field_dist | 字段值分布 |
| top、前几、排名 | group_top_n | 分组TopN |
| 删除分区 | drop_partition | 删除分区 |
| 备份、移动分区 | move_partition | 移动分区 |
| 创建临时分区 | create_temp_partition | 创建临时分区 |
| 过滤、清洗 | data_clean | 数据清洗 |
| 插入、生成测试数据 | insert_values | 插入测试数据 |
| 合并、union | union_merge | 合并查询 |
| 修改表结构、添加列 | alter_table | 修改表结构 |
| 字段长度、最长 | check_field_len | 字段长度检查 |
| HDFS大小、存储大小 | hdfs_du | HDFS存储查询 |
| 批量统计 | batch_data_num | 批量数据统计 |

---

## 6. 注意事项

1. **分区表必须指定分区**：如果表是分区表但用户未指定分区，AI会提示用户补充
2. **主键字段必须提供**：数据对比类模板需要明确指定主键字段
3. **自动补全**：如果用户未提供某些可选参数（如columns），AI会自动从表结构中获取
4. **跨数据库表名**：建议使用 `db.table` 格式明确指定数据库，避免歧义
