# 智能 SQL 生成 Skill 用户手册（自然语言输入指南）

本文档面向使用者：教你如何用自然语言（或 YAML）向 `intelligent_sql_generation` 提需求，让它稳定产出符合约束的 Hive SQL / HDFS 命令。

相关文档：
- 系统/agent 约束与工作流：`SKILL.md`
- 模板索引（自动生成）：`TEMPLATE_GUIDE.md`
- 参数契约（权威）：`templates/yaml/*.yaml`

---

## 1. 你能让它做什么

常见场景：
- 查询类：行数、空值率、空值检查、字段分布、分组 TopN、字段长度等
- 对比类：两表/两分区差异对比（`data_diff`）、反向连接缺失（`anti_join`）
- 操作类：清洗过滤（`data_clean`）、删除/移动/创建分区、插入测试数据、合并 union
- 结构类：`ALTER TABLE` 添加/修改列等
- 系统类：用 `hdfs_du` 生成 `hadoop fs -du -h` 命令

模板清单以 `TEMPLATE_GUIDE.md` 为准；更细的参数要求以 `templates/yaml/<type>.yaml` 为准。

---

## 2. 最重要的使用规则

1. 默认只生成，不执行：
   - 你不写 “执行/运行/run/execute”，它只会返回 SQL/命令文本。
   - 你写了 “执行/运行/run/execute”，它也必须先给出 SQL，再向你确认后才会执行。
2. 分区表必须给分区：
   - 如果表是分区表但你没提供分区，它会反问你补充。
3. 对比类必须给主键/关联键：
   - `data_diff` / `anti_join` 需要 `join_keys`（支持多字段）。
4. 查询返回明细必须 `LIMIT`：
   - 需要看样例数据时，把“看 20 行/limit 20”写进需求。

---

## 3. 输入方式

### 3.1 直接自然语言（推荐）
写清楚：表名、分区、字段（如有）、主键（对比类）、你想要的结果。

### 3.2 直接给 YAML（高级/可重复）
你也可以直接描述成：
```yaml
type: data_diff
params:
  source_table: imd_aml_safe.t_a
  target_table: imd_amlai_ads_safe.t_b
  source_partition: "ds='2026-02-01'"
  target_partition: "ds='2026-02-01'"
  join_keys: ["cust_id"]
```
YAML 的字段名必须与 `templates/yaml/<type>.yaml` 一致。

---

## 4. 参数写法规范（建议）

### 4.1 表名（table_name / source_table / target_table）
支持：
- `db.table_name`（推荐，避免歧义）
- `table_name`（只给表名时，系统会尝试在当前 Hive 环境中自动发现 db）

示例：
- `对比 imd_aml_safe.t_a 和 imd_amlai_ads_safe.t_b`
- `查询 t_table 的数据量`（系统会尝试补全 db）

### 4.2 分区（partition / source_partition / target_partition）
支持：
- `ds='2026-01-01'` 或 `ds=2026-01-01`
- 多级分区：`ds='2026-01-01', hour='23'`
- 多值：`ds=2026-02-01 和 2026-02-02`（通常会转成 `IN (...)`）

示例：
- `查询 t_table ds=2026-01-01 的数据量`
- `对比 t_table ds=2026-02-01 和 2026-02-02 分区`

### 4.3 主键/关联字段（join_keys / group_by_columns）
支持：
- 单字段：`cust_id`
- 多字段：`cust_id, account_no` 或 `cust_id 和 account_no`

示例：
- `主键 cust_id`
- `key cust_id,account_no`

### 4.4 字段列表（columns）
支持：
- `user_id,email` 或 `user_id 和 email`

---

## 5. 模板选择：关键词到模板类型

你不需要指定模板类型，通常写意图关键词就够了。系统会按关键词选择最接近的模板（模板名称以 `TEMPLATE_GUIDE.md` 为准）：

| 用户意图关键词 | 模板类型 | 说明 |
| --- | --- | --- |
| 对比、差异、diff、区别 | `data_diff` | 数据对比/差异分析 |
| 没有、不存在、missing、在A不在B | `anti_join` | 反向连接查找 |
| 重复、去重、duplicate | `repeat_check` | 重复值检查 |
| 数据量、行数、count、记录数 | `data_num` | 数据计数 |
| 空值、为空、null | `null_rate` / `null_checks` | 空值率/空值检查 |
| 分布、分布情况 | `field_dist` | 字段值分布 |
| top、前几、排名 | `group_top_n` | 分组 TopN |
| 删除分区 | `drop_partition` | 删除分区 |
| 备份、移动分区 | `move_partition` | 移动分区 |
| 创建临时分区 | `create_temp_partition` | 创建临时分区 |
| 过滤、清洗 | `data_clean` | 数据清洗 |
| 插入、生成测试数据 | `insert_values` | 插入测试数据 |
| 合并、union | `union_merge` | 合并查询 |
| 修改表结构、添加列 | `alter_table` | 修改表结构 |
| 字段长度、最长 | `check_field_len` | 字段长度检查 |
| HDFS大小、存储大小 | `hdfs_du` | HDFS 存储查询 |
| 批量统计 | `batch_data_num` | 批量数据统计 |

---

## 6. 常用自然语言示例

### 6.1 数据对比（data_diff）
```
对比 imd_aml_safe.t_local_hs2_aml_safe_20260304 和 imd_amlai_ads_safe.t_local_hs2_amlai_ads_20260304 在 ds=2026-02-01 分区的数据差异，主键 id
```

### 6.2 反向连接缺失（anti_join）
```
查找 imd_aml_safe.t_a 有但 imd_amlai_ads_safe.t_b 没有的记录 ds=2026-01-01，主键 cust_id
```

### 6.3 空值率（null_rate）
```
查询 t_table 表的 user_id 和 email 字段空值率 ds=2026-01-01
```

### 6.4 字段分布（field_dist）
```
查询 t_table 表的 status 字段值分布 ds=2026-01-01
```

### 6.5 数据量（data_num）
```
查询 t_table ds=2026-01-01 的数据量
```

### 6.6 空值检查（null_checks）
```
检查 t_table 表的 name 和 phone 字段是否有空值 ds=2026-01-01
```

### 6.7 重复检查（repeat_check）
```
检查 t_table 表的 cust_id 字段是否有重复 ds=2026-01-01
```

### 6.8 删除分区（drop_partition）
```
删除 t_table 表的 ds=2026-01-01 分区
```

### 6.9 移动分区（move_partition）
```
将 t_table 表的 ds=2026-01-01 分区移动到 ds=2026-01-01-temp
```

### 6.10 创建临时分区（create_temp_partition）
```
为 t_table 表创建 ds=2026-01-01-temp 临时分区
```

### 6.11 数据清洗（data_clean）
```
清洗 t_table 表 ds=2026-01-01 分区，过滤掉 status='invalid' 的数据
```

### 6.12 插入测试数据（insert_values）
```
向 t_table 表插入测试数据，字段 cust_id, name
```

### 6.13 合并查询（union_merge）
```
合并 t_table 表 ds=2026-01-01 和 ds=2026-01-02 的查询结果
```

### 6.14 修改表结构（alter_table）
```
为 t_table 表添加新列 age int
```

### 6.15 字段长度检查（check_field_len）
```
查询 t_table 表的 address 字段最长值 ds=2026-01-01
```

### 6.16 分组 TopN（group_top_n）
```
查询 t_table 表每个 cust_id 的最近 3 条订单 ds=2026-01-01
```

### 6.17 批量统计（batch_data_num）
```
批量统计 t_a, t_b, t_c 三个表 ds=2026-01-01 的数据量
```

### 6.18 HDFS 大小（hdfs_du）
```
查询 t_local_hs2_amlai_ads_20260304 的 HDFS 存储大小
```

---

## 7. 常见问题与提示

- 忘了分区：如果是分区表，请补上 `ds=...` / `dt=...`，或明确多级分区。
- 对比没写主键：请补 `主键 xxx`，多字段用 `k1,k2`。