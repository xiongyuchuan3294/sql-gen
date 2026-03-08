# Intelligent SQL Generation User Guide

This guide shows end users how to ask `intelligent_sql_generation` for Hive SQL or HDFS commands using natural language or YAML. Natural-language requests may be written in English or Chinese.

Related files:
- System behavior and workflow rules: `../SKILL.md`
- Auto-generated template index: `TEMPLATE_GUIDE.md`
- Exact parameter contracts: `assets/templates/yaml/*.yaml`

Contents:
- `1. What it can do`
- `2. The most important rules`
- `3. Input styles`
- `4. Parameter writing conventions`
- `5. Mapping intent to template types`
- `6. Common natural-language examples`
- `7. Tips and troubleshooting`

---

## 1. What it can do

Common use cases:
- Query checks: row count, null rate, null checks, field distribution, group top N, field length
- Comparison checks: compare two tables or partitions with `data_diff`, find missing records with `anti_join`
- Operations: clean data, drop / move / create partitions, insert test rows, union queries
- Schema changes: generate `ALTER TABLE` statements
- System tasks: generate `hadoop fs -du -h` commands with `hdfs_du`

Use `TEMPLATE_GUIDE.md` for the current template list, and use `assets/templates/yaml/<type>.yaml` for the authoritative parameter contract.

---

## 2. The most important rules

1. Generate first, execute later.
   - If you do not say `execute` or `run`, the skill returns SQL or shell text only.
   - Even if you ask to execute, it must still show the generated SQL first and ask for confirmation.
2. Partitioned tables need partition values.
   - If the target table is partitioned and you do not provide a partition, the skill should ask for it.
3. Comparison templates need join keys.
   - `data_diff` and `anti_join` require `join_keys`.
4. Detail queries should usually include `LIMIT`.
   - If you want sample rows, say something like `show 20 rows`.

---

## 3. Input styles

### 3.1 Natural language
Describe the table, partition, fields, keys, and expected output.

### 3.2 YAML
You can also provide the request directly in YAML:

```yaml
type: data_diff
params:
  source_table: imd_aml_safe.t_a
  target_table: imd_amlai_ads_safe.t_b
  source_partition: "ds='2026-02-01'"
  target_partition: "ds='2026-02-01'"
  join_keys: ["cust_id"]
```

YAML field names must match `assets/templates/yaml/<type>.yaml`.

---

## 4. Parameter writing conventions

### 4.1 Table names
Supported forms:
- `db.table_name` (recommended)
- `table_name` only, if you want the system to try database discovery

Examples:
- `Compare imd_aml_safe.t_a and imd_amlai_ads_safe.t_b`
- `Count rows in t_table`

### 4.2 Partitions
Supported forms:
- `ds='2026-01-01'` or `ds=2026-01-01`
- multi-level partitions such as `ds='2026-01-01',hour='23'`
- multi-value phrasing that may normalize into `IN (...)`

Examples:
- `Count rows in t_table for ds=2026-01-01`
- `Compare t_table for ds=2026-02-01 and ds=2026-02-02`

### 4.3 Join keys or grouping keys
Supported forms:
- single field: `cust_id`
- multiple fields: `cust_id, account_no`
- natural language: `primary key cust_id, account_no`

### 4.4 Column lists
Supported forms:
- `user_id,email`
- `user_id, email`

---

## 5. Mapping intent to template types

You usually do not need to name the template directly. The skill should map intent keywords to the closest template.

| User intent keywords | Template type | Purpose |
| --- | --- | --- |
| compare, diff, difference | `data_diff` | compare two datasets |
| missing, not in target, anti join | `anti_join` | find records missing from the target |
| duplicate, deduplicate | `repeat_check` | detect duplicate keys |
| count, row count, volume | `data_num` | count rows |
| null, null rate, empty | `null_rate` / `null_checks` | null quality checks |
| distribution | `field_dist` | field value distribution |
| top, ranking | `group_top_n` | top N per group |
| drop partition | `drop_partition` | drop a partition |
| move partition, backup partition | `move_partition` | copy or move partition data |
| create temp partition | `create_temp_partition` | create an empty temp partition |
| clean, filter | `data_clean` | overwrite filtered data |
| insert, mock data | `insert_values` | insert sample rows |
| union, merge | `union_merge` | combine multiple queries |
| alter table, add column | `alter_table` | schema changes |
| field length, longest value | `check_field_len` | inspect field length |
| hdfs size, storage size | `hdfs_du` | HDFS size checks |
| batch count | `batch_data_num` | count multiple tables |

---

## 6. Common natural-language examples

### 6.1 Data diff (`data_diff`)
`Compare source_table and target_table for ds=2026-02-01 using primary key cust_id.`

### 6.2 Missing records (`anti_join`)
`Find rows in source_table that do not exist in target_table for ds=2026-02-01 using key cust_id.`

### 6.3 Null rate (`null_rate`)
`Calculate the null rate of email and phone in foo.bar for ds=2026-02-01.`

### 6.4 Field distribution (`field_dist`)
`Show the value distribution of status in foo.bar for ds=2026-02-01.`

### 6.5 Row count (`data_num`)
`Count rows in foo.bar for ds=2026-02-01.`

### 6.6 Null checks (`null_checks`)
`Check whether email, phone, and address contain nulls in foo.bar for ds=2026-02-01.`

### 6.7 Duplicate keys (`repeat_check`)
`Check duplicate cust_id values in foo.bar for ds=2026-02-01.`

### 6.8 Drop partition (`drop_partition`)
`Generate SQL to drop partition ds=2026-02-01 from foo.bar.`

### 6.9 Move partition (`move_partition`)
`Move partition ds=2026-02-01 to ds=2026-02-01-temp in foo.bar.`

### 6.10 Create temp partition (`create_temp_partition`)
`Create an empty temp partition ds=2026-02-01-temp for foo.bar.`

### 6.11 Data clean (`data_clean`)
`Overwrite foo.bar for ds=2026-02-01 while filtering out rows where status = 'deleted'.`

### 6.12 Insert values (`insert_values`)
`Insert mock rows into foo.bar for ds=2026-02-01.`

### 6.13 Union merge (`union_merge`)
`Generate a union query that combines foo.bar and foo.bar_backup for ds=2026-02-01.`

### 6.14 Alter table (`alter_table`)
`Add column risk_level string to foo.bar.`

### 6.15 Field length check (`check_field_len`)
`Find the longest values in column address from foo.bar for ds=2026-02-01.`

### 6.16 Group top N (`group_top_n`)
`Get top 3 rows per cust_id ordered by updated_at from foo.bar for ds=2026-02-01.`

### 6.17 Batch count (`batch_data_num`)
`Count rows for table_a, table_b, and table_c on ds=2026-02-01.`

### 6.18 HDFS size (`hdfs_du`)
`Check the HDFS size of imd_aml_safe.table_a for ds=2026-02-01.`

---

## 7. Tips and troubleshooting

- Prefer `db.table` over bare table names when possible.
- If the system cannot discover compare columns automatically, provide `compare_columns` explicitly.
- If the table has multiple partition fields, provide all required fields.
- If you need deterministic replay, prefer YAML over free-form natural language.
