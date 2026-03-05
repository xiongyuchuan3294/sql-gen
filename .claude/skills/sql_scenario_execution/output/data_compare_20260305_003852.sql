-- Step: 创建 temp 分区并复制数据
-- Move partition data to target partition
INSERT OVERWRITE TABLE imd_aml_safe.t_sql_hdfs_smoke PARTITION (ds='2026-02-01-temp')
SELECT * FROM imd_aml_safe.t_sql_hdfs_smoke PARTITION (ds='2026-02-01');

-- Step: 统计原分区数据量
-- Data Count Query
SELECT COUNT(1) as total_count
FROM imd_aml_safe.t_sql_hdfs_smoke
WHERE ds='2026-02-01';

-- Step: 统计 temp 分区数据量
-- Data Count Query
SELECT COUNT(1) as total_count
FROM imd_aml_safe.t_sql_hdfs_smoke
WHERE ds='2026-02-01-temp';

-- Step: 对比原分区和 temp 分区的差异
-- Data Diff Comparison Query
SELECT count(1)
FROM (
  SELECT cust_id, *
  FROM imd_aml_safe.t_sql_hdfs_smoke
  WHERE ds='2026-02-01'
) t1
FULL OUTER JOIN (
  SELECT cust_id, *
  FROM imd_aml_safe.t_sql_hdfs_smoke
  WHERE ds='2026-02-01-temp'
) t2 ON t1.cust_id = t2.cust_id
WHERE
nvl(t1.*, '#null') <> nvl(t2.*, '#null') OR
t1.cust_id IS NULL OR t2.cust_id IS NULL;