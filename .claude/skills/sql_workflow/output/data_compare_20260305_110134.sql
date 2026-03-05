-- Step: 创建 temp 分区并复制数据
-- Move partition data to target partition
INSERT OVERWRITE TABLE imd_aml_safe.t_local_hs2_aml_safe_p_ds PARTITION (ds='2026-01-02 key cu-temp')
SELECT * FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds PARTITION (ds='2026-01-02 key cu');

-- Step: 统计原分区数据量
-- Data Count Query
SELECT COUNT(1) as total_count
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
WHERE ds='2026-01-02 key cu';

-- Step: 统计 temp 分区数据量
-- Data Count Query
SELECT COUNT(1) as total_count
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
WHERE ds='2026-01-02 key cu-temp';

-- Step: 对比原分区和 temp 分区的差异
-- Data Diff Comparison Query
-- 自动获取非分区字段进行对比
SELECT count(1)
FROM (
  SELECT cust_id, amount, note, ds, ds
  FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
  WHERE ds='2026-01-02 key cu'
) t1
FULL OUTER JOIN (
  SELECT cust_id, amount, note, ds, ds
  FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
  WHERE ds='2026-01-02 key cu-temp'
) t2 ON t1.cust_id = t2.cust_id
WHERE
nvl(t1.cust_id, '#null') <> nvl(t2.cust_id, '#null') OR
nvl(t1.amount, '#null') <> nvl(t2.amount, '#null') OR
nvl(t1.note, '#null') <> nvl(t2.note, '#null') OR
nvl(t1.ds, '#null') <> nvl(t2.ds, '#null') OR
nvl(t1.ds, '#null') <> nvl(t2.ds, '#null')
OR t1.cust_id IS NULL OR t2.cust_id IS NULL
;