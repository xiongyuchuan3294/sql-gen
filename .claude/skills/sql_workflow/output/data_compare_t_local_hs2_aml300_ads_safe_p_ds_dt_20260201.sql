-- Step: copy_source_partition_to_temp
-- Move partition data to target partition
INSERT OVERWRITE TABLE imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt PARTITION (ds='2026-02-01-temp')
SELECT * FROM imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt WHERE ds='2026-02-01';

-- Step: count_source_partition
-- Data Count Query
SELECT COUNT(1) as total_count
FROM imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt
WHERE ds='2026-02-01';

-- Step: count_temp_partition
-- Data Count Query
SELECT COUNT(1) as total_count
FROM imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt
WHERE ds='2026-02-01-temp';

-- Step: diff_source_vs_temp
-- Data Diff Comparison Query
-- 分区表: 自动获取非分区字段进行对比，WHERE 添加分区过滤
-- 非分区表: 获取所有字段，WHERE 不添加分区过滤
SELECT count(1)
FROM (
  SELECT cust_id
  FROM imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt
  WHERE ds='2026-02-01'
) t1
FULL OUTER JOIN (
  SELECT cust_id
  FROM imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt
  WHERE ds='2026-02-01-temp'
) t2 ON t1.cust_id = t2.cust_id
WHERE
nvl(t1.cust_id, '#null') <> nvl(t2.cust_id, '#null')
OR t1.cust_id IS NULL OR t2.cust_id IS NULL
;