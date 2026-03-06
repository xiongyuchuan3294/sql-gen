-- Step: probe_table_metadata
-- Metadata Probe Query
-- Purpose:
-- 1) verify table can be discovered
-- 2) inspect schema/partition metadata
-- 3) verify requested partition

SHOW TABLES LIKE 't_local_hs2_aml300_ads_safe_p_ds_dt';


DESCRIBE FORMATTED imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt;
SHOW PARTITIONS imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt;
SHOW PARTITIONS imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt PARTITION (ds='2026-02-01');


-- Step: count_partition
-- Data Count Query
SELECT COUNT(1) as total_count
FROM imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt
WHERE ds='2026-02-01';

-- Step: null_checks
-- Null Value Check Query
SELECT
COUNT(CASE WHEN cust_id IS NULL THEN 1 END) as cust_id_null_count,
COUNT(CASE WHEN status IS NULL THEN 1 END) as status_null_count
FROM imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt
WHERE ds='2026-02-01';

-- Step: null_rate
-- Null Rate Analysis Query
SELECT
  count(*) as total_count,
sum(if(cust_id is null, 1, 0)) as cust_id_null_cnt,
  sum(if(cust_id is null, 1, 0))/count(*) as cust_id_null_rate,
sum(if(status is null, 1, 0)) as status_null_cnt,
  sum(if(status is null, 1, 0))/count(*) as status_null_rate
FROM imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt
WHERE ds='2026-02-01';

-- Step: duplicate_key_check
-- Duplicate Key Check Query
SELECT
  cust_id,
  COUNT(1) as duplicate_count
FROM imd_aml300_ads_safe.t_local_hs2_aml300_ads_safe_p_ds_dt
WHERE ds='2026-02-01'
GROUP BY cust_id
HAVING COUNT(1) > 1;