-- Step: probe_table_metadata
-- Metadata Probe Query
-- Purpose:
-- 1) verify table can be discovered
-- 2) inspect schema/partition metadata
-- 3) verify requested partition

SHOW TABLES LIKE 't_test_partition';


DESCRIBE FORMATTED imd_aml_safe.t_test_partition;
SHOW PARTITIONS imd_aml_safe.t_test_partition;
SHOW PARTITIONS imd_aml_safe.t_test_partition PARTITION (ds='2026-02-01');


-- Step: count_partition
-- Data Count Query
SELECT COUNT(1) as total_count
FROM imd_aml_safe.t_test_partition
WHERE ds='2026-02-01';

-- Step: null_checks
-- Null Value Check Query
SELECT
COUNT(CASE WHEN id IS NULL THEN 1 END) as id_null_count
FROM imd_aml_safe.t_test_partition
WHERE ds='2026-02-01';

-- Step: null_rate
-- Null Rate Analysis Query
SELECT
  count(*) as total_count,
sum(if(id is null, 1, 0)) as id_null_cnt,
  sum(if(id is null, 1, 0))/count(*) as id_null_rate
FROM imd_aml_safe.t_test_partition
WHERE ds='2026-02-01';

-- Step: duplicate_key_check
-- Duplicate Key Check Query
SELECT
  id,
  COUNT(1) as duplicate_count
FROM imd_aml_safe.t_test_partition
WHERE ds='2026-02-01'
GROUP BY id
HAVING COUNT(1) > 1;