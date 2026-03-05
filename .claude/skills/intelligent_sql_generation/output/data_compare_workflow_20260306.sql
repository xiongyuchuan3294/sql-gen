-- ====================================================================
-- 对账工作流 (Data Compare Workflow)
-- 生成时间: 2026-03-06
-- 表: imd_aml_safe.t_test_partition
-- 分区: ds='2026-02-01'
-- 主键: id
-- ====================================================================

-- --------------------------------------------------------------------
-- Step 1: 移动分区到临时表
-- 说明: 将原分区数据复制到临时分区，用于后续对比
-- --------------------------------------------------------------------
INSERT OVERWRITE TABLE imd_aml_safe.t_test_partition PARTITION (ds='2026-02-01-temp')
SELECT * FROM imd_aml_safe.t_test_partition WHERE ds='2026-02-01';

-- --------------------------------------------------------------------
-- Step 2: 统计原分区数据量
-- 说明: 检查原分区的记录数
-- --------------------------------------------------------------------
SELECT COUNT(1) as original_partition_count
FROM imd_aml_safe.t_test_partition
WHERE ds='2026-02-01';

-- --------------------------------------------------------------------
-- Step 3: 统计临时分区数据量
-- 说明: 检查临时分区的记录数
-- --------------------------------------------------------------------
SELECT COUNT(1) as temp_partition_count
FROM imd_aml_safe.t_test_partition
WHERE ds='2026-02-01-temp';

-- --------------------------------------------------------------------
-- Step 4: 对比差异
-- 说明: 使用 FULL OUTER JOIN 对比原分区和临时分区的数据差异
-- --------------------------------------------------------------------
SELECT count(1) as diff_count
FROM (
  SELECT id, name, note
  FROM imd_aml_safe.t_test_partition
  WHERE ds='2026-02-01'
) t1
FULL OUTER JOIN (
  SELECT id, name, note
  FROM imd_aml_safe.t_test_partition
  WHERE ds='2026-02-01-temp'
) t2 ON t1.id = t2.id
WHERE
nvl(t1.id, '#null') <> nvl(t2.id, '#null') OR
nvl(t1.name, '#null') <> nvl(t2.name, '#null') OR
nvl(t1.note, '#null') <> nvl(t2.note, '#null')
OR t1.id IS NULL OR t2.id IS NULL
;

-- ====================================================================
-- 执行说明:
-- 1. 按顺序执行 Step 1 -> Step 4
-- 2. Step 1 的结果用于创建备份
-- 3. Step 2 和 Step 3 验证数据量是否一致
-- 4. Step 4 返回差异记录数，0 表示无差异
-- ====================================================================
