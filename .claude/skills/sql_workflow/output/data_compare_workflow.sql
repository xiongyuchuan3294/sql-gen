-- =====================================================================
-- SQL 场景: 数据对比 (Data Compare)
-- 目标: 对比 t_test_partition 分区数据差异
-- 主键: id
-- =====================================================================

-- ---------------------------------------------------------------------
-- Step 1: 创建临时分区并复制数据
-- 从 ds='2026-02-01' 复制到 ds='2026-02-01-temp'
-- ---------------------------------------------------------------------
INSERT OVERWRITE TABLE t_test_partition PARTITION (ds='2026-02-01-temp')
SELECT * FROM t_test_partition WHERE ds='2026-02-01';


-- ---------------------------------------------------------------------
-- Step 2: 统计原分区数据量
-- ---------------------------------------------------------------------
SELECT COUNT(1) as source_count
FROM t_test_partition
WHERE ds='2026-02-01';


-- ---------------------------------------------------------------------
-- Step 3: 统计临时分区数据量
-- ---------------------------------------------------------------------
SELECT COUNT(1) as temp_count
FROM t_test_partition
WHERE ds='2026-02-01-temp';


-- ---------------------------------------------------------------------
-- Step 4: 对比两个分区的数据差异
-- 主键: id
-- ---------------------------------------------------------------------
SELECT count(1) as diff_count
FROM (
  SELECT id, name, note
  FROM t_test_partition
  WHERE ds='2026-02-01'
) t1
FULL OUTER JOIN (
  SELECT id, name, note
  FROM t_test_partition
  WHERE ds='2026-02-01-temp'
) t2 ON t1.id = t2.id
WHERE
nvl(t1.id, '#null') <> nvl(t2.id, '#null') OR
nvl(t1.name, '#null') <> nvl(t2.name, '#null') OR
nvl(t1.note, '#null') <> nvl(t2.note, '#null')
OR t1.id IS NULL OR t2.id IS NULL
;

-- ---------------------------------------------------------------------
-- 清理: 删除临时分区 (可选)
-- ---------------------------------------------------------------------
-- ALTER TABLE t_test_partition DROP IF EXISTS PARTITION (ds='2026-02-01-temp');
