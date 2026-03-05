-- Data Diff Comparison Query
-- 分区表: 自动获取非分区字段进行对比，WHERE 添加分区过滤
-- 非分区表: 获取所有字段，WHERE 不添加分区过滤
SELECT count(1)
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