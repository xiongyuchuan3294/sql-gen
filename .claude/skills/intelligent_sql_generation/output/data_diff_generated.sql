-- Data Diff Comparison Query
-- 自动获取非分区字段进行对比
SELECT count(1)
FROM (
  SELECT cust_id, amount, note
  FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
  WHERE ds='2026-02-01'
) t1
FULL OUTER JOIN (
  SELECT cust_id, amount, note
  FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
  WHERE ds='2026-02-02'
) t2 ON t1.cust_id = t2.cust_id
WHERE
nvl(t1.cust_id, '#null') <> nvl(t2.cust_id, '#null') OR
nvl(t1.amount, '#null') <> nvl(t2.amount, '#null') OR
nvl(t1.note, '#null') <> nvl(t2.note, '#null')
OR t1.cust_id IS NULL OR t2.cust_id IS NULL
;