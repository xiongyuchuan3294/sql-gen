-- Duplicate Key Check Query
SELECT
  cust_id,
  COUNT(1) as duplicate_count
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
WHERE ds='2026-02-01'
GROUP BY cust_id
HAVING COUNT(1) > 1;