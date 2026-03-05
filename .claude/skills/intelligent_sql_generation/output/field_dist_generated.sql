-- Field Distribution Query
SELECT
  note,
  COUNT(1) as cnt
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
WHERE ds='2026-02-01'
GROUP BY note
ORDER BY cnt DESC
LIMIT 20;