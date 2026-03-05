-- Check Field Length Query
SELECT
  length(cast(name as string)) as len,
  name
FROM imd_aml_safe.t_local_hs2_aml_safe_demo
WHERE ds='2026-02-01'
ORDER BY length(cast(name as string)) DESC
LIMIT 5;