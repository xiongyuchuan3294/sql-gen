-- Data Cleaning (Overwrite with Filter)
INSERT OVERWRITE TABLE imd_aml_safe.t_local_hs2_aml_safe_demo PARTITION (ds='2026-02-01')
SELECT
  id, name
FROM imd_aml_safe.t_local_hs2_aml_safe_demo
WHERE ds='2026-02-01'
  AND (id IS NOT NULL);