-- Union Merge Query
SELECT id, name
FROM imd_aml_safe.t_local_hs2_aml_safe_demo
WHERE ds='2026-02-01' AND name='test'
UNION ALL
SELECT id, name
FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_demo
WHERE ds='2026-02-01' AND name='test'

;