-- Batch Data Count Query
SELECT 'imd_aml_safe.t_local_hs2_aml_safe_demo' as table_name, COUNT(1) as total_count FROM imd_aml_safe.t_local_hs2_aml_safe_demo WHERE ds='2026-02-01'
UNION ALL
SELECT 'imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_demo' as table_name, COUNT(1) as total_count FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_demo WHERE ds='2026-02-01'
;