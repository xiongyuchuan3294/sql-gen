-- Null Value Check Query
SELECT
COUNT(CASE WHEN id IS NULL THEN 1 END) as id_null_count,
COUNT(CASE WHEN name IS NULL THEN 1 END) as name_null_count
FROM imd_aml_safe.t_local_hs2_aml_safe_demo
WHERE ds='2026-02-01';