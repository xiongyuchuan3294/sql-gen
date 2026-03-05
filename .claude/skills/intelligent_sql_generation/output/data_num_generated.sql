-- Data Count Query
SELECT COUNT(1) as total_count
FROM imd_aml_safe.t_test_partition
WHERE ds='2026-02-01-temp';