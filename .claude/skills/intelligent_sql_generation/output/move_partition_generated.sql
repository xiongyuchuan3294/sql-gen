-- Move partition data to target partition
INSERT OVERWRITE TABLE imd_aml_safe.t_test_partition PARTITION (ds='2026-02-01-temp')
SELECT * FROM imd_aml_safe.t_test_partition WHERE ds='2026-02-01';