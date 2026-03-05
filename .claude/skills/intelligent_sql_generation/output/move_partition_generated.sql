-- Move partition data to target partition
INSERT OVERWRITE TABLE imd_aml_safe.t_local_hs2_aml_safe_demo PARTITION (ds='2026-02-01-temp')
SELECT * FROM imd_aml_safe.t_local_hs2_aml_safe_demo PARTITION (ds='2026-02-01');