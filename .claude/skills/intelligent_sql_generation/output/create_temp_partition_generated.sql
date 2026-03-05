-- Create empty temp partition
ALTER TABLE imd_aml_safe.t_local_hs2_aml_safe_demo ADD PARTITION (ds='2026-02-01-temp');