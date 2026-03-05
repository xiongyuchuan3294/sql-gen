-- Insert Values Query
INSERT INTO TABLE imd_aml_safe.t_local_hs2_aml_safe_demo PARTITION (ds='2026-02-01')
VALUES
(1, 'test1'),
(2, 'test2');