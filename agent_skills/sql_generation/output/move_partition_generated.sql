-- Move partition data to target partition
INSERT OVERWRITE TABLE example_table PARTITION (ds='2026-02-01-temp')
SELECT * FROM example_table PARTITION (ds='2026-02-01');