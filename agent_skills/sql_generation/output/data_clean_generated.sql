-- Data Cleaning (Overwrite with Filter)
INSERT OVERWRITE TABLE example_table PARTITION (dt='2024-01-01')
SELECT
  id, name, status
FROM example_table
WHERE dt='2024-01-01'
  AND (user_id IS NOT NULL AND status != 'deleted');