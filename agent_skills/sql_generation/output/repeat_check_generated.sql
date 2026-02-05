-- Duplicate Key Check Query
SELECT
  user_id,
  COUNT(1) as duplicate_count
FROM example_table
WHERE dt='2024-01-01'
GROUP BY user_id
HAVING COUNT(1) > 1;