-- Field Distribution Query
SELECT
  status, type,
  COUNT(1) as cnt
FROM example_table
WHERE ds='2025-01-01'
GROUP BY status, type
ORDER BY cnt DESC
LIMIT 20;