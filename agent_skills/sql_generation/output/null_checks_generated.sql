-- Null Value Check Query
SELECT
COUNT(CASE WHEN user_id IS NULL THEN 1 END) as user_id_null_count,
COUNT(CASE WHEN order_id IS NULL THEN 1 END) as order_id_null_count
FROM example_table
WHERE dt='2024-01-01';