-- Null Rate Analysis Query
SELECT
  count(*) as total_count,
sum(if(user_id is null, 1, 0)) as user_id_null_cnt,
  sum(if(user_id is null, 1, 0))/count(*) as user_id_null_rate,
sum(if(email is null, 1, 0)) as email_null_cnt,
  sum(if(email is null, 1, 0))/count(*) as email_null_rate
FROM example_table
WHERE ds='2025-01-01';