-- Data Count Query
SELECT COUNT(1) as total_count
FROM {{ params.table_name }}
WHERE {{ params.partition }};
