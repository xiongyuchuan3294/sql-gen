-- Duplicate Key Check Query
SELECT
  {{ params.group_by_columns | join(', ') }},
  COUNT(1) as duplicate_count
FROM {{ params.table_name }}
WHERE {{ params.partition }}
GROUP BY {{ params.group_by_columns | join(', ') }}
HAVING COUNT(1) > {{ params.having_threshold }};
