-- Check Field Length Query
SELECT
  length(cast({{ params.column }} as string)) as len,
  {{ params.column }}
FROM {{ params.table_name }}
WHERE {{ params.partition }}
ORDER BY length(cast({{ params.column }} as string)) DESC
LIMIT {{ params.limit | default(5) }};
