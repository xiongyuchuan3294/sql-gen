-- Field Distribution Query
SELECT
  {{ params.columns | join(', ') }},
  COUNT(1) as cnt
FROM {{ params.table_name }}
WHERE {{ params.partition }}
GROUP BY {{ params.columns | join(', ') }}
ORDER BY cnt DESC
LIMIT {{ params.limit | default(100) }};
