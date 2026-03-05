-- Group Top N Query
SELECT * FROM (
  SELECT
    *,
    row_number() over (partition by {{ params.partition_by | join(', ') }} order by {{ params.order_by }}) as rn
  FROM {{ params.table_name }}
  WHERE {{ params.partition }}
) t WHERE rn <= {{ params.limit_n }};
