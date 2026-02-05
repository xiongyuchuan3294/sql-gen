-- Group Top N Query
SELECT * FROM (
  SELECT
    *,
    row_number() over (partition by class order by score DESC) as rn
  FROM example_table
  WHERE ds='2025-01-01'
) t WHERE rn <= 3;