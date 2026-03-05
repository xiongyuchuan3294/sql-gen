-- Group Top N Query
SELECT * FROM (
  SELECT
    *,
    row_number() over (partition by name order by id DESC) as rn
  FROM imd_aml_safe.t_local_hs2_aml_safe_demo
  WHERE ds='2026-02-01'
) t WHERE rn <= 3;