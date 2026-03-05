-- Null Rate Analysis Query
SELECT
  count(*) as total_count,
sum(if(cust_id is null, 1, 0)) as cust_id_null_cnt,
  sum(if(cust_id is null, 1, 0))/count(*) as cust_id_null_rate,
sum(if(amount is null, 1, 0)) as amount_null_cnt,
  sum(if(amount is null, 1, 0))/count(*) as amount_null_rate
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
WHERE ds='2026-02-01';