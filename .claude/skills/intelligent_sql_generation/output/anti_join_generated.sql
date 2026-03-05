-- Anti Join (Missing Records) Query
SELECT
  t1.*
FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds t1
LEFT JOIN imd_aml_safe.t_local_hs2_aml_safe_p_ds t2 ON t1.cust_id = t2.cust_id
WHERE ds='2026-02-01'
  AND t2.cust_id IS NULL
  AND t2.ds='2026-02-01'; -- Auto-adapt partition for t2 alias