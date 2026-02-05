-- Check Field Length Query
SELECT
  length(cast(txn_amt as string)) as len,
  txn_amt
FROM ads_aml_prod_eval_lg_case_df
WHERE ds='2024-09-30' and dt='q3'
ORDER BY length(cast(txn_amt as string)) DESC
LIMIT 5;