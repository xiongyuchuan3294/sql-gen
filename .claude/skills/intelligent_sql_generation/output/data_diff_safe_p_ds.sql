-- ====================================================================
-- 数据差异对比 SQL
-- 对比 imd_aml_safe.t_local_hs2_aml_safe_p_ds 和 imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds
-- 分区: ds='2026-02-01', ds='2026-02-02'
-- 主键: cust_id
-- 对比字段: cust_id, amount, note
-- ====================================================================

-- 1. 只存在于 pds 表的记录（ads 表中不存在）
-- ====================================================================
SELECT
    'only_in_pds' AS diff_type,
    cust_id,
    amount,
    note,
    ds,
    NULL AS compare_cust_id,
    NULL AS compare_amount,
    NULL AS compare_note
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
WHERE ds IN ('2026-02-01', '2026-02-02')
  AND cust_id NOT IN (
    SELECT cust_id
    FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds
    WHERE ds IN ('2026-02-01', '2026-02-02')
  );

-- 2. 只存在于 ads 表的记录（pds 表中不存在）
-- ====================================================================
SELECT
    'only_in_ads' AS diff_type,
    cust_id,
    amount,
    note,
    ds,
    NULL AS compare_cust_id,
    NULL AS compare_amount,
    NULL AS compare_note
FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds
WHERE ds IN ('2026-02-01', '2026-02-02')
  AND cust_id NOT IN (
    SELECT cust_id
    FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
    WHERE ds IN ('2026-02-01', '2026-02-02')
  );

-- 3. 两表都存在但有字段值差异的记录
-- ====================================================================
SELECT
    'value_diff' AS diff_type,
    t1.cust_id,
    t1.amount,
    t1.note,
    t1.ds,
    t2.cust_id AS compare_cust_id,
    t2.amount AS compare_amount,
    t2.note AS compare_note,
    CASE
        WHEN t1.amount != t2.amount THEN 'amount'
        WHEN t1.note != t2.note THEN 'note'
        WHEN t1.ds != t2.ds THEN 'ds'
    END AS diff_field
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds t1
JOIN imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds t2
  ON t1.cust_id = t2.cust_id AND t1.ds = t2.ds
WHERE t1.ds IN ('2026-02-01', '2026-02-02')
  AND (
      t1.amount != t2.amount
      OR t1.note != t2.note
      OR t1.ds != t2.ds
  );

-- 4. 差异汇总统计
-- ====================================================================
SELECT
    'summary' AS summary_type,
    (SELECT COUNT(*) FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds WHERE ds IN ('2026-02-01', '2026-02-02')) AS pds_total_count,
    (SELECT COUNT(*) FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds WHERE ds IN ('2026-02-01', '2026-02-02')) AS ads_total_count,
    (SELECT COUNT(*)
     FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds t1
     WHERE t1.ds IN ('2026-02-01', '2026-02-02')
       AND t1.cust_id NOT IN (
           SELECT cust_id
           FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds
           WHERE ds IN ('2026-02-01', '2026-02-02')
       )
    ) AS only_in_pds_count,
    (SELECT COUNT(*)
     FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds t2
     WHERE t2.ds IN ('2026-02-01', '2026-02-02')
       AND t2.cust_id NOT IN (
           SELECT cust_id
           FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
           WHERE ds IN ('2026-02-01', '2026-02-02')
       )
    ) AS only_in_ads_count,
    (SELECT COUNT(*)
     FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds t1
     JOIN imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds t2
       ON t1.cust_id = t2.cust_id AND t1.ds = t2.ds
     WHERE t1.ds IN ('2026-02-01', '2026-02-02')
       AND (t1.amount != t2.amount OR t1.note != t2.note OR t1.ds != t2.ds)
    ) AS value_diff_count,
    (SELECT COUNT(*)
     FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds t1
     JOIN imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds t2
       ON t1.cust_id = t2.cust_id AND t1.ds = t2.ds
     WHERE t1.ds IN ('2026-02-01', '2026-02-02')
       AND t1.amount = t2.amount AND t1.note = t2.note
    ) AS match_count;
