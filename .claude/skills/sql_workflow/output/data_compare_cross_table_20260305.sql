-- =====================================================
-- 对比 imd_aml_safe.t_local_hs2_aml_safe_p_ds 和 imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds
-- 分区: ds='2026-02-01' 和 ds='2026-02-02'
-- 主键: cust_id
-- =====================================================

-- =====================================================
-- 1. ds='2026-02-01' 分区数据对比
-- =====================================================

-- 1.1 统计两个表的记录数
SELECT 'imd_aml_safe.ds=2026-02-01' AS table_info, COUNT(*) AS cnt
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
WHERE ds = '2026-02-01'

UNION ALL

SELECT 'imd_amlai_ads_safe.ds=2026-02-01' AS table_info, COUNT(*) AS cnt
FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds
WHERE ds = '2026-02-01';

-- 1.2 在 imd_aml_safe 但不在 imd_amlai_ads_safe 的记录 (ds='2026-02-01')
SELECT '仅在 imd_aml_safe' AS diff_type, a.*
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
WHERE a.ds = '2026-02-01'
  AND NOT EXISTS (
    SELECT 1 FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds b
    WHERE b.cust_id = a.cust_id AND b.ds = '2026-02-01'
  );

-- 1.3 在 imd_amlai_ads_safe 但不在 imd_aml_safe 的记录 (ds='2026-02-01')
SELECT '仅在 imd_amlai_ads_safe' AS diff_type, b.*
FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds b
WHERE b.ds = '2026-02-01'
  AND NOT EXISTS (
    SELECT 1 FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
    WHERE a.cust_id = b.cust_id AND a.ds = '2026-02-01'
  );

-- 1.4 数据差异对比 - 相同 cust_id 但 amount 或 note 不同 (ds='2026-02-01')
SELECT '数据差异' AS diff_type, a.cust_id, a.amount AS amount_safe, b.amount AS amount_ads_safe,
       a.note AS note_safe, b.note AS note_ads_safe
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
JOIN imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds b
  ON a.cust_id = b.cust_id AND a.ds = '2026-02-01' AND b.ds = '2026-02-01'
WHERE a.amount <> b.amount OR a.note <> b.note;


-- =====================================================
-- 2. ds='2026-02-02' 分区数据对比
-- =====================================================

-- 2.1 统计两个表的记录数
SELECT 'imd_aml_safe.ds=2026-02-02' AS table_info, COUNT(*) AS cnt
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
WHERE ds = '2026-02-02'

UNION ALL

SELECT 'imd_amlai_ads_safe.ds=2026-02-02' AS table_info, COUNT(*) AS cnt
FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds
WHERE ds = '2026-02-02';

-- 2.2 在 imd_aml_safe 但不在 imd_amlai_ads_safe 的记录 (ds='2026-02-02')
SELECT '仅在 imd_aml_safe' AS diff_type, a.*
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
WHERE a.ds = '2026-02-02'
  AND NOT EXISTS (
    SELECT 1 FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds b
    WHERE b.cust_id = a.cust_id AND b.ds = '2026-02-02'
  );

-- 2.3 在 imd_amlai_ads_safe 但不在 imd_aml_safe 的记录 (ds='2026-02-02')
SELECT '仅在 imd_amlai_ads_safe' AS diff_type, b.*
FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds b
WHERE b.ds = '2026-02-02'
  AND NOT EXISTS (
    SELECT 1 FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
    WHERE a.cust_id = b.cust_id AND a.ds = '2026-02-02'
  );

-- 2.4 数据差异对比 - 相同 cust_id 但 amount 或 note 不同 (ds='2026-02-02')
SELECT '数据差异' AS diff_type, a.cust_id, a.amount AS amount_safe, b.amount AS amount_ads_safe,
       a.note AS note_safe, b.note AS note_ads_safe
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
JOIN imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds b
  ON a.cust_id = b.cust_id AND a.ds = '2026-02-02' AND b.ds = '2026-02-02'
WHERE a.amount <> b.amount OR a.note <> b.note;
