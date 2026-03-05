-- ================================================================
-- 数据差异对比脚本
-- 对比表: imd_aml_safe.t_local_hs2_aml_safe_p_ds vs imd_aml_safe.t_local_hs2_amlai_ads_safe_p_ds
-- 主键: cust_id
-- 分区: ds='2026-02-01', ds='2026-02-02'
-- ================================================================

-- 设置变量
set hive.exec.dynamic.mode=true;
set hive.exec.dynamic.partition=true;

-- ================================================================
-- 第一步: 预览两个表的行数
-- ================================================================
SELECT '表1 t_local_hs2_aml_safe_p_ds 行数' as description,
       COUNT(*) as row_count,
       ds
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds
WHERE ds IN ('2026-02-01', '2026-02-02')
GROUP BY ds
ORDER BY ds;

SELECT '表2 t_local_hs2_amlai_ads_safe_p_ds 行数' as description,
       COUNT(*) as row_count,
       ds
FROM imd_aml_safe.t_local_hs2_amlai_ads_safe_p_ds
WHERE ds IN ('2026-02-01', '2026-02-02')
GROUP BY ds
ORDER BY ds;

-- ================================================================
-- 第二步: 在表1中存在但表2中不存在的记录 (根据主键 cust_id)
-- ================================================================
SELECT '表1独有' as diff_type,
       a.*
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
LEFT JOIN imd_aml_safe.t_local_hs2_amlai_ads_safe_p_ds b
ON a.cust_id = b.cust_id AND a.ds = b.ds
WHERE a.ds IN ('2026-02-01', '2026-02-02')
  AND b.cust_id IS NULL
ORDER BY a.ds, a.cust_id
LIMIT 1000;

-- ================================================================
-- 第三步: 在表2中存在但表1中不存在的记录 (根据主键 cust_id)
-- ================================================================
SELECT '表2独有' as diff_type,
       b.*
FROM imd_aml_safe.t_local_hs2_amlai_ads_safe_p_ds b
LEFT JOIN imd_aml_safe.t_local_hs2_aml_safe_p_ds a
ON b.cust_id = a.cust_id AND b.ds = a.ds
WHERE b.ds IN ('2026-02-01', '2026-02-02')
  AND a.cust_id IS NULL
ORDER BY b.ds, b.cust_id
LIMIT 1000;

-- ================================================================
-- 第四步: 统计差异汇总
-- ================================================================
SELECT ds,
       '表1独有' as diff_type,
       COUNT(*) as count
FROM (
    SELECT a.cust_id, a.ds
    FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
    LEFT JOIN imd_aml_safe.t_local_hs2_amlai_ads_safe_p_ds b
    ON a.cust_id = b.cust_id AND a.ds = b.ds
    WHERE a.ds IN ('2026-02-01', '2026-02-02')
      AND b.cust_id IS NULL
) t
GROUP BY ds

UNION ALL

SELECT ds,
       '表2独有' as diff_type,
       COUNT(*) as count
FROM (
    SELECT b.cust_id, b.ds
    FROM imd_aml_safe.t_local_hs2_amlai_ads_safe_p_ds b
    LEFT JOIN imd_aml_safe.t_local_hs2_aml_safe_p_ds a
    ON b.cust_id = a.cust_id AND b.ds = a.ds
    WHERE b.ds IN ('2026-02-01', '2026-02-02')
      AND a.cust_id IS NULL
) t
GROUP BY ds

UNION ALL

SELECT ds,
       '两表交集' as diff_type,
       COUNT(*) as count
FROM (
    SELECT a.cust_id, a.ds
    FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
    INNER JOIN imd_aml_safe.t_local_hs2_amlai_ads_safe_p_ds b
    ON a.cust_id = b.cust_id AND a.ds = b.ds
    WHERE a.ds IN ('2026-02-01', '2026-02-02')
    GROUP BY a.cust_id, a.ds
) t
GROUP BY ds

ORDER BY ds, diff_type;

-- ================================================================
-- 第五步: 对比两表交集记录中的字段差异
-- ================================================================
-- 注意: 此部分需要根据实际表结构调整字段列表
-- ================================================================

-- 获取两表的列信息
SELECT 'SHOW COLUMNS FROM t_local_hs2_aml_safe_p_ds' as info;

SHOW COLUMNS FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds;

SELECT 'SHOW COLUMNS FROM t_local_hs2_amlai_ads_safe_p_ds' as info;

SHOW COLUMNS FROM imd_aml_safe.t_local_hs2_amlai_ads_safe_p_ds;

-- ================================================================
-- 第六步: 通用字段对比 (排除分区字段)
-- ================================================================
-- 假设两表结构相同，使用 FULL OUTER JOIN 对比所有非分区字段
-- ================================================================

-- 创建临时视图存储需要对比的列 (排除 cust_id 和 ds)
-- 需要根据实际表结构调整以下查询

SELECT
    COALESCE(a.cust_id, b.cust_id) as cust_id,
    COALESCE(a.ds, b.ds) as ds,
    CASE
        WHEN a.cust_id IS NULL THEN '仅在表2中存在'
        WHEN b.cust_id IS NULL THEN '仅在表1中存在'
        ELSE '两表都存在'
    END as existence_status,
    -- 示例字段对比 (需要根据实际表结构调整)
    -- CASE WHEN a.column1 != b.column1 THEN '不同' ELSE '相同' END as column1_diff,
    -- CASE WHEN a.column2 != b.column2 THEN '不同' ELSE '相同' END as column2_diff
FROM imd_aml_safe.t_local_hs2_aml_safe_p_ds a
FULL OUTER JOIN imd_aml_safe.t_local_hs2_amlai_ads_safe_p_ds b
ON a.cust_id = b.cust_id AND a.ds = b.ds
WHERE COALESCE(a.ds, b.ds) IN ('2026-02-01', '2026-02-02')
ORDER BY COALESCE(a.ds, b.ds), COALESCE(a.cust_id, b.cust_id)
LIMIT 100;
