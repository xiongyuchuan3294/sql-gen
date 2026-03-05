#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '/Users/xiongyuc/workspace/sql-gen')
from tools.hive_client import JdbcHiveUtils

env = 'local_hs2_mac'

print('=== 数据差异对比 ===')
print('源表: imd_aml_safe.t_local_hs2_aml_safe_20260304')
print('目标表: imd_amlai_ads_safe.t_local_hs2_amlai_ads_20260304')
print('分区: ds=2026-02-01')
print('���键: id')
print()

# 1. 源表行数
print('=== 1. 源表行数 ===')
result = JdbcHiveUtils.execute_query('imd_aml_safe', "SELECT COUNT(*) as cnt FROM t_local_hs2_aml_safe_20260304 WHERE ds='2026-02-01'", env)
print(result)

# 2. 目标表行数
print('\n=== 2. 目标表行数 ===')
result = JdbcHiveUtils.execute_query('imd_amlai_ads_safe', "SELECT COUNT(*) as cnt FROM t_local_hs2_amlai_ads_20260304 WHERE ds='2026-02-01'", env)
print(result)

# 3. 只在源表存在
print('\n=== 3. 只在源表存在 (源有目标无) ===')
sql = """
SELECT COUNT(*) as cnt
FROM imd_aml_safe.t_local_hs2_aml_safe_20260304 s
WHERE s.ds='2026-02-01'
  AND NOT EXISTS (
      SELECT 1
      FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_20260304 t
      WHERE t.ds='2026-02-01'
        AND s.id = t.id
  )
"""
result = JdbcHiveUtils.execute_query('imd_aml_safe', sql, env)
print(result)

# 4. 只在目标表存在
print('\n=== 4. 只在目标表存在 (源无目标有) ===')
sql = """
SELECT COUNT(*) as cnt
FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_20260304 t
WHERE t.ds='2026-02-01'
  AND NOT EXISTS (
      SELECT 1
      FROM imd_aml_safe.t_local_hs2_aml_safe_20260304 s
      WHERE s.ds='2026-02-01'
        AND t.id = s.id
  )
"""
result = JdbcHiveUtils.execute_query('imd_amlai_ads_safe', sql, env)
print(result)

# 5. 两表交集
print('\n=== 5. 两表交集 (共同存在) ===')
sql = """
SELECT COUNT(*) as cnt
FROM imd_aml_safe.t_local_hs2_aml_safe_20260304 s
INNER JOIN imd_amlai_ads_safe.t_local_hs2_amlai_ads_20260304 t
  ON s.id = t.id
WHERE s.ds='2026-02-01' AND t.ds='2026-02-01'
"""
result = JdbcHiveUtils.execute_query('imd_aml_safe', sql, env)
print(result)

# 6. 查看只在源表存在的具体数据
print('\n=== 6. 只在源表存在的具体数据 (前10条) ===')
sql = """
SELECT s.id, s.note, s.created_at
FROM imd_aml_safe.t_local_hs2_aml_safe_20260304 s
WHERE s.ds='2026-02-01'
  AND NOT EXISTS (
      SELECT 1
      FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_20260304 t
      WHERE t.ds='2026-02-01'
        AND s.id = t.id
  )
LIMIT 10
"""
result = JdbcHiveUtils.execute_query('imd_aml_safe', sql, env)
print(result)

# 7. 查看只在目标表存在的具体数据
print('\n=== 7. 只在目标表存在的具体数据 (前10条) ===')
sql = """
SELECT t.id, t.note, t.created_at
FROM imd_amlai_ads_safe.t_local_hs2_amlai_ads_20260304 t
WHERE t.ds='2026-02-01'
  AND NOT EXISTS (
      SELECT 1
      FROM imd_aml_safe.t_local_hs2_aml_safe_20260304 s
      WHERE s.ds='2026-02-01'
        AND t.id = s.id
  )
LIMIT 10
"""
result = JdbcHiveUtils.execute_query('imd_amlai_ads_safe', sql, env)
print(result)

JdbcHiveUtils.close_all()
