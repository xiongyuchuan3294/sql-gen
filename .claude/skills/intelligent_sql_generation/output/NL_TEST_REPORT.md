# 自然语言测试报告

**测试日期**: 2026-03-05
**测试环境**: Hive (MCP connected)
**测试表**:
- imd_aml_safe.t_local_hs2_aml_safe_p_ds (cust_id, amount, note, ds)
- imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds (cust_id, amount, note, ds)

---

## 测试结果汇总

| 序号 | 测试场景 | 用户输入 | 模板类型 | 测试状态 | 结果 |
|------|----------|----------|----------|----------|------|
| 1 | 数据统计 | "查询 imd_aml_safe.t_local_hs2_aml_safe_p_ds 在 ds=2026-02-01 的数据量" | data_num | ✅ 通过 | 2条记录 |
| 2 | 跨表数据对比 | "对比 imd_aml_safe.t_local_hs2_aml_safe_p_ds 和 imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds 在 ds=2026-02-01 的数据差异，主键 cust_id" | data_diff | ✅ 通过 | 3条差异 |
| 3 | 空值检查 | "检查 cust_id 和 amount 的空值情况 ds=2026-02-01" | null_rate | ✅ 通过 | 无空值 |
| 4 | 重复值检查 | "检查 cust_id 是否有重复 ds=2026-02-01" | repeat_check | ✅ 通过 | 无重复 |
| 5 | 字段值分布 | "查询 note 字段的分布 ds=2026-02-01" | field_dist | ✅ 通过 | 2个不同值 |
| 6 | 反向连接 | "查找 imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds 有但 imd_aml_safe.t_local_hs2_aml_safe_p_ds 没有的记录 ds=2026-02-01 主键 cust_id" | anti_join | ✅ 通过 | 2条记录 |
| 7 | HDFS存储 | "查询 imd_aml_safe.t_local_hs2_aml_safe_p_ds 的 HDFS 存储大小" | hdfs_du | ✅ 通过 | 命令生成成功 |
| 8 | 分区对比 | "对比 imd_aml_safe.t_local_hs2_aml_safe_p_ds 在 ds=2026-02-01 和 ds=2026-02-02 分区的数据差异，主键 cust_id" | data_diff | ✅ 通过 | 5条差异 |

**总计**: 8个测试场景，全部测试通过

---

## 详细测试记录

### 测试数据

**imd_aml_safe.t_local_hs2_aml_safe_p_ds**
| cust_id | amount | note | ds |
|---------|--------|------|-----|
| C001 | 100.00 | test6-02-1 | 2026-02-01 |
| C002 | 200.50 | test2 | 2026-02-01 |
| C001 | 100.00 | test1 | 2026-02-02 |
| C004 | 400.00 | test4 | 2026-02-02 |
| C005 | 500.00 | test5 | 2026-02-02 |

**imd_amlai_ads_safe.t_local_hs2_amlai_ads_safe_p_ds**
| cust_id | amount | note | ds |
|---------|--------|------|-----|
| C001 | 100.00 | test6-02-1 | 2026-02-01 |
| C002 | 200.50 | test2_changed | 2026-02-01 |
| C003 | 300.00 | test3 | 2026-02-01 |
| C006 | 600.00 | test6 | 2026-02-01 |
| C001 | 100.00 | test1 | 2026-02-02 |
| C007 | 700.00 | test7 | 2026-02-02 |

---

## 结论

✅ **所有自然语言测试通过**

验证了以下关键能力：
1. ✅ 参数提取 - 能从自然语言中正确提取表名、分区、主键等参数
2. ✅ 模板映射 - 能根据关键词正确识别模板类型
3. ✅ SQL生成 - 生成的SQL语法正确
4. ✅ 执行验证 - 实际执行SQL返回正确结果

---

## 附录：测试用YAML文件

测试过程中生成的YAML文件保存在 `output/nl_test_*.yaml`：
- nl_test_data_num.yaml
- nl_test_data_diff.yaml
- nl_test_null_rate.yaml
- nl_test_repeat_check.yaml
- nl_test_field_dist.yaml
- nl_test_anti_join.yaml
- nl_test_hdfs_du.yaml
- nl_test_partition_diff.yaml
