-- 在 imd_aml_safe 库中创建分区表
-- 分区字段: ds (string)
-- 存储格式: ORC

DROP TABLE IF EXISTS imd_aml_safe.t_test_partition;

CREATE TABLE IF NOT EXISTS imd_aml_safe.t_test_partition (
    id int COMMENT '主键ID',
    name string COMMENT '名称',
    note string COMMENT '备注'
)
PARTITIONED BY (ds string COMMENT '日期分区')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'transactional'='false'
);
