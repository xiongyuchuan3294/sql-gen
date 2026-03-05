-- Data Diff Comparison Query
-- 分区表: 自动获取非分区字段进行对比，WHERE 添加分区过滤
-- 非分区表: 获取所有字段，WHERE 不添加分区过滤
SELECT count(1)
FROM (
  SELECT {{ params.compare_columns | join(', ') }}
  FROM {{ params.source_table }}
  {% if params.source_partition %}WHERE {{ params.source_partition }}{% endif %}
) t1
FULL OUTER JOIN (
  SELECT {{ params.compare_columns | join(', ') }}
  FROM {{ params.target_table }}
  {% if params.target_partition %}WHERE {{ params.target_partition }}{% endif %}
) t2 ON {% for key in params.join_keys %}t1.{{key}} = t2.{{key}}{% if not loop.last %} AND {% endif %}{% endfor %}
WHERE
{% for col in params.compare_columns -%}
  nvl(t1.{{col}}, '#null') <> nvl(t2.{{col}}, '#null'){% if not loop.last %} OR
{% endif %}{% endfor %}
{% for key in params.join_keys -%}
  OR t1.{{key}} IS NULL OR t2.{{key}} IS NULL
{% endfor %};
