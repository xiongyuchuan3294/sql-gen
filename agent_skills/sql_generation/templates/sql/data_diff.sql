-- Data Diff Comparison Query
-- 自动获取非分区字段进行对比
SELECT count(1)
FROM (
  SELECT {{ params.non_partition_columns | join(', ') }}
  FROM {{ params.source_table }}
  WHERE {{ params.source_partition }}
) t1
FULL OUTER JOIN (
  SELECT {{ params.non_partition_columns | join(', ') }}
  FROM {{ params.target_table }}
  WHERE {{ params.target_partition }}
) t2 ON {% for key in params.join_keys %}t1.{{key}} = t2.{{key}}{% if not loop.last %} AND {% endif %}{% endfor %}
WHERE
{% for col in params.non_partition_columns -%}
  nvl(t1.{{col}}, '#null') <> nvl(t2.{{col}}, '#null'){% if not loop.last %} OR
{% endif %}{% endfor %}
{% for key in params.join_keys -%}
  OR t1.{{key}} IS NULL OR t2.{{key}} IS NULL
{% endfor %};
