-- Data Diff Comparison Query
SELECT count(1)
FROM (
  SELECT {{ params.join_keys | join(',') }}, {{ params.compare_columns | join(',') }}
  FROM {{ params.source_table }}
  WHERE {{ params.source_partition }}
) t1
FULL OUTER JOIN (
  SELECT {{ params.join_keys | join(',') }}, {{ params.compare_columns | join(',') }}
  FROM {{ params.target_table }}
  WHERE {{ params.target_partition }}
) t2 ON {% for key in params.join_keys %}t1.{{key}} = t2.{{key}}{% if not loop.last %} AND {% endif %}{% endfor %}
WHERE
{% for col in params.compare_columns -%}
  nvl(t1.{{col}}, '#null') <> nvl(t2.{{col}}, '#null') OR
{% endfor -%}
  t1.{{ params.join_keys[0] }} IS NULL OR t2.{{ params.join_keys[0] }} IS NULL;
