-- Data diff comparison query
-- Partitioned tables: compare non-partition columns and add partition filters in WHERE clauses.
-- Non-partitioned tables: compare all available columns and skip partition filters.
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
