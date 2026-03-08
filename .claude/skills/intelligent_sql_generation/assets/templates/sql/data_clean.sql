-- Data Cleaning (Overwrite with Filter)
INSERT OVERWRITE TABLE {{ params.table_name }} PARTITION ({{ params.partition }})
SELECT
  {% if params.columns %}{{ params.columns | join(', ') }}{% else %}*{% endif %}
FROM {{ params.table_name }}
WHERE {{ params.partition }}
  AND ({{ params.filter_condition | default('1=1') }});
