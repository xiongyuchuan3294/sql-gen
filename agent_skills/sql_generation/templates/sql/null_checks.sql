-- Null Value Check Query
SELECT
{% for col in params.columns -%}
  COUNT(CASE WHEN {{ col }} IS NULL THEN 1 END) as {{ col }}_null_count{{ ",\n" if not loop.last }}
{%- endfor %}
FROM {{ params.table_name }}
WHERE {{ params.partition }};
