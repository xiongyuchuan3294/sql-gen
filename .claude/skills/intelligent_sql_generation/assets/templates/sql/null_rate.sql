-- Null Rate Analysis Query
SELECT
  count(*) as total_count,
{% for col in params.columns -%}
  sum(if({{ col }} is null, 1, 0)) as {{ col }}_null_cnt,
  sum(if({{ col }} is null, 1, 0))/count(*) as {{ col }}_null_rate{{ ",\n" if not loop.last }}
{%- endfor %}
FROM {{ params.table_name }}
WHERE {{ params.partition }};
