-- Batch Data Count Query
{% for t in params.tables -%}
SELECT '{{ t.name }}' as table_name, COUNT(1) as total_count FROM {{ t.name }} WHERE {{ t.partition }}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %};
