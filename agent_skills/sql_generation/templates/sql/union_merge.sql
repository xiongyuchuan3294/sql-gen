-- Union Merge Query
{% for q in params.queries -%}
SELECT {{ q.columns | join(', ') }}
FROM {{ q.table_name }}
WHERE {{ q.partition }} AND {{ q.condition | default('1=1') }}
{% if not loop.last %}{{ params.union_type }}{% endif %}
{% endfor %};
