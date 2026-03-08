-- Anti Join (Missing Records) Query
SELECT
  t1.*
FROM {{ params.source_table }} t1
LEFT JOIN {{ params.target_table }} t2 ON {% for key in params.join_keys %}t1.{{key}} = t2.{{key}}{% if not loop.last %} AND {% endif %}{% endfor %}
WHERE {{ params.source_partition }}
  AND t2.{{ params.join_keys[0] }} IS NULL
  AND {{ params.target_partition | replace('ds=', 't2.ds=') }}; -- Auto-adapt partition for t2 alias
