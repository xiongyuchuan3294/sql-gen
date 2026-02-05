#!/bin/bash
# Check Size (Batch)
{% for t in params.targets -%}
  {%- if t.path -%}
hadoop fs -du -h {{ t.path }};
  {%- else -%}
hadoop fs -du -h /user/hive/warehouse/{{ t.user | default('hduser1009') }}/{{ t.db }}.db/{{ t.table }}/{{ t.partition | replace("'", "") }};
  {%- endif %}
{% endfor %}
