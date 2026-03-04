#!/bin/bash
# Check Size (Batch)
{% for t in params.targets -%}
hadoop fs -du -h {{ t.hdfs_path }};
{% endfor %}
