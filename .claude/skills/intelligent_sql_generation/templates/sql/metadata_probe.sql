-- Metadata Probe Query
-- Purpose:
-- 1) verify table can be discovered
-- 2) inspect schema/partition metadata
-- 3) verify requested partition

SHOW TABLES LIKE '{{ params.table_name }}';

{% if params.db %}
DESCRIBE FORMATTED {{ params.db }}.{{ params.table_name }};
SHOW PARTITIONS {{ params.db }}.{{ params.table_name }};
SHOW PARTITIONS {{ params.db }}.{{ params.table_name }} PARTITION ({{ params.partition }});
{% else %}
DESCRIBE FORMATTED {{ params.table_name_full }};
SHOW PARTITIONS {{ params.table_name_full }};
SHOW PARTITIONS {{ params.table_name_full }} PARTITION ({{ params.partition }});
{% endif %}
