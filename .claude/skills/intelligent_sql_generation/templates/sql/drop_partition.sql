-- Drop Partition Query
ALTER TABLE {{ params.table_name }} DROP {% if params.if_exists %}IF EXISTS {% endif %}PARTITION ({{ params.partition }});
