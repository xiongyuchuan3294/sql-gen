-- Move partition data to target partition
INSERT OVERWRITE TABLE {{ params.table_name }} PARTITION ({{ params.target_partition }})
SELECT * FROM {{ params.table_name }} PARTITION ({{ params.source_partition }});
