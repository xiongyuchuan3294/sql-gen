-- Create empty temp partition
ALTER TABLE {{ params.table_name }} ADD PARTITION ({{ params.partition }});
