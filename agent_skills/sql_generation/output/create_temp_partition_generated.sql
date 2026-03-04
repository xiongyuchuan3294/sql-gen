-- Create empty temp partition
ALTER TABLE example_table ADD PARTITION (ds='2026-02-01-temp');