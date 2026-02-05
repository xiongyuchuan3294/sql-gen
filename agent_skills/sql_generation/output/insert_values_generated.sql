-- Insert Values Query
INSERT INTO TABLE example_table PARTITION (ds='2025-01-01')
VALUES
('val1', 10),
('val2', 20);