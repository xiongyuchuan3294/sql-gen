-- Batch Data Count Query
SELECT 'table_A' as table_name, COUNT(1) as total_count FROM table_A WHERE ds='2025-01-01'
UNION ALL
SELECT 'table_B' as table_name, COUNT(1) as total_count FROM table_B WHERE ds='2025-01-02'
;