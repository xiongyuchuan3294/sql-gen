-- Union Merge Query
SELECT id, name, category
FROM table_a
WHERE ds='2025-01-01' AND status='active'
UNION ALL
SELECT id, name, category
FROM table_b
WHERE ds='2025-01-02' AND status='active'
;