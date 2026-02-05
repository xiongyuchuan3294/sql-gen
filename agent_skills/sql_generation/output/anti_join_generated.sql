-- Anti Join (Missing Records) Query
SELECT
  t1.*
FROM table_a t1
LEFT JOIN table_b t2 ON t1.cust_id = t2.cust_id AND t1.trans_id = t2.trans_id
WHERE ds='2025-01-01'
  AND t2.cust_id IS NULL
  AND t2.ds='2025-01-01'; -- Auto-adapt partition for t2 alias