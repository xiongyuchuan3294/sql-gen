-- Data Diff Comparison Query
SELECT count(1)
FROM (
  SELECT cust_id,account_no, name,class
  FROM my_table
  WHERE ds='2024-04-30'
) t1
FULL OUTER JOIN (
  SELECT cust_id,account_no, name,class
  FROM your_table
  WHERE ds='2024-04-30'
) t2 ON t1.cust_id = t2.cust_id AND t1.account_no = t2.account_no
WHERE
nvl(t1.name, '#null') <> nvl(t2.name, '#null') OR
nvl(t1.class, '#null') <> nvl(t2.class, '#null') OR
t1.cust_id IS NULL OR t2.cust_id IS NULL;