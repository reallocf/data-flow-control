SELECT
  o_orderpriority,
  COUNT(*) AS order_count
FROM orders
INNER JOIN (
  SELECT
    l_orderkey,
    AVG(l_quantity) AS agg_0
  FROM lineitem
  WHERE
    l_commitdate < l_receiptdate
  GROUP BY
    l_orderkey
) AS exists_subquery
  ON o_orderkey = exists_subquery.l_orderkey
WHERE
  o_orderdate >= CAST('1993-07-01' AS DATE)
  AND o_orderdate < CAST('1993-10-01' AS DATE)
GROUP BY
  o_orderpriority
HAVING
  (
    MAX(exists_subquery.agg_0) >= 30
  )
ORDER BY
  o_orderpriority;
