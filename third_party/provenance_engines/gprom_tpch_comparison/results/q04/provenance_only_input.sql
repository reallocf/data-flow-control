PROVENANCE OF (SELECT
    o_orderpriority,
    count(*) AS order_count
FROM orders
JOIN (
    SELECT l_orderkey
    FROM lineitem
    WHERE l_commitdate < l_receiptdate
    GROUP BY l_orderkey
) AS qualifying_lineitem
  ON o_orderkey = qualifying_lineitem.l_orderkey
WHERE o_orderdate >= CAST('1993-07-01' AS date)
  AND o_orderdate < CAST('1993-10-01' AS date)
GROUP BY o_orderpriority
ORDER BY o_orderpriority);
