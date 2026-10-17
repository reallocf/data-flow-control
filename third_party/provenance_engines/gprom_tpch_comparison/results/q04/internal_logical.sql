WITH base_query AS (
  SELECT
    o_orderpriority,
    COUNT(*) AS order_count
  FROM orders
  WHERE
    o_orderdate >= CAST('1993-07-01' AS DATE)
    AND o_orderdate < CAST('1993-10-01' AS DATE)
    AND EXISTS(
      SELECT
        *
      FROM lineitem
      WHERE
        l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
    )
  GROUP BY
    o_orderpriority
  ORDER BY
    o_orderpriority
), rewrite AS (
  SELECT
    base_query.o_orderpriority AS o_orderpriority,
    base_query.order_count AS order_count,
    orders.o_orderkey,
    AVG(lineitem.l_quantity) AS policy_1
  FROM base_query
  JOIN orders
    ON base_query.o_orderpriority = orders.o_orderpriority
  JOIN lineitem
    ON l_orderkey = o_orderkey
  WHERE
    l_commitdate < l_receiptdate
  GROUP BY
    base_query.o_orderpriority,
    base_query.order_count,
    orders.o_orderkey
)
SELECT
  rewrite.o_orderpriority AS o_orderpriority,
  rewrite.order_count AS order_count
FROM rewrite
GROUP BY
  rewrite.o_orderpriority,
  rewrite.order_count
HAVING
  MAX(rewrite.policy_1) >= 30
ORDER BY
  o_orderpriority;
