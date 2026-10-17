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
), policy_eval AS (
  SELECT
    o_orderpriority AS o_orderpriority
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
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.o_orderpriority = policy_eval.o_orderpriority;
