WITH base_query AS (
  SELECT
    l_shipmode,
    SUM(
      CASE
        WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
        THEN 1
        ELSE 0
      END
    ) AS high_line_count,
    SUM(
      CASE
        WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
        THEN 1
        ELSE 0
      END
    ) AS low_line_count
  FROM orders, lineitem
  WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('1994-01-01' AS DATE)
    AND l_receiptdate < CAST('1995-01-01' AS DATE)
  GROUP BY
    l_shipmode
  ORDER BY
    l_shipmode
), policy_eval AS (
  SELECT
    l_shipmode AS l_shipmode
  FROM orders, lineitem
  WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('1994-01-01' AS DATE)
    AND l_receiptdate < CAST('1995-01-01' AS DATE)
  GROUP BY
    l_shipmode
  HAVING
    (
      AVG(lineitem.l_quantity) >= 30
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.l_shipmode = policy_eval.l_shipmode;
