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
)
SELECT
  base_query.l_shipmode,
  MAX(base_query.high_line_count) AS high_line_count,
  MAX(base_query.low_line_count) AS low_line_count
FROM base_query, orders, lineitem
WHERE
  orders.o_orderkey = lineitem.l_orderkey
  AND lineitem.l_shipmode IN ('MAIL', 'SHIP')
  AND lineitem.l_commitdate < lineitem.l_receiptdate
  AND lineitem.l_shipdate < lineitem.l_commitdate
  AND lineitem.l_receiptdate >= CAST('1994-01-01' AS DATE)
  AND lineitem.l_receiptdate < CAST('1995-01-01' AS DATE)
  AND base_query.l_shipmode = lineitem.l_shipmode
GROUP BY
  base_query.l_shipmode
HAVING
  AVG(lineitem.l_quantity) >= 30
ORDER BY
  base_query.l_shipmode;
