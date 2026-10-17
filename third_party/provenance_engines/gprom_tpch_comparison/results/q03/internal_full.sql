WITH cte AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue,
    o_orderdate,
    o_shippriority,
    AVG(l_quantity) AS dfc
  FROM customer, orders, lineitem
  WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS DATE)
    AND l_shipdate > CAST('1995-03-15' AS DATE)
  GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
  ORDER BY
    revenue DESC NULLS LAST,
    o_orderdate
  LIMIT 10
)
SELECT
  l_orderkey,
  revenue,
  o_orderdate,
  o_shippriority
FROM cte
WHERE
  dfc >= 30;
