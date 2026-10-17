WITH base_query AS (
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue,
    o_orderdate,
    o_shippriority
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
  base_query.l_orderkey,
  MAX(base_query.revenue) AS revenue,
  base_query.o_orderdate,
  base_query.o_shippriority
FROM base_query, customer, orders, lineitem
WHERE
  customer.c_mktsegment = 'BUILDING'
  AND customer.c_custkey = orders.o_custkey
  AND lineitem.l_orderkey = orders.o_orderkey
  AND orders.o_orderdate < CAST('1995-03-15' AS DATE)
  AND lineitem.l_shipdate > CAST('1995-03-15' AS DATE)
  AND base_query.l_orderkey = lineitem.l_orderkey
  AND base_query.o_orderdate = orders.o_orderdate
  AND base_query.o_shippriority = orders.o_shippriority
GROUP BY
  base_query.l_orderkey,
  base_query.o_orderdate,
  base_query.o_shippriority
HAVING
  AVG(lineitem.l_quantity) >= 30;
