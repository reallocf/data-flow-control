WITH base_query AS (
  SELECT
    n_name,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue
  FROM customer, orders, lineitem, supplier, nation, region
  WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS DATE)
    AND o_orderdate < CAST('1995-01-01' AS DATE)
  GROUP BY
    n_name
)
SELECT
  base_query.n_name,
  MAX(base_query.revenue) AS revenue
FROM base_query, customer, orders, lineitem, supplier, nation, region
WHERE
  customer.c_custkey = orders.o_custkey
  AND lineitem.l_orderkey = orders.o_orderkey
  AND lineitem.l_suppkey = supplier.s_suppkey
  AND customer.c_nationkey = supplier.s_nationkey
  AND supplier.s_nationkey = nation.n_nationkey
  AND nation.n_regionkey = region.r_regionkey
  AND region.r_name = 'ASIA'
  AND orders.o_orderdate >= CAST('1994-01-01' AS DATE)
  AND orders.o_orderdate < CAST('1995-01-01' AS DATE)
  AND base_query.n_name = nation.n_name
GROUP BY
  base_query.n_name
HAVING
  AVG(lineitem.l_quantity) >= 30
ORDER BY
  revenue DESC NULLS LAST;
