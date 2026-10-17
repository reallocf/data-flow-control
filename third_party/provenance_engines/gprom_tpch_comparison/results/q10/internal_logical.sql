WITH base_query AS (
  SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
  FROM customer, orders, lineitem, nation
  WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= CAST('1993-10-01' AS DATE)
    AND o_orderdate < CAST('1994-01-01' AS DATE)
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
  GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
  ORDER BY
    revenue DESC NULLS LAST
  LIMIT 20
)
SELECT
  base_query.c_custkey,
  base_query.c_name,
  MAX(base_query.revenue) AS revenue,
  base_query.c_acctbal,
  base_query.n_name,
  base_query.c_address,
  base_query.c_phone,
  base_query.c_comment
FROM base_query, customer, orders, lineitem, nation
WHERE
  customer.c_custkey = orders.o_custkey
  AND lineitem.l_orderkey = orders.o_orderkey
  AND orders.o_orderdate >= CAST('1993-10-01' AS DATE)
  AND orders.o_orderdate < CAST('1994-01-01' AS DATE)
  AND lineitem.l_returnflag = 'R'
  AND customer.c_nationkey = nation.n_nationkey
  AND base_query.c_custkey = customer.c_custkey
  AND base_query.c_name = customer.c_name
  AND base_query.c_acctbal = customer.c_acctbal
  AND base_query.c_phone = customer.c_phone
  AND base_query.n_name = nation.n_name
  AND base_query.c_address = customer.c_address
  AND base_query.c_comment = customer.c_comment
GROUP BY
  base_query.c_custkey,
  base_query.c_name,
  base_query.c_acctbal,
  base_query.c_phone,
  base_query.n_name,
  base_query.c_address,
  base_query.c_comment
HAVING
  AVG(lineitem.l_quantity) >= 30;
