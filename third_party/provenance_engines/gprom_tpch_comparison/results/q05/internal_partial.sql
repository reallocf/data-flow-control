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
  ORDER BY
    revenue DESC NULLS LAST
), policy_eval AS (
  SELECT
    n_name AS n_name
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
  HAVING
    (
      AVG(lineitem.l_quantity) >= 30
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.n_name = policy_eval.n_name;
