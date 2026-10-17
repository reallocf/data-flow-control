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
), policy_eval AS (
  SELECT
    l_orderkey AS l_orderkey,
    o_orderdate AS o_orderdate,
    o_shippriority AS o_shippriority,
    AVG(lineitem.l_quantity) AS dfc
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
), cte AS (
  SELECT
    base_query.*,
    policy_eval.dfc AS dfc
  FROM base_query
  JOIN policy_eval
    ON base_query.l_orderkey = policy_eval.l_orderkey
    AND base_query.o_orderdate = policy_eval.o_orderdate
    AND base_query.o_shippriority = policy_eval.o_shippriority
  ORDER BY
    revenue DESC NULLS LAST,
    base_query.o_orderdate
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
