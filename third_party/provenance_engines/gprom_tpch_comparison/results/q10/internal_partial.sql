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
), policy_eval AS (
  SELECT
    c_custkey AS c_custkey,
    c_name AS c_name,
    c_acctbal AS c_acctbal,
    c_phone AS c_phone,
    n_name AS n_name,
    c_address AS c_address,
    c_comment AS c_comment,
    AVG(lineitem.l_quantity) AS dfc
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
), cte AS (
  SELECT
    base_query.*,
    policy_eval.dfc AS dfc
  FROM base_query
  JOIN policy_eval
    ON base_query.c_custkey = policy_eval.c_custkey
    AND base_query.c_name = policy_eval.c_name
    AND base_query.c_acctbal = policy_eval.c_acctbal
    AND base_query.c_phone = policy_eval.c_phone
    AND base_query.n_name = policy_eval.n_name
    AND base_query.c_address = policy_eval.c_address
    AND base_query.c_comment = policy_eval.c_comment
  ORDER BY
    revenue DESC NULLS LAST
  LIMIT 20
)
SELECT
  c_custkey,
  c_name,
  revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
FROM cte
WHERE
  dfc >= 30;
