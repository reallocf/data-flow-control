WITH base_query AS (
  SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) AS sum_l_quantity
  FROM customer, orders, lineitem
  WHERE
    o_orderkey IN (
      SELECT
        l_orderkey
      FROM lineitem
      GROUP BY
        l_orderkey
      HAVING
        SUM(l_quantity) > 300
    )
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
  GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
), policy_eval AS (
  SELECT
    c_name AS c_name,
    c_custkey AS c_custkey,
    o_orderkey AS o_orderkey,
    o_orderdate AS o_orderdate,
    o_totalprice AS o_totalprice,
    AVG(lineitem.l_quantity) AS dfc,
    MAX(in_subquery.dfc2) AS dfc2
  FROM customer
  INNER JOIN orders
    ON customer.c_custkey = orders.o_custkey
  INNER JOIN lineitem
    ON orders.o_orderkey = lineitem.l_orderkey
  INNER JOIN (
    SELECT
      l_orderkey,
      MAX(l_quantity) AS dfc2
    FROM lineitem
    GROUP BY
      l_orderkey
    HAVING
      SUM(l_quantity) > 300
  ) AS in_subquery
    ON o_orderkey = in_subquery.l_orderkey
  GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
), cte AS (
  SELECT
    base_query.*,
    policy_eval.dfc AS dfc,
    policy_eval.dfc2 AS dfc2
  FROM base_query
  JOIN policy_eval
    ON base_query.c_name = policy_eval.c_name
    AND base_query.c_custkey = policy_eval.c_custkey
    AND base_query.o_orderkey = policy_eval.o_orderkey
    AND base_query.o_orderdate = policy_eval.o_orderdate
    AND base_query.o_totalprice = policy_eval.o_totalprice
  ORDER BY
    base_query.o_totalprice DESC NULLS LAST,
    base_query.o_orderdate
  LIMIT 100
)
SELECT
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum_l_quantity
FROM cte
WHERE
  dfc >= 30 AND dfc2 >= 30;
