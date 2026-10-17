WITH cte AS (
  SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity) AS sum_l_quantity,
    AVG(l_quantity) AS dfc,
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
  ORDER BY
    o_totalprice DESC NULLS LAST,
    o_orderdate
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
