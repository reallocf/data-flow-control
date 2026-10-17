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
  ORDER BY
    o_totalprice DESC NULLS LAST,
    o_orderdate
  LIMIT 100
), rewrite AS (
  SELECT
    base_query.c_name AS c_name,
    base_query.c_custkey AS c_custkey,
    base_query.o_orderkey AS o_orderkey,
    base_query.o_orderdate AS o_orderdate,
    base_query.o_totalprice AS o_totalprice,
    MAX(base_query.sum_l_quantity) AS sum_l_quantity,
    AVG(lineitem.l_quantity) AS policy_1,
    AVG(inner_lineitem.l_quantity) AS policy_2
  FROM base_query
  JOIN lineitem
    ON base_query.o_orderkey = lineitem.l_orderkey
  JOIN (
    SELECT
      l_orderkey
    FROM lineitem
    GROUP BY
      l_orderkey
    HAVING
      SUM(l_quantity) > 300
  ) AS in_subquery
    ON base_query.o_orderkey = in_subquery.l_orderkey
  JOIN lineitem AS inner_lineitem
    ON in_subquery.l_orderkey = inner_lineitem.l_orderkey
  GROUP BY
    base_query.c_name,
    base_query.c_custkey,
    base_query.o_orderkey,
    base_query.o_orderdate,
    base_query.o_totalprice
)
SELECT
  rewrite.c_name AS c_name,
  rewrite.c_custkey AS c_custkey,
  rewrite.o_orderkey AS o_orderkey,
  rewrite.o_orderdate AS o_orderdate,
  rewrite.o_totalprice AS o_totalprice,
  MAX(rewrite.sum_l_quantity) AS sum_l_quantity
FROM rewrite
GROUP BY
  rewrite.c_name,
  rewrite.c_custkey,
  rewrite.o_orderkey,
  rewrite.o_orderdate,
  rewrite.o_totalprice
HAVING
  (
    (
      MAX(rewrite.policy_1) >= 30
    ) AND (
      MAX(rewrite.policy_2) >= 30
    )
  )
ORDER BY
  o_totalprice DESC NULLS LAST,
  o_orderdate;
