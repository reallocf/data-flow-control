WITH base_query AS (
  SELECT
    n_name AS nation,
    EXTRACT(YEAR FROM o_orderdate) AS o_year,
    SUM(l_extendedprice * (
      1 - l_discount
    ) - ps_supplycost * l_quantity) AS sum_profit
  FROM part, supplier, lineitem, partsupp, orders, nation
  WHERE
    s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
  GROUP BY
    n_name,
    EXTRACT(YEAR FROM o_orderdate)
)
SELECT
  base_query.nation,
  base_query.o_year,
  MAX(base_query.sum_profit) AS sum_profit
FROM base_query, part, supplier, lineitem, partsupp, orders, nation
WHERE
  supplier.s_suppkey = lineitem.l_suppkey
  AND partsupp.ps_suppkey = lineitem.l_suppkey
  AND partsupp.ps_partkey = lineitem.l_partkey
  AND part.p_partkey = lineitem.l_partkey
  AND orders.o_orderkey = lineitem.l_orderkey
  AND supplier.s_nationkey = nation.n_nationkey
  AND part.p_name LIKE '%green%'
  AND base_query.nation = nation.n_name
  AND base_query.o_year = EXTRACT(YEAR FROM orders.o_orderdate)
GROUP BY
  base_query.nation,
  base_query.o_year
HAVING
  AVG(lineitem.l_quantity) >= 30
ORDER BY
  base_query.nation,
  base_query.o_year DESC NULLS LAST;
