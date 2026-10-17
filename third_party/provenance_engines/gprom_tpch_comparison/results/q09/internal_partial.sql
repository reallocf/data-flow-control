WITH base_query AS (
  SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
  FROM (
    SELECT
      n_name AS nation,
      EXTRACT(YEAR FROM o_orderdate) AS o_year,
      l_extendedprice * (
        1 - l_discount
      ) - ps_supplycost * l_quantity AS amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE
      s_suppkey = l_suppkey
      AND ps_suppkey = l_suppkey
      AND ps_partkey = l_partkey
      AND p_partkey = l_partkey
      AND o_orderkey = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
  ) AS profit
  GROUP BY
    nation,
    o_year
  ORDER BY
    nation,
    o_year DESC NULLS LAST
), policy_eval AS (
  SELECT
    nation AS nation,
    o_year AS o_year
  FROM (
    SELECT
      n_name AS nation,
      EXTRACT(YEAR FROM o_orderdate) AS o_year,
      l_extendedprice * (
        1 - l_discount
      ) - ps_supplycost * l_quantity AS amount,
      lineitem.l_quantity
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE
      s_suppkey = l_suppkey
      AND ps_suppkey = l_suppkey
      AND ps_partkey = l_partkey
      AND p_partkey = l_partkey
      AND o_orderkey = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
  ) AS profit
  GROUP BY
    nation,
    o_year
  HAVING
    (
      AVG(profit.l_quantity) >= 30
    )
)
SELECT
  base_query.*
FROM base_query
JOIN policy_eval
  ON base_query.nation = policy_eval.nation AND base_query.o_year = policy_eval.o_year;
